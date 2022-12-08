#include <Analyzer/Passes/IfTransformStringsToEnumPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

/// Visitor for finding functions that are used inside another function
class FindUsedFunctionsVisitor : public ConstInDepthQueryTreeVisitor<FindUsedFunctionsVisitor>
{
public:
    FindUsedFunctionsVisitor(
        std::unordered_set<FunctionNode *> & used_functions_,
        const std::unordered_set<std::string> & function_names_,
        size_t stack_size_)
        : used_functions(used_functions_), function_names(function_names_), stack_size(stack_size_)
    {
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType & /* child */)
    {
        return parent->getNodeType() != QueryTreeNodeType::FUNCTION;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        const auto & function_name = function_node->getFunctionName();
        if (function_names.contains(function_name) && stack_size > 0)
            used_functions.insert(function_node);

        FindUsedFunctionsVisitor visitor(used_functions, function_names, stack_size + 1);
        visitor.visit(function_node->getArgumentsNode());
    }

private:
    /// we store only function pointers because these nodes won't be modified
    std::unordered_set<FunctionNode *> & used_functions;
    const std::unordered_set<std::string> & function_names;
    size_t stack_size;
};

/// We place strings in ascending order here under the assumption it could speed up String to Enum conversion.
template <typename EnumType>
auto getDataEnumType(const std::set<std::string> & string_values)
{
    using EnumValues = typename EnumType::Values;
    EnumValues enum_values;
    enum_values.reserve(string_values.size());

    size_t number = 1;
    for (const auto & value : string_values)
        enum_values.emplace_back(value, number++);

    return std::make_shared<EnumType>(std::move(enum_values));
}

DataTypePtr getEnumType(const std::set<std::string> & string_values)
{
    if (string_values.size() >= 255)
        return getDataEnumType<DataTypeEnum16>(string_values);
    else
        return getDataEnumType<DataTypeEnum8>(string_values);
}

QueryTreeNodePtr createCastFunction(QueryTreeNodePtr from, DataTypePtr result_type, ContextPtr context)
{
    auto enum_literal = std::make_shared<ConstantValue>(result_type->getName(), std::make_shared<DataTypeString>());
    auto enum_literal_node = std::make_shared<ConstantNode>(std::move(enum_literal));

    auto cast_function = FunctionFactory::instance().get("_CAST", std::move(context));
    QueryTreeNodes arguments{std::move(from), std::move(enum_literal_node)};

    auto function_node = std::make_shared<FunctionNode>("_CAST");
    function_node->resolveAsFunction(std::move(cast_function), std::move(result_type));
    function_node->getArguments().getNodes() = std::move(arguments);

    return function_node;
}

void changeIfArguments(
    QueryTreeNodePtr & first, QueryTreeNodePtr & second, const std::set<std::string> & string_values, const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    first = createCastFunction(first, result_type, context);
    second = createCastFunction(second, result_type, context);
}

void changeTransformArguments(
    QueryTreeNodePtr & array_to,
    QueryTreeNodePtr & default_value,
    const std::set<std::string> & string_values,
    const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    array_to = createCastFunction(array_to, std::make_shared<DataTypeArray>(result_type), context);
    default_value = createCastFunction(default_value, std::move(result_type), context);
}

class ConvertStringsToEnumVisitor : public InDepthQueryTreeVisitor<ConvertStringsToEnumVisitor>
{
public:
    explicit ConvertStringsToEnumVisitor(std::unordered_set<FunctionNode *> used_functions_, ContextPtr context_)
        : used_functions(std::move(used_functions_)), context(std::move(context_))
    {
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType & /* child */)
    {
        return parent->getNodeType() != QueryTreeNodeType::FUNCTION;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        /// we cannot change the type of its result because it's used
        /// as argument in another function
        if (used_functions.contains(function_node))
            return;

        std::string_view function_name = function_node->getFunctionName();
        if (function_name == "if")
        {
            auto & argument_nodes = function_node->getArguments().getNodes();

            if (argument_nodes.size() != 3)
                return;

            const auto * first_literal = argument_nodes[1]->as<ConstantNode>();
            const auto * second_literal = argument_nodes[2]->as<ConstantNode>();

            if (!first_literal || !second_literal)
                return;

            if (!WhichDataType(first_literal->getResultType()).isString() || !WhichDataType(second_literal->getResultType()).isString())
                return;

            std::set<std::string> string_values;
            string_values.insert(first_literal->getValue().get<std::string>());
            string_values.insert(second_literal->getValue().get<std::string>());

            changeIfArguments(argument_nodes[1], argument_nodes[2], string_values, context);
            return;
        }

        if (function_name == "transform")
        {
            auto & argument_nodes = function_node->getArguments().getNodes();

            if (argument_nodes.size() != 4)
                return;

            const auto * literal_to = argument_nodes[2]->as<ConstantNode>();
            const auto * literal_default = argument_nodes[3]->as<ConstantNode>();

            if (!literal_to || !literal_default)
                return;

            if (!WhichDataType(literal_to->getResultType()).isArray() || !WhichDataType(literal_default->getResultType()).isString())
                return;

            auto array_to = literal_to->getValue().get<Array>();

            if (array_to.empty())
                return;

            if (!std::all_of(
                    array_to.begin(),
                    array_to.end(),
                    [](const auto & field) { return field.getType() == Field::Types::Which::String; }))
                return;

            /// collect possible string values
            std::set<std::string> string_values;

            for (const auto & value : array_to)
                string_values.insert(value.get<std::string>());

            string_values.insert(literal_default->getValue().get<std::string>());

            changeTransformArguments(argument_nodes[2], argument_nodes[3], string_values, context);
            return;
        }
    }

private:
    std::unordered_set<FunctionNode *> used_functions;
    ContextPtr context;
};

}

void IfTransformStringsToEnumPass::run(QueryTreeNodePtr query, ContextPtr context)
{
    std::unordered_set<FunctionNode *> used_functions;
    std::unordered_set<std::string> function_names{"if", "transform"};

    {
        FindUsedFunctionsVisitor visitor(used_functions, function_names, 0);
        visitor.visit(query);
    }

    {
        ConvertStringsToEnumVisitor visitor(std::move(used_functions), context);
        visitor.visit(query);
    }
}

}
