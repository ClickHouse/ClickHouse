#include <Analyzer/Passes/IfTransformStringsToEnumPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

#include <Functions/FunctionFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_if_transform_strings_to_enum;
}

namespace
{

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

/// if(arg1, arg2, arg3) will be transformed to if(arg1, _CAST(arg2, Enum...), _CAST(arg3, Enum...))
/// where Enum is generated based on the possible values stored in string_values
void changeIfArguments(
    FunctionNode & if_node, const std::set<std::string> & string_values, const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    auto & argument_nodes = if_node.getArguments().getNodes();

    argument_nodes[1] = createCastFunction(argument_nodes[1], result_type, context);
    argument_nodes[2] = createCastFunction(argument_nodes[2], result_type, context);

    auto if_resolver = FunctionFactory::instance().get("if", context);

    if_node.resolveAsFunction(if_resolver->build(if_node.getArgumentColumns()));
}

/// transform(value, array_from, array_to, default_value) will be transformed to transform(value, array_from, _CAST(array_to, Array(Enum...)), _CAST(default_value, Enum...))
/// where Enum is generated based on the possible values stored in string_values
void changeTransformArguments(
    FunctionNode & transform_node,
    const std::set<std::string> & string_values,
    const ContextPtr & context)
{
    auto result_type = getEnumType(string_values);

    auto & arguments = transform_node.getArguments().getNodes();

    auto & array_to = arguments[2];
    auto & default_value = arguments[3];

    array_to = createCastFunction(array_to, std::make_shared<DataTypeArray>(result_type), context);
    default_value = createCastFunction(default_value, std::move(result_type), context);

    auto transform_resolver = FunctionFactory::instance().get("transform", context);

    transform_node.resolveAsFunction(transform_resolver->build(transform_node.getArgumentColumns()));
}

void wrapIntoToString(FunctionNode & function_node, QueryTreeNodePtr arg, ContextPtr context)
{
    auto to_string_function = FunctionFactory::instance().get("toString", std::move(context));
    QueryTreeNodes arguments{ std::move(arg) };
    function_node.getArguments().getNodes() = std::move(arguments);

    function_node.resolveAsFunction(to_string_function->build(function_node.getArgumentColumns()));

    assert(isString(function_node.getResultType()));
}

class ConvertStringsToEnumVisitor : public InDepthQueryTreeVisitorWithContext<ConvertStringsToEnumVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<ConvertStringsToEnumVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_if_transform_strings_to_enum])
            return;

        auto * function_node = node->as<FunctionNode>();

        if (!function_node)
            return;

        const auto & context = getContext();

        /// to preserve return type (String) of the current function_node, we wrap the newly
        /// generated function nodes into toString

        std::string_view function_name = function_node->getFunctionName();
        if (function_name == "if")
        {
            if (function_node->getArguments().getNodes().size() != 3)
                return;

            auto modified_if_node = function_node->clone();
            auto * function_if_node = modified_if_node->as<FunctionNode>();
            auto & argument_nodes = function_if_node->getArguments().getNodes();

            const auto * first_literal = argument_nodes[1]->as<ConstantNode>();
            const auto * second_literal = argument_nodes[2]->as<ConstantNode>();

            if (!first_literal || !second_literal)
                return;

            if (!isString(first_literal->getResultType()) || !isString(second_literal->getResultType()))
                return;

            std::set<std::string> string_values;
            string_values.insert(first_literal->getValue().safeGet<std::string>());
            string_values.insert(second_literal->getValue().safeGet<std::string>());

            changeIfArguments(*function_if_node, string_values, context);
            wrapIntoToString(*function_node, std::move(modified_if_node), context);
            return;
        }

        if (function_name == "transform")
        {
            if (function_node->getArguments().getNodes().size() != 4)
                return;

            auto modified_transform_node = function_node->clone();
            auto * function_modified_transform_node = modified_transform_node->as<FunctionNode>();
            auto & argument_nodes = function_modified_transform_node->getArguments().getNodes();

            if (!isString(function_node->getResultType()))
                return;

            const auto * literal_to = argument_nodes[2]->as<ConstantNode>();
            const auto * literal_default = argument_nodes[3]->as<ConstantNode>();

            if (!literal_to || !literal_default)
                return;

            if (!isArray(literal_to->getResultType()) || !isString(literal_default->getResultType()))
                return;

            auto array_to = literal_to->getValue().safeGet<Array>();

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
                string_values.insert(value.safeGet<std::string>());

            string_values.insert(literal_default->getValue().safeGet<std::string>());

            changeTransformArguments(*function_modified_transform_node, string_values, context);
            wrapIntoToString(*function_node, std::move(modified_transform_node), context);
            return;
        }
    }
};

}

void IfTransformStringsToEnumPass::run(QueryTreeNodePtr & query, ContextPtr context)
{
    ConvertStringsToEnumVisitor visitor(std::move(context));
    visitor.visit(query);
}

}
