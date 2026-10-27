#include <Common/StackWithMemoryTracking.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <stack>
#include <unordered_set>

#include <Common/UnorderedMapWithMemoryTracking.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool skip_redundant_aliases_in_udf;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// The parser also builds an `ASTFunction` for the name of a codec, statistics or index type, engine, table function
/// or aggregate function in a data type (`Name`). A SQL UDF replaces only a `Call`; the arguments of a `Name` are calls.
enum class Slot : uint8_t
{
    Call,
    Name,
    NameList,
};

Slot getChildSlot(const IAST & parent, Slot parent_slot, const IAST * child)
{
    if (parent_slot == Slot::NameList)
        return Slot::Name;

    if (const auto * function = parent.as<ASTFunction>())
    {
        const auto kind = function->getKind();
        if ((kind == ASTFunction::Kind::CODEC || kind == ASTFunction::Kind::STATISTICS) && child == function->arguments.get())
            return Slot::NameList;
    }
    else if (const auto * statistics = parent.as<ASTStatisticsDeclaration>())
    {
        if (child == statistics->types)
            return Slot::NameList;
    }
    else if (const auto * index = parent.as<ASTIndexDeclaration>())
    {
        if (child != index->getExpression().get())
            return Slot::Name;
    }
    else if (const auto * projection = parent.as<ASTProjectionDeclaration>())
    {
        if (child == projection->type)
            return Slot::Name;
    }
    else if (const auto * storage = parent.as<ASTStorage>())
    {
        if (child == storage->engine)
            return Slot::Name;
    }
    else if (const auto * create = parent.as<ASTCreateQuery>())
    {
        if (child == create->as_table_function)
            return Slot::Name;
    }
    else if (const auto * table_expression = parent.as<ASTTableExpression>())
    {
        if (child == table_expression->table_function.get())
            return Slot::Name;
    }
    else if (const auto * data_type = parent.as<ASTDataType>())
    {
        if ((data_type->name == "AggregateFunction" || data_type->name == "SimpleAggregateFunction")
            && child == data_type->getArguments().get())
            return Slot::NameList;
    }
    return Slot::Call;
}

bool isCall(const IAST & ast, Slot slot)
{
    const auto * function = ast.as<ASTFunction>();
    if (!function || slot != Slot::Call || function->isLambdaFunction() || isASTLambdaFunction(*function))
        return false;
    const auto kind = function->getKind();
    return kind == ASTFunction::Kind::ORDINARY_FUNCTION || kind == ASTFunction::Kind::WINDOW_FUNCTION;
}

/// Visits `ast`, which sits in `slot`, bottom-up and passes every call to `replace`.
template <typename Replace>
void visitSlots(ASTPtr & ast, Slot slot, Replace && replace)
{
    for (auto & child : ast->children)
    {
        if (!child)
            return;

        const auto * old_ptr = child.get();
        visitSlots(child, getChildSlot(*ast, slot, old_ptr), replace);

        /// Some AST classes have naked pointers to children elements as members.
        /// We have to replace them if the child was replaced.
        if (child.get() != old_ptr)
            ast->updatePointerToChild(old_ptr, child);
    }

    /// `GROUP BY` keys and `SET` assignments of a `TTL` rule are not among its children.
    if (auto * ttl_element = ast->template as<ASTTTLElement>())
    {
        for (auto & key : ttl_element->group_by_key)
            visitSlots(key, Slot::Call, replace);
        for (auto & assignment : ttl_element->group_by_assignments)
            visitSlots(assignment, Slot::Call, replace);
    }

    if (isCall(*ast, slot))
        replace(ast);
}

}

void UserDefinedSQLFunctionVisitor::visit(ASTPtr & ast, ContextPtr context_)
{
    chassert(ast);

    visitSlots(ast, Slot::Call, [&](ASTPtr & node)
    {
        UnorderedSetWithMemoryTracking<std::string> udf_in_replace_process;
        if (auto replace_result = tryToReplaceFunction(node->as<ASTFunction &>(), udf_in_replace_process, context_))
            node = replace_result;
    });
}

namespace
{
bool isVariadic(const ASTPtr & arg)
{
    return arg->as<ASTAsterisk>() || arg->as<ASTQualifiedAsterisk>() || arg->as<ASTColumnsRegexpMatcher>()
        || arg->as<ASTColumnsListMatcher>() || arg->as<ASTQualifiedColumnsRegexpMatcher>() || arg->as<ASTQualifiedColumnsListMatcher>();
}
}

ASTPtr UserDefinedSQLFunctionVisitor::tryToReplaceFunction(const ASTFunction & function, UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process, ContextPtr context_)
{
    if (udf_in_replace_process.contains(function.name))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive function call detected during function call {}",
            function.name);

    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function.name);
    if (!user_defined_function)
        return nullptr;

    const auto & function_arguments_list = function.children.at(0)->as<ASTExpressionList>();
    auto & function_arguments = function_arguments_list->children;

    auto * create_function_query = user_defined_function->as<ASTCreateSQLFunctionQuery>();

    if (!create_function_query && user_defined_function->as<ASTCreateWasmFunctionQuery>())
        return nullptr;

    /// Driver-created executable functions are resolved through `UserDefinedExecutableFunctionFactory`.
    if (!create_function_query && user_defined_function->as<ASTCreateFunctionWithDriverQuery>())
        return nullptr;

    if (!create_function_query)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "The function '{}' is not a SQL defined function", function.formatForErrorMessage());

    auto & function_core_expression = create_function_query->function_core->children.at(0);

    const auto & identifiers_expression_list = function_core_expression->children.at(0)->children.at(0)->as<ASTExpressionList>();
    const auto & identifiers_raw = identifiers_expression_list->children;

    if (function_arguments.size() != identifiers_raw.size())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Function {} expects {} arguments actual arguments {}",
            create_function_query->getFunctionName(),
            identifiers_raw.size(),
            function_arguments.size());

    for (const auto & arg : function_arguments)
    {
        if (isVariadic(arg))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "It is not possible to replace a variadic argument '{}' in UDF {}",
                arg->getColumnName(),
                function.name);
    }

    if (isVariadic(function_core_expression->children.at(1)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "It is not possible to replace a variadic argument '{}' in UDF {}",
            function_core_expression->children.at(1)->getColumnName(),
            function.name);

    UnorderedMapWithMemoryTracking<std::string, ASTPtr> identifier_name_to_function_argument;

    for (size_t parameter_index = 0; parameter_index < identifiers_raw.size(); ++parameter_index)
    {
        const auto & identifier = identifiers_raw[parameter_index]->as<ASTIdentifier>();
        const auto & function_argument = function_arguments[parameter_index];
        const auto & identifier_name = identifier->name();

        identifier_name_to_function_argument.emplace(identifier_name, function_argument);
    }

    auto [it, _] = udf_in_replace_process.emplace(function.name);

    auto function_body_to_update = function_core_expression->children.at(1)->clone();

    if (context_->getSettingsRef()[Setting::skip_redundant_aliases_in_udf])
    {
        Aliases aliases;
        QueryAliasesVisitor(aliases).visit(function_body_to_update);

        /// Mark table ASTIdentifiers with not a column marker
        MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
        MarkTableIdentifiersVisitor(identifiers_data).visit(function_body_to_update);

        /// Common subexpression elimination. Rewrite rules.
        /// `source_columns` must be a named local: `QueryNormalizer::Data` stores `source_columns_set`
        /// by reference, so a `{}` temporary would dangle once this statement ends.
        NameSet source_columns;
        QueryNormalizer::Data normalizer_data(aliases, source_columns, true, QueryNormalizer::ExtractedSettings(context_->getSettingsRef()), true, false);
        QueryNormalizer(normalizer_data).visit(function_body_to_update);
    }

    auto expression_list = make_intrusive<ASTExpressionList>();
    expression_list->children.emplace_back(std::move(function_body_to_update));

    StackWithMemoryTracking<std::pair<ASTPtr, Slot>> ast_nodes_to_update;
    ast_nodes_to_update.push({expression_list, Slot::Call});

    while (!ast_nodes_to_update.empty())
    {
        auto [ast_node_to_update, node_slot] = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        for (auto & child : ast_node_to_update->children)
        {
            const auto child_slot = getChildSlot(*ast_node_to_update, node_slot, child.get());
            if (isCall(*child, child_slot))
            {
                auto replace_result = tryToReplaceFunction(child->as<ASTFunction &>(), udf_in_replace_process, context_);
                if (replace_result)
                    child = replace_result;
            }

            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
            {
                auto function_argument_it = identifier_name_to_function_argument.find(*identifier_name_opt);

                if (function_argument_it == identifier_name_to_function_argument.end())
                    continue;

                auto child_alias = child->tryGetAlias();
                child = function_argument_it->second->clone();

                if (!child_alias.empty())
                    child->setAlias(child_alias);

                continue;
            }

            ast_nodes_to_update.push({child, child_slot});
        }
    }

    udf_in_replace_process.erase(it);

    function_body_to_update = expression_list->children[0];

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}

}
