#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <stack>

#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/checkStackSize.h>

#include <Core/Settings.h>
#include <DataTypes/UDT/QualifiedTypeReferenceCandidate.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTCastTarget.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateFunctionWithDriverQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUDTReference.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool skip_redundant_aliases_in_udf;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
extern const int TOO_BIG_AST;
extern const int TOO_DEEP_RECURSION;
extern const int UNSUPPORTED_METHOD;
}

namespace
{

constexpr size_t maximum_stored_udt_udf_scan_nodes = 1U << 20;
constexpr size_t maximum_stored_udt_udf_scan_depth = 256;

enum class StoredUDTUDFScanFailure : uint8_t
{
    None,
    NodeLimit,
    DepthLimit,
    MalformedAST,
};

struct StoredUDTUDFScanState
{
    size_t remaining_nodes = maximum_stored_udt_udf_scan_nodes;
    StoredUDTUDFScanFailure failure = StoredUDTUDFScanFailure::None;
};

bool consumeStoredUDTUDFScanNode(StoredUDTUDFScanState & state)
{
    if (state.remaining_nodes == 0)
    {
        state.failure = StoredUDTUDFScanFailure::NodeLimit;
        return false;
    }
    --state.remaining_nodes;
    return true;
}

bool classifiedSlotContainsPotentialStoredUDTSyntax(const UDT::StoredObjectTypeStringSlotClassification & slot)
{
    switch (slot.status)
    {
        case UDT::StoredObjectTypeStringSlotStatus::Unregistered:
        case UDT::StoredObjectTypeStringSlotStatus::NoExplicitSchemaString: return false;
        case UDT::StoredObjectTypeStringSlotStatus::ExactExpression: {
            const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
            if (!literal || literal->value.getType() != Field::Types::String)
                return true;
            return UDT::hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>());
        }
        case UDT::StoredObjectTypeStringSlotStatus::ContextRequired:
        case UDT::StoredObjectTypeStringSlotStatus::UnclassifiedLayout: return true;
    }
    return true;
}

size_t directChildCount(const IAST & parent, const IAST * candidate)
{
    if (!candidate)
        return 0;
    return std::count_if(
        parent.children.begin(), parent.children.end(), [candidate](const ASTPtr & child) { return child.get() == candidate; });
}

bool containsPotentialStoredUDTSyntax(const IAST & ast, size_t depth, StoredUDTUDFScanState & state)
{
    checkStackSize();
    if (depth > maximum_stored_udt_udf_scan_depth)
    {
        state.failure = StoredUDTUDFScanFailure::DepthLimit;
        return true;
    }
    if (!consumeStoredUDTUDFScanNode(state))
        return true;

    if (ast.as<ASTCastTarget>() || ast.as<ASTUDTReference>())
        return true;
    if (const auto * parameter = ast.as<ASTQueryParameter>(); parameter && UDT::hasQualifiedTypeReferenceCandidate(parameter->type))
        return true;

    if (const auto * settings = ast.as<ASTSetQuery>())
    {
        /// ASTSetQuery stores SettingsChanges outside IAST::children. Charge
        /// every opaque entry to the same aggregate body budget and inspect
        /// the closed setting registry explicitly.
        for (const auto & change : settings->changes)
        {
            if (!consumeStoredUDTUDFScanNode(state))
                return true;
            if (!UDT::tryGetStoredSettingTypeStringSlotContract(change.name))
                continue;
            if (change.value.getType() != Field::Types::String)
                return true;
            if (UDT::hasQualifiedTypeReferenceCandidate(change.value.safeGet<String>()))
                return true;
        }
    }

    if (const auto * table_expression = ast.as<ASTTableExpression>())
    {
        if (table_expression->children.size() > state.remaining_nodes)
        {
            state.failure = StoredUDTUDFScanFailure::NodeLimit;
            return true;
        }

        const std::array<const IAST *, 7> owned_fields{
            table_expression->database_and_table_name.get(),
            table_expression->table_function.get(),
            table_expression->subquery.get(),
            table_expression->sample_size.get(),
            table_expression->sample_offset.get(),
            table_expression->stream_settings.get(),
            table_expression->column_aliases.get(),
        };
        if (std::any_of(
                owned_fields.begin(),
                owned_fields.end(),
                [&](const IAST * field) { return field && directChildCount(*table_expression, field) != 1; }))
        {
            state.failure = StoredUDTUDFScanFailure::MalformedAST;
            return true;
        }

        if (table_expression->table_function)
        {
            const auto * table_function = table_expression->table_function->as<ASTFunction>();
            if (!table_function)
            {
                state.failure = StoredUDTUDFScanFailure::MalformedAST;
                return true;
            }
            if (table_function->arguments && table_function->arguments->children.size() > state.remaining_nodes)
            {
                state.failure = StoredUDTUDFScanFailure::NodeLimit;
                return true;
            }
            const auto tree = UDT::classifyStoredObjectTableFunctionTypeStringTree(*table_function);
            if (tree.status != UDT::StoredObjectTableFunctionTypeStringTreeStatus::Complete || !tree.schema_owner)
            {
                state.failure = StoredUDTUDFScanFailure::MalformedAST;
                return true;
            }
            if (classifiedSlotContainsPotentialStoredUDTSyntax(tree.schema_slot))
                return true;
        }
    }

    if (const auto * function = ast.as<ASTFunction>())
    {
        /// These owning fields are not independently discoverable through the
        /// generic interface if a malformed AST forgets to register them as
        /// children. Reject such a graph before a hidden type-string slot or
        /// nested SQL UDF can cross the durable boundary.
        const bool malformed_owned_field = (function->arguments && directChildCount(*function, function->arguments.get()) != 1)
            || (function->parameters && directChildCount(*function, function->parameters.get()) != 1)
            || (function->window_definition && directChildCount(*function, function->window_definition.get()) != 1);
        if (malformed_owned_field)
        {
            state.failure = StoredUDTUDFScanFailure::MalformedAST;
            return true;
        }
        if (classifiedSlotContainsPotentialStoredUDTSyntax(UDT::classifyStoredExpressionTypeStringSlot(*function)))
            return true;
    }

    for (const auto & child : ast.children)
    {
        if (!child)
        {
            state.failure = StoredUDTUDFScanFailure::MalformedAST;
            return true;
        }
        if (containsPotentialStoredUDTSyntax(*child, depth + 1, state))
            return true;
    }
    return false;
}

[[noreturn]] void throwUnsupportedStoredUDTUDFBody(const String & function_name)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "SQL UDF '{}' cannot be substituted into stored-object DDL because global SQL UDF bodies are not a durable "
        "user-defined type context",
        function_name);
}

void assertSafeStoredObjectUDFBody(const IAST & function_core, const String & function_name, StoredUDTUDFScanState & scan_state)
{
    if (!containsPotentialStoredUDTSyntax(function_core, 0, scan_state))
        return;

    switch (scan_state.failure)
    {
        case StoredUDTUDFScanFailure::NodeLimit:
            throw Exception(
                ErrorCodes::TOO_BIG_AST,
                "SQL UDF bodies reachable from stored-object DDL exceed the bounded {}-node inspection budget",
                maximum_stored_udt_udf_scan_nodes);
        case StoredUDTUDFScanFailure::DepthLimit:
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "SQL UDF '{}' exceeds the maximum stored-object inspection depth ({})",
                function_name,
                maximum_stored_udt_udf_scan_depth);
        case StoredUDTUDFScanFailure::MalformedAST:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "SQL UDF '{}' has a malformed body and cannot be substituted into stored-object DDL",
                function_name);
        case StoredUDTUDFScanFailure::None: break;
    }

    throwUnsupportedStoredUDTUDFBody(function_name);
}

}

void UserDefinedSQLFunctionVisitor::assertNoStoredUDTSyntaxInFunctionBodiesToReplace(const ASTPtr & ast, ContextPtr context_)
{
    if (!ast || !context_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot audit SQL UDF substitution without an AST and Context");

    /// The query and every distinct definition image are inspected iteratively.
    /// Retained ASTPtrs keep a definition alive if it is concurrently replaced;
    /// exact substitution repeats the body check against the image it clones.
    VectorWithMemoryTracking<ASTPtr> pending{ast};
    UnorderedSetWithMemoryTracking<const IAST *> inspected_definitions;
    size_t remaining_discovery_nodes = maximum_stored_udt_udf_scan_nodes;
    StoredUDTUDFScanState body_scan_state;
    while (!pending.empty())
    {
        if (remaining_discovery_nodes == 0)
            throw Exception(
                ErrorCodes::TOO_BIG_AST,
                "Stored-object SQL UDF discovery exceeds the bounded {}-node inspection budget",
                maximum_stored_udt_udf_scan_nodes);
        --remaining_discovery_nodes;

        auto current = std::move(pending.back());
        pending.pop_back();
        if (!current)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Malformed AST cannot be audited for stored-object SQL UDF substitution");

        if (const auto * function = current->as<ASTFunction>())
        {
            const auto definition = UserDefinedSQLFunctionFactory::instance().tryGet(function->name);
            const auto * create_function = definition ? definition->as<ASTCreateSQLFunctionQuery>() : nullptr;
            if (create_function && inspected_definitions.emplace(create_function).second)
            {
                if (!create_function->function_core || create_function->function_core->children.empty()
                    || !create_function->function_core->children.front())
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "SQL UDF '{}' has a malformed body and cannot be substituted into stored-object DDL",
                        function->name);

                const auto & function_core = create_function->function_core->children.front();
                assertSafeStoredObjectUDFBody(*function_core, function->name, body_scan_state);
                pending.push_back(function_core);
            }
        }

        if (pending.size() > remaining_discovery_nodes || current->children.size() > remaining_discovery_nodes - pending.size())
        {
            throw Exception(
                ErrorCodes::TOO_BIG_AST,
                "Stored-object SQL UDF discovery exceeds the bounded {}-node inspection budget",
                maximum_stored_udt_udf_scan_nodes);
        }
        pending.insert(pending.end(), current->children.begin(), current->children.end());
    }
}

void UserDefinedSQLFunctionVisitor::assertNoStoredUDTSyntaxInFunctionDefinition(
    const ASTPtr & create_function_ast, size_t & remaining_inspection_nodes)
{
    const auto * create_function = create_function_ast ? create_function_ast->as<ASTCreateSQLFunctionQuery>() : nullptr;
    if (!create_function || !create_function->function_core)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot audit an invalid SQL UDF definition for stored-object substitution");

    StoredUDTUDFScanState scan_state{.remaining_nodes = remaining_inspection_nodes};
    assertSafeStoredObjectUDFBody(*create_function->function_core, create_function->getFunctionName(), scan_state);
    remaining_inspection_nodes = scan_state.remaining_nodes;
}

void UserDefinedSQLFunctionVisitor::visit(ASTPtr & ast, ContextPtr context_, bool reject_stored_udt_syntax_in_function_bodies)
{
    chassert(ast);
    reject_stored_udt_syntax_in_function_bodies
        = reject_stored_udt_syntax_in_function_bodies || (context_ && context_->shouldRejectStoredUDTSyntaxInSQLUDFBodies());

    size_t remaining_inspection_nodes = maximum_stored_udt_udf_scan_nodes;
    visitImpl(
        ast,
        context_,
        reject_stored_udt_syntax_in_function_bodies,
        reject_stored_udt_syntax_in_function_bodies ? &remaining_inspection_nodes : nullptr);
}

void UserDefinedSQLFunctionVisitor::visitImpl(
    ASTPtr & ast, ContextPtr context_, bool reject_stored_udt_syntax_in_function_bodies, size_t * remaining_inspection_nodes)
{
    chassert(ast);
    checkStackSize();

    for (auto & child : ast->children)
    {
        if (!child)
        {
            if (reject_stored_udt_syntax_in_function_bodies)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Malformed AST cannot be inspected for stored-object SQL UDF substitution");
            }
            return;
        }

        auto * old_ptr = child.get();
        visitImpl(child, context_, reject_stored_udt_syntax_in_function_bodies, remaining_inspection_nodes);
        auto * new_ptr = child.get();

        /// Some AST classes have naked pointers to children elements as members.
        /// We have to replace them if the child was replaced.
        if (new_ptr != old_ptr)
            ast->updatePointerToChild(old_ptr, new_ptr);
    }

    if (const auto * function = ast->template as<ASTFunction>())
    {
        UnorderedSetWithMemoryTracking<std::string> udf_in_replace_process;
        auto replace_result = tryToReplaceFunction(
            *function, udf_in_replace_process, context_, reject_stored_udt_syntax_in_function_bodies, remaining_inspection_nodes, 0);
        if (replace_result)
            ast = replace_result;
    }
}

namespace
{
bool isVariadic(const ASTPtr & arg)
{
    return arg->as<ASTAsterisk>() || arg->as<ASTQualifiedAsterisk>() || arg->as<ASTColumnsRegexpMatcher>()
        || arg->as<ASTColumnsListMatcher>() || arg->as<ASTQualifiedColumnsRegexpMatcher>() || arg->as<ASTQualifiedColumnsListMatcher>();
}
}

ASTPtr UserDefinedSQLFunctionVisitor::tryToReplaceFunction(
    const ASTFunction & function,
    UnorderedSetWithMemoryTracking<std::string> & udf_in_replace_process,
    ContextPtr context_,
    bool reject_stored_udt_syntax_in_function_bodies,
    size_t * remaining_inspection_nodes,
    size_t udf_expansion_depth)
{
    if (reject_stored_udt_syntax_in_function_bodies && udf_expansion_depth > maximum_stored_udt_udf_scan_depth)
    {
        throw Exception(
            ErrorCodes::TOO_DEEP_RECURSION,
            "Stored-object SQL UDF expansion exceeds the maximum nested definition depth ({})",
            maximum_stored_udt_udf_scan_depth);
    }
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
            "The function '{}' is not a SQL defined function and is not supported when 'enable_analyzer' is set to false", function.formatForErrorMessage());
    if (context_ && context_->isStoredObjectSQLUDFSubstitutionFrozen())
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "SQL UDF '{}' became visible after the stored-object DDL substitution snapshot was frozen",
            function.name);
    }

    auto & function_core_expression = create_function_query->function_core->children.at(0);
    if (reject_stored_udt_syntax_in_function_bodies)
    {
        chassert(remaining_inspection_nodes);
        StoredUDTUDFScanState scan_state{.remaining_nodes = *remaining_inspection_nodes};
        assertSafeStoredObjectUDFBody(*function_core_expression, function.name, scan_state);
        *remaining_inspection_nodes = scan_state.remaining_nodes;
    }

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

    std::stack<ASTPtr> ast_nodes_to_update;
    ast_nodes_to_update.push(expression_list);

    while (!ast_nodes_to_update.empty())
    {
        auto ast_node_to_update = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        for (auto & child : ast_node_to_update->children)
        {
            if (auto * inner_function = child->as<ASTFunction>())
            {
                auto replace_result = tryToReplaceFunction(
                    *inner_function,
                    udf_in_replace_process,
                    context_,
                    reject_stored_udt_syntax_in_function_bodies,
                    remaining_inspection_nodes,
                    udf_expansion_depth + 1);
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

            ast_nodes_to_update.push(child);
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
