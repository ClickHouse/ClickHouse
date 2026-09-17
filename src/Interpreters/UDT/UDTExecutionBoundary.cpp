#include <Interpreters/UDT/UDTExecutionBoundary.h>

#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/QualifiedTypeReferenceCandidate.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCastTarget.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTParallelWithQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTCreateRowPolicyQuery.h>
#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <optional>
#include <string_view>
#include <typeinfo>
#include <utility>

#include <absl/container/flat_hash_set.h>
#include <base/StringViewHash.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
extern const int SYNTAX_ERROR;
extern const int TOO_BIG_AST;
}

namespace UDT
{

namespace
{

/// The first distinct dotted spelling stays inline because it is by far the
/// common case. A flat set is allocated only when one boundary pass sees more
/// than one distinct spelling. The views remain valid because the proof keeps
/// the complete AST alive for the whole validation pass.
class StringCastTargetMemo
{
public:
    void markPotentialSemanticSinkCandidate() noexcept { has_potential_semantic_sink_candidate = true; }
    bool hasPotentialSemanticSinkCandidate() const noexcept { return has_potential_semantic_sink_candidate; }
    void markPotentialContextualSinkCandidate(QueryResultCacheContextualSinkCandidate candidate) noexcept
    {
        potential_contextual_sink_candidates |= queryResultCacheContextualSinkCandidateBit(candidate);
    }
    QueryResultCacheContextualSinkCandidateMask getPotentialContextualSinkCandidates() const noexcept
    {
        return potential_contextual_sink_candidates;
    }
    void markObservedStorageReference() noexcept
    {
        has_potential_storage_reference = true;
        has_observed_storage_reference = true;
    }
    bool hasPotentialStorageReference() const noexcept { return has_potential_storage_reference; }
    bool hasObservedStorageReference() const noexcept { return has_observed_storage_reference; }
    void markPotentialQueryResultCacheUse() noexcept { has_potential_query_result_cache_use = true; }
    bool hasPotentialQueryResultCacheUse() const noexcept { return has_potential_query_result_cache_use; }
    bool contains(std::string_view type_name) const
    {
        if (!first_target)
            return false;
        if (*first_target == type_name)
            return true;
        return additional_targets && additional_targets->contains(type_name);
    }

    void remember(std::string_view type_name)
    {
        if (!first_target)
        {
            first_target = type_name;
            return;
        }
        if (*first_target == type_name)
            return;
        if (!additional_targets)
            additional_targets.emplace();
        additional_targets->insert(type_name);
    }

private:
    std::optional<std::string_view> first_target;
    std::optional<absl::flat_hash_set<std::string_view, StringViewHash>> additional_targets;
    bool has_potential_semantic_sink_candidate = false;
    bool has_potential_storage_reference = false;
    bool has_observed_storage_reference = false;
    bool has_potential_query_result_cache_use = false;
    QueryResultCacheContextualSinkCandidateMask potential_contextual_sink_candidates = 0;
};

[[noreturn]] void throwStructuredCastTargetBoundary(const ASTCastTarget & target)
{
    const auto & type = target.getType();
    if (!type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A structured UDT CAST has an empty target type");
    if (DataTypeFactory::instance().hasQualifiedBuiltInCollision(*type))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "A qualified user-defined type reference cannot use a registered built-in family or alias");
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "CAST to a user-defined type cannot be resolved by this execution path");
}

bool isPublicCastFunctionName(const String & name)
{
    switch (name.size())
    {
        case 4:
            if (name[0] != 'C' && name[0] != 'c')
                return false;
            return name == "CAST" || equalsCaseInsensitive(name, "CAST");
        case 12:
            if (name[0] != 'A' && name[0] != 'a')
                return false;
            return name == "accurateCast" || equalsCaseInsensitive(name, "accurateCast");
        case 18:
            if (name[0] != 'A' && name[0] != 'a')
                return false;
            return name == "accurateCastOrNull" || equalsCaseInsensitive(name, "accurateCastOrNull");
        case 21:
            if (name[0] != 'A' && name[0] != 'a')
                return false;
            return name == "accurateCastOrDefault" || equalsCaseInsensitive(name, "accurateCastOrDefault");
        default: return false;
    }
}

bool hasQualifiedCandidateInExactTypeStringSlot(const StoredObjectTypeStringSlotClassification & slot)
{
    if (slot.status != StoredObjectTypeStringSlotStatus::ExactExpression || !slot.expression)
        return false;
    const auto * literal = slot.expression->as<ASTLiteral>();
    return literal && literal->value.getType() == Field::Types::String
        && hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>());
}

void inspectPotentialUDTSemanticSinkCandidate(const IAST & ast, StringCastTargetMemo & memo)
{
    /// When this hook is reached from the mandatory bounded AST-size walk,
    /// collecting the cache/sink bits is constant work piggybacked on a visit
    /// which already has to happen.  The option controls only whether an
    /// otherwise-unnecessary walk is started for an unlimited AST; gating the
    /// hook itself would miss an explicit `SETTINGS use_query_cache` carried by
    /// a nested query node.
    if (const auto * table_expression = ast.as<ASTTableExpression>();
        table_expression && (table_expression->database_and_table_name || table_expression->table_function))
    {
        memo.markObservedStorageReference();
        if (const auto * table_function = table_expression->table_function ? table_expression->table_function->as<ASTFunction>() : nullptr;
            table_function)
        {
            const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*table_function);
            if (tree.status == StoredObjectTableFunctionTypeStringTreeStatus::Complete && tree.schema_owner
                && hasQualifiedCandidateInExactTypeStringSlot(tree.schema_slot))
            {
                memo.markPotentialSemanticSinkCandidate();
            }
        }
    }
    if (const auto * settings = ast.as<ASTSetQuery>(); settings && settings->changes.tryGet("use_query_cache"))
        memo.markPotentialQueryResultCacheUse();
    if (const auto * settings = ast.as<ASTSetQuery>())
    {
        for (const auto & change : settings->changes)
        {
            if (tryGetStoredSettingTypeStringSlotContract(change.name) && change.value.getType() == Field::Types::String
                && hasQualifiedTypeReferenceCandidate(change.value.safeGet<String>()))
            {
                memo.markPotentialSemanticSinkCandidate();
                break;
            }
        }
    }
    if (typeid(ast) == typeid(ASTCastTarget))
    {
        memo.markPotentialSemanticSinkCandidate();
        return;
    }
    if (typeid(ast) != typeid(ASTFunction))
        return;

    const auto & function = static_cast<const ASTFunction &>(ast);
    if (hasQualifiedCandidateInExactTypeStringSlot(classifyStoredExpressionTypeStringSlot(function)))
        memo.markPotentialSemanticSinkCandidate();
    if (function.getKind() == ASTFunction::Kind::ORDINARY_FUNCTION && !function.parameters && function.arguments
        && function.arguments->children.size() == 2)
    {
        if (equalsCaseInsensitive(function.name, "equals") || equalsCaseInsensitive(function.name, "notEquals"))
            memo.markPotentialContextualSinkCandidate(QueryResultCacheContextualSinkCandidate::Equality);
        else if (function.name == "in" || function.name == "notIn" || function.name == "globalIn" || function.name == "globalNotIn")
            memo.markPotentialContextualSinkCandidate(QueryResultCacheContextualSinkCandidate::In);
        else if (equalsCaseInsensitive(function.name, "has") || equalsCaseInsensitive(function.name, "hasAny"))
            memo.markPotentialContextualSinkCandidate(QueryResultCacheContextualSinkCandidate::Has);
    }

    if (memo.hasPotentialSemanticSinkCandidate())
        return;
    if (!isPublicCastFunctionName(function.name) || function.parameters || !function.arguments || function.arguments->children.size() < 2)
        return;
    const auto & target_argument = function.arguments->children[1];
    if (target_argument && target_argument->as<ASTCastTarget>())
    {
        memo.markPotentialSemanticSinkCandidate();
        return;
    }
    const auto * literal = target_argument ? target_argument->as<ASTLiteral>() : nullptr;
    if (!literal)
    {
        /// Exact public CAST accepts any expression which resolves to a
        /// constant String.  The mandatory pre-analysis walk cannot prove
        /// whether that expression folds to a qualified type spelling, so it
        /// must reach the analyzer before query-result-cache admission.  The
        /// accurate* families remain rejection-only and never enter this route.
        if (equalsCaseInsensitive(function.name, "CAST"))
            memo.markPotentialSemanticSinkCandidate();
        return;
    }
    if (literal->value.getType() == Field::Types::String && hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>()))
        memo.markPotentialSemanticSinkCandidate();
}

void rejectUnsupportedPersistedTypeStringCandidate(const ASTFunction & function, bool public_cast_has_dedicated_validation)
{
    /// Public CAST families have a stricter dedicated path below, including
    /// structured targets and built-in collision diagnostics.
    if (public_cast_has_dedicated_validation && isPublicCastFunctionName(function.name))
        return;

    const auto slot = classifyStoredExpressionTypeStringSlot(function);
    if (slot.status != StoredObjectTypeStringSlotStatus::ExactExpression || !slot.expression)
        return;
    const auto * literal = slot.expression->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String
        || !hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>()))
        return;

    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "A qualified user-defined type string in {} cannot be persisted by this execution path",
        function.name);
}

void rejectUnsupportedPersistedTypeStringSettingCandidate(const ASTSetQuery & settings)
{
    for (const auto & change : settings.changes)
    {
        if (!tryGetStoredSettingTypeStringSlotContract(change.name) || change.value.getType() != Field::Types::String
            || !hasQualifiedTypeReferenceCandidate(change.value.safeGet<String>()))
            continue;
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "A qualified user-defined type string in setting {} cannot be persisted by this execution path",
            change.name);
    }
}

void validatePublicStringCast(const ASTFunction & function, StringCastTargetMemo & memo)
{
    if (isPublicCastFunctionName(function.name) && !function.parameters && function.arguments && function.arguments->children.size() >= 2)
    {
        const auto & target_argument = function.arguments->children[1];
        if (const auto * structured_target = target_argument ? target_argument->as<ASTCastTarget>() : nullptr)
            throwStructuredCastTargetBoundary(*structured_target);
        const auto * literal = target_argument ? target_argument->as<ASTLiteral>() : nullptr;
        if (!literal || literal->value.getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument to {} must be a constant string describing type.", function.name);

        const auto & type_name = literal->value.safeGet<String>();
        const auto first_dot = type_name.find('.');
        if (first_dot == String::npos || memo.contains(type_name))
            return;

        /// A trailing first dot cannot have a local family name, and there is
        /// no earlier separator to classify. Keep malformed CAST strings
        /// deferred without rescanning an arbitrarily long prefix.
        if (first_dot + 1 == type_name.size())
        {
            memo.remember(type_name);
            return;
        }

        /// A dot in an Enum label, quoted identifier, string literal, or
        /// comment is not a qualified type family. Malformed spellings which
        /// cannot contain a qualified family remain deferred as before.
        if (!hasQualifiedTypeReferenceCandidate(type_name))
        {
            memo.remember(type_name);
            return;
        }

        try
        {
            DataTypeFactory::instance().rejectQualifiedUDTSyntax(type_name);
        }
        catch (const Exception & exception)
        {
            if (exception.code() == ErrorCodes::BAD_ARGUMENTS || exception.code() == ErrorCodes::SUPPORT_IS_DISABLED)
                throw;
            /// Preserve pre-existing deferred validation for syntactically
            /// malformed `CAST` strings; resource failures propagate. Cache
            /// the deferred result so an identical target is parsed once.
            if (exception.code() != ErrorCodes::SYNTAX_ERROR)
                throw;
            memo.remember(type_name);
            return;
        }

        memo.remember(type_name);
    }
}

template <bool inspect_constant_string_targets>
void validateUDTCastNode(const IAST & ast, const std::type_info & ast_type, StringCastTargetMemo & memo)
{
    if (ast_type == typeid(ASTSetQuery))
        rejectUnsupportedPersistedTypeStringSettingCandidate(static_cast<const ASTSetQuery &>(ast));
    if (ast_type == typeid(ASTFunction))
        rejectUnsupportedPersistedTypeStringCandidate(static_cast<const ASTFunction &>(ast), inspect_constant_string_targets);

    if constexpr (inspect_constant_string_targets)
    {
        if (ast_type == typeid(ASTFunction))
        {
            validatePublicStringCast(static_cast<const ASTFunction &>(ast), memo);
            return;
        }
    }

    if (ast_type == typeid(ASTCastTarget))
        throwStructuredCastTargetBoundary(static_cast<const ASTCastTarget &>(ast));
}

template <typename Visitor>
void visitRootExecutionASTsOutsideChildren(const IAST & ast, Visitor && visitor)
{
    const auto visit_if_outside_children = [&](const ASTPtr & hidden_ast)
    {
        if (!hidden_ast)
            return;
        for (const auto & child : ast.children)
            if (child.get() == hidden_ast.get())
                return;
        visitor(*hidden_ast);
    };

    const auto & ast_type = typeid(ast);
    if (ast_type == typeid(ASTCreateRowPolicyQuery))
    {
        const auto & row_policy = static_cast<const ASTCreateRowPolicyQuery &>(ast);
        for (const auto & [_, filter] : row_policy.filters)
            visit_if_outside_children(filter);
    }
    else if (ast_type == typeid(ASTCreateMaskingPolicyQuery))
    {
        const auto & masking_policy = static_cast<const ASTCreateMaskingPolicyQuery &>(ast);
        visit_if_outside_children(masking_policy.update_assignments);
        visit_if_outside_children(masking_policy.where_condition);
    }
    else if (ast_type == typeid(ASTBackupQuery))
    {
        const auto & backup = static_cast<const ASTBackupQuery &>(ast);
        for (const auto & element : backup.elements)
            if (element.partitions)
                for (const auto & partition : *element.partitions)
                    visit_if_outside_children(partition);

        visit_if_outside_children(backup.cluster_host_ids);
    }
}

template <typename Visitor>
void visitTTLExecutionASTsOutsideChildren(const ASTTTLElement & ttl, Visitor && visitor)
{
    const auto visit_if_outside_children = [&](const ASTPtr & hidden_ast)
    {
        if (!hidden_ast)
            return;
        for (const auto & child : ttl.children)
            if (child.get() == hidden_ast.get())
                return;
        visitor(*hidden_ast);
    };

    for (const auto & expression : ttl.group_by_key)
        visit_if_outside_children(expression);
    for (const auto & expression : ttl.group_by_assignments)
        visit_if_outside_children(expression);
    visit_if_outside_children(ttl.recompression_codec);
}

bool mayContainTTLElements(const IAST & root)
{
    const auto & root_type = typeid(root);
    return root_type == typeid(ASTCreateQuery) || root_type == typeid(ASTAlterQuery);
}

bool mayContainNestedStatements(const IAST & root)
{
    const auto & root_type = typeid(root);
    /// These are the audited statement containers whose interpreters format a
    /// child statement and pass it through `executeQuery` again.
    return root_type == typeid(ASTParallelWithQuery) || root_type == typeid(ASTExecuteAsQuery);
}

bool mayContainExecutionASTsOutsideChildren(const IAST & root)
{
    const auto & root_type = typeid(root);
    return root_type == typeid(ASTCreateRowPolicyQuery) || root_type == typeid(ASTCreateMaskingPolicyQuery)
        || root_type == typeid(ASTBackupQuery) || mayContainTTLElements(root) || mayContainNestedStatements(root);
}

const IAST * getExecutionRootOutputSettings(const IAST & root) noexcept
{
    /// SETTINGS in the output suffix of the statement being executed affect
    /// only that execution. The same ASTSetQuery shape nested in a stored
    /// SELECT (for example a view definition) is durable and must keep the
    /// ordinary fail-closed validation below.
    const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(&root);
    return query_with_output ? query_with_output->settings_ast.get() : nullptr;
}

bool isNestedStatementChild(const IAST & root, const IAST & child)
{
    if (typeid(root) == typeid(ASTParallelWithQuery))
        return true;
    if (typeid(root) == typeid(ASTExecuteAsQuery))
        return static_cast<const ASTExecuteAsQuery &>(root).subquery.get() == std::addressof(child);
    return false;
}

const IAST * getAnalyzerOwnedInsertSelect(const IAST & root, const UDTExecutionBoundaryOptions & options)
{
    if (!options.allow_experimental_analyzer || !options.allow_experimental_user_defined_types)
        return nullptr;

    if (typeid(root) != typeid(ASTInsertQuery))
        return nullptr;

    /// `INSERT SELECT` is the only audited non-`SELECT` handoff to `QueryAnalyzer`.
    /// Do not generalize this to arbitrary nested `SELECT` statements: `CREATE`/view/`EXPLAIN`
    /// and other owners remain fail-closed until their lifecycle is inventoried.
    return static_cast<const ASTInsertQuery &>(root).select.get();
}

struct StoredObjectDDLSelectCandidate
{
    const IAST * owner = nullptr;
    const IAST * select = nullptr;
    bool is_alter = false;
};

struct AnalyzerOwnedExecutionChild
{
    const IAST * owner = nullptr;
    const IAST * child = nullptr;
    bool inspect_candidates = false;
};

std::optional<StoredObjectDDLSelectCandidate>
getStoredObjectDDLSelectCandidate(const IAST & root, const UDTExecutionBoundaryOptions & options)
{
    if (!options.allow_experimental_analyzer || !options.allow_experimental_user_defined_types)
        return std::nullopt;

    if (const auto * create = root.as<ASTCreateQuery>())
    {
        const bool supported_kind = !create->is_dictionary
            && (create->is_ordinary_view || create->is_materialized_view || (!create->isView() && !create->getTable().empty()));
        const bool supported_surface = supported_kind && create->select && create->select->as<ASTSelectWithUnionQuery>() && !create->attach
            && !create->isTemporary() && !create->if_not_exists && !create->replace_view && !create->replace_table
            && !create->create_or_replace && !create->has_attach_from_path && !create->attach_short_syntax
            && !create->attach_as_replicated.has_value() && create->cluster.empty() && !create->as_table_function && !create->is_clone_as
            && create->as_table.empty() && (!create->is_materialized_view || !create->refresh_strategy);
        if (!supported_surface)
            return std::nullopt;
        const auto direct_owners = std::count_if(
            root.children.begin(), root.children.end(), [&](const ASTPtr & child) { return child.get() == create->select; });
        if (direct_owners != 1)
            return std::nullopt;
        return StoredObjectDDLSelectCandidate{.owner = create, .select = create->select, .is_alter = false};
    }

    const auto * alter = root.as<ASTAlterQuery>();
    if (!alter || alter->alter_object != ASTAlterQuery::AlterObjectType::TABLE || !alter->cluster.empty() || !alter->command_list
        || std::count_if(
               root.children.begin(), root.children.end(), [&](const ASTPtr & child) { return child.get() == alter->command_list; })
            != 1)
    {
        return std::nullopt;
    }

    if (alter->command_list->children.size() != 1)
        return std::nullopt;

    const ASTAlterCommand * modify_query = nullptr;
    for (const auto & child : alter->command_list->children)
    {
        const auto * command = child ? child->as<ASTAlterCommand>() : nullptr;
        if (!command)
            return std::nullopt;
        if (command->type != ASTAlterCommand::MODIFY_QUERY)
            return std::nullopt;
        if (modify_query)
            return std::nullopt;
        modify_query = command;
    }
    if (!modify_query || !modify_query->select || !modify_query->select->as<ASTSelectWithUnionQuery>()
        || std::count_if(
               modify_query->children.begin(),
               modify_query->children.end(),
               [&](const ASTPtr & child) { return child.get() == modify_query->select; })
            != 1)
    {
        return std::nullopt;
    }
    return StoredObjectDDLSelectCandidate{.owner = modify_query, .select = modify_query->select, .is_alter = true};
}

std::optional<AnalyzerOwnedExecutionChild> getAnalyzerOwnedExecutionChild(const IAST & root, const UDTExecutionBoundaryOptions & options)
{
    if (const auto * insert_select = getAnalyzerOwnedInsertSelect(root, options))
    {
        const auto direct_owners
            = std::count_if(root.children.begin(), root.children.end(), [&](const ASTPtr & child) { return child.get() == insert_select; });
        if (direct_owners == 1)
            return AnalyzerOwnedExecutionChild{.owner = &root, .child = insert_select};
        return std::nullopt;
    }
    const auto stored_object = getStoredObjectDDLSelectCandidate(root, options);
    if (!stored_object)
        return std::nullopt;
    return AnalyzerOwnedExecutionChild{
        .owner = stored_object->owner,
        .child = stored_object->select,
        .inspect_candidates = true,
    };
}

bool requiresUDTExecutionBoundaryValidationImpl(const IAST & root) noexcept
{
    return !root.as<ASTSelectQuery>() && !root.as<ASTSelectWithUnionQuery>() && !root.as<ASTSelectIntersectExceptQuery>();
}

void inspectRegularASTForBoundaryCandidates(const IAST & ast, StringCastTargetMemo & memo);

template <bool inspect_constant_string_targets, bool inspect_ttl_elements>
void validateUDTCastsInRegularTree(
    const IAST & ast, StringCastTargetMemo & memo, const AnalyzerOwnedExecutionChild * analyzer_owned_child = nullptr)
{
    checkStackSize();
    if (ast.children.empty())
    {
        /// ASTSetQuery keeps setting values in `changes`, not in `children`.
        /// It is therefore a semantically non-empty leaf for the closed
        /// persisted type-string registry and must cross the same fail-closed
        /// check as a root settings clause.
        if (typeid(ast) == typeid(ASTSetQuery))
            validateUDTCastNode<inspect_constant_string_targets>(ast, typeid(ast), memo);
        if constexpr (!inspect_ttl_elements)
            return;
        if (typeid(ast) != typeid(ASTTTLElement))
            return;
    }

    const auto & ast_type = typeid(ast);
    validateUDTCastNode<inspect_constant_string_targets>(ast, ast_type, memo);

    if constexpr (inspect_ttl_elements)
    {
        if (ast_type == typeid(ASTTTLElement))
            visitTTLExecutionASTsOutsideChildren(
                static_cast<const ASTTTLElement &>(ast),
                [&](const IAST & hidden_ast) { validateUDTCastsInRegularTree<inspect_constant_string_targets, false>(hidden_ast, memo); });
    }

    for (const auto & child : ast.children)
    {
        if (analyzer_owned_child && std::addressof(ast) == analyzer_owned_child->owner && child.get() == analyzer_owned_child->child)
        {
            if (analyzer_owned_child->inspect_candidates)
                inspectRegularASTForBoundaryCandidates(*child, memo);
            continue;
        }
        validateUDTCastsInRegularTree<inspect_constant_string_targets, inspect_ttl_elements>(*child, memo, analyzer_owned_child);
    }
}

template <bool inspect_constant_string_targets>
void validateRootExecutionASTsOutsideChildren(const IAST & root, StringCastTargetMemo & memo)
{
    visitRootExecutionASTsOutsideChildren(
        root, [&](const IAST & hidden_ast) { validateUDTCastsInRegularTree<inspect_constant_string_targets, false>(hidden_ast, memo); });
}

void validateUDTCastsBeforeSideEffects(const IAST & root, const UDTExecutionBoundaryOptions & options, StringCastTargetMemo & memo);

template <bool inspect_constant_string_targets, bool inspect_ttl_elements, bool inspect_nested_statements>
void validateUDTCastsBeforeSideEffectsImpl(
    const IAST & root,
    const UDTExecutionBoundaryOptions & options,
    const AnalyzerOwnedExecutionChild * analyzer_owned_child,
    StringCastTargetMemo & memo)
{
    checkStackSize();
    const IAST * execution_root_output_settings = getExecutionRootOutputSettings(root);
    validateUDTCastNode<inspect_constant_string_targets>(root, typeid(root), memo);
    validateRootExecutionASTsOutsideChildren<inspect_constant_string_targets>(root, memo);

    for (const auto & child : root.children)
    {
        if constexpr (inspect_nested_statements)
        {
            if (isNestedStatementChild(root, *child))
            {
                auto nested_options = options;
                /// A statement container can start a sibling before this child
                /// reaches its analyzer. Validate nested analyzer-owned trees
                /// fail-closed at the outer pre-side-effect boundary.
                nested_options.allow_experimental_analyzer = false;
                validateUDTCastsBeforeSideEffects(*child, nested_options, memo);
                continue;
            }
        }

        if (child.get() == execution_root_output_settings)
        {
            if (options.inspect_query_result_cache_candidates)
                inspectRegularASTForBoundaryCandidates(*child, memo);
            continue;
        }

        if (analyzer_owned_child && std::addressof(root) == analyzer_owned_child->owner && child.get() == analyzer_owned_child->child)
        {
            if (analyzer_owned_child->inspect_candidates)
                inspectRegularASTForBoundaryCandidates(*child, memo);
            continue;
        }
        validateUDTCastsInRegularTree<inspect_constant_string_targets, inspect_ttl_elements>(*child, memo, analyzer_owned_child);
    }
}

template <bool inspect_constant_string_targets>
void validateUDTCastsBeforeSideEffectsWithTTLPolicy(
    const IAST & root,
    const UDTExecutionBoundaryOptions & options,
    const AnalyzerOwnedExecutionChild * analyzer_owned_child,
    StringCastTargetMemo & memo)
{
    if (mayContainNestedStatements(root))
        validateUDTCastsBeforeSideEffectsImpl<inspect_constant_string_targets, false, true>(root, options, analyzer_owned_child, memo);
    else if (mayContainTTLElements(root))
        validateUDTCastsBeforeSideEffectsImpl<inspect_constant_string_targets, true, false>(root, options, analyzer_owned_child, memo);
    else
        validateUDTCastsBeforeSideEffectsImpl<inspect_constant_string_targets, false, false>(root, options, analyzer_owned_child, memo);
}

void validateUDTCastsBeforeSideEffects(const IAST & root, const UDTExecutionBoundaryOptions & options, StringCastTargetMemo & memo)
{
    const auto analyzer_owned_child = getAnalyzerOwnedExecutionChild(root, options);
    if (options.allow_experimental_user_defined_types)
        validateUDTCastsBeforeSideEffectsWithTTLPolicy<true>(root, options, analyzer_owned_child ? &*analyzer_owned_child : nullptr, memo);
    else
        validateUDTCastsBeforeSideEffectsWithTTLPolicy<false>(root, options, analyzer_owned_child ? &*analyzer_owned_child : nullptr, memo);
}

class ASTElementBudget
{
public:
    explicit ASTElementBudget(size_t maximum_) noexcept
        : remaining(maximum_)
        , maximum(maximum_)
    {
    }

    void consume()
    {
        if (remaining == 0)
            throw Exception(ErrorCodes::TOO_BIG_AST, "AST is too big. Maximum: {}", maximum);
        --remaining;
    }

private:
    size_t remaining;
    const size_t maximum;
};

void checkRegularASTSize(const IAST & ast, ASTElementBudget & budget, StringCastTargetMemo * memo = nullptr)
{
    checkStackSize();
    budget.consume();
    if (memo)
        inspectPotentialUDTSemanticSinkCandidate(ast, *memo);
    for (const auto & child : ast.children)
        checkRegularASTSize(*child, budget, memo);
}

void inspectRegularASTForBoundaryCandidates(const IAST & ast, StringCastTargetMemo & memo)
{
    checkStackSize();
    inspectPotentialUDTSemanticSinkCandidate(ast, memo);
    for (const auto & child : ast.children)
        inspectRegularASTForBoundaryCandidates(*child, memo);
}

template <bool inspect_constant_string_targets, bool inspect_ttl_elements>
void inspectUDTExecutionSubtreeAndSize(
    const IAST & ast,
    ASTElementBudget & budget,
    StringCastTargetMemo & memo,
    const AnalyzerOwnedExecutionChild * analyzer_owned_child = nullptr)
{
    checkStackSize();
    budget.consume();
    if (ast.children.empty())
    {
        /// See `validateUDTCastsInRegularTree`: settings payloads live outside
        /// the ordinary child vector even though the AST node itself is a
        /// leaf.
        if (typeid(ast) == typeid(ASTSetQuery))
            validateUDTCastNode<inspect_constant_string_targets>(ast, typeid(ast), memo);
        if constexpr (!inspect_ttl_elements)
            return;
        if (typeid(ast) != typeid(ASTTTLElement))
            return;
    }

    const auto & ast_type = typeid(ast);
    validateUDTCastNode<inspect_constant_string_targets>(ast, ast_type, memo);

    if constexpr (inspect_ttl_elements)
    {
        if (ast_type == typeid(ASTTTLElement))
            visitTTLExecutionASTsOutsideChildren(
                static_cast<const ASTTTLElement &>(ast),
                [&](const IAST & hidden_ast)
                { inspectUDTExecutionSubtreeAndSize<inspect_constant_string_targets, false>(hidden_ast, budget, memo); });
    }

    for (const auto & child : ast.children)
    {
        if (analyzer_owned_child && std::addressof(ast) == analyzer_owned_child->owner && child.get() == analyzer_owned_child->child)
        {
            checkRegularASTSize(*child, budget, &memo);
            continue;
        }
        inspectUDTExecutionSubtreeAndSize<inspect_constant_string_targets, inspect_ttl_elements>(
            *child, budget, memo, analyzer_owned_child);
    }
}

void inspectUDTExecutionStatementAndSize(
    const IAST & root,
    ASTElementBudget & budget,
    const UDTExecutionBoundaryOptions & options,
    bool allow_analyzer_handoff,
    StringCastTargetMemo & memo);

template <bool inspect_constant_string_targets, bool inspect_ttl_elements, bool inspect_nested_statements>
void inspectUDTExecutionRootAndSize(
    const IAST & root,
    ASTElementBudget & budget,
    const UDTExecutionBoundaryOptions & options,
    const AnalyzerOwnedExecutionChild * analyzer_owned_child,
    StringCastTargetMemo & memo)
{
    checkStackSize();
    const IAST * execution_root_output_settings = getExecutionRootOutputSettings(root);
    budget.consume();
    validateUDTCastNode<inspect_constant_string_targets>(root, typeid(root), memo);

    visitRootExecutionASTsOutsideChildren(
        root,
        [&](const IAST & hidden_ast)
        { inspectUDTExecutionSubtreeAndSize<inspect_constant_string_targets, false>(hidden_ast, budget, memo); });

    for (const auto & child : root.children)
    {
        if constexpr (inspect_nested_statements)
        {
            if (isNestedStatementChild(root, *child))
            {
                inspectUDTExecutionStatementAndSize(*child, budget, options, false, memo);
                continue;
            }
        }

        if (child.get() == execution_root_output_settings)
        {
            checkRegularASTSize(*child, budget, &memo);
            continue;
        }

        if (analyzer_owned_child && std::addressof(root) == analyzer_owned_child->owner && child.get() == analyzer_owned_child->child)
            checkRegularASTSize(*child, budget, &memo);
        else
            inspectUDTExecutionSubtreeAndSize<inspect_constant_string_targets, inspect_ttl_elements>(
                *child, budget, memo, analyzer_owned_child);
    }
}

template <bool inspect_constant_string_targets>
void inspectUDTExecutionRootAndSizeWithTTLPolicy(
    const IAST & root,
    ASTElementBudget & budget,
    const UDTExecutionBoundaryOptions & options,
    const AnalyzerOwnedExecutionChild * analyzer_owned_child,
    StringCastTargetMemo & memo)
{
    if (mayContainNestedStatements(root))
    {
        inspectUDTExecutionRootAndSize<inspect_constant_string_targets, false, true>(root, budget, options, analyzer_owned_child, memo);
        return;
    }
    if (mayContainTTLElements(root))
    {
        inspectUDTExecutionRootAndSize<inspect_constant_string_targets, true, false>(root, budget, options, analyzer_owned_child, memo);
        return;
    }
    inspectUDTExecutionRootAndSize<inspect_constant_string_targets, false, false>(root, budget, options, analyzer_owned_child, memo);
}

void inspectUDTExecutionStatementAndSize(
    const IAST & root,
    ASTElementBudget & budget,
    const UDTExecutionBoundaryOptions & options,
    bool allow_analyzer_handoff,
    StringCastTargetMemo & memo)
{
    if (allow_analyzer_handoff && !requiresUDTExecutionBoundaryValidationImpl(root))
    {
        checkRegularASTSize(root, budget, &memo);
        return;
    }

    auto effective_options = options;
    if (!allow_analyzer_handoff)
        effective_options.allow_experimental_analyzer = false;

    const auto analyzer_owned_child = getAnalyzerOwnedExecutionChild(root, effective_options);
    if (effective_options.allow_experimental_user_defined_types)
    {
        inspectUDTExecutionRootAndSizeWithTTLPolicy<true>(
            root, budget, effective_options, analyzer_owned_child ? &*analyzer_owned_child : nullptr, memo);
        return;
    }
    inspectUDTExecutionRootAndSizeWithTTLPolicy<false>(
        root, budget, effective_options, analyzer_owned_child ? &*analyzer_owned_child : nullptr, memo);
}

void checkExecutionASTSize(const IAST & ast, ASTElementBudget & budget)
{
    checkStackSize();
    budget.consume();

    visitRootExecutionASTsOutsideChildren(ast, [&](const IAST & hidden_ast) { checkExecutionASTSize(hidden_ast, budget); });

    if (typeid(ast) == typeid(ASTTTLElement))
        visitTTLExecutionASTsOutsideChildren(
            static_cast<const ASTTTLElement &>(ast), [&](const IAST & hidden_ast) { checkExecutionASTSize(hidden_ast, budget); });

    for (const auto & child : ast.children)
        checkExecutionASTSize(*child, budget);
}

void checkSizeForBoundaryErrorPriority(const IAST & root, size_t max_ast_elements)
{
    ASTElementBudget budget(max_ast_elements);
    if (mayContainExecutionASTsOutsideChildren(root))
        checkExecutionASTSize(root, budget);
    else
        checkRegularASTSize(root, budget);
}

}

UDTStoredObjectDDLSelectBoundaryHandoff::UDTStoredObjectDDLSelectBoundaryHandoff(ASTPtr root_, const IAST * select_, Owner owner_)
    : root(std::move(root_))
    , select(select_)
    , owner(owner_)
{
    if (!root || !select)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot construct an incomplete stored-object DDL boundary handoff");
}

void UDTStoredObjectDDLSelectBoundaryHandoff::consume(const IAST & root_, const IAST & select_, Owner owner_)
{
    std::lock_guard lock(mutex);
    if (consumed || !root || !select)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stored-object DDL boundary handoff was already consumed");
    if (root.get() != std::addressof(root_) || select != std::addressof(select_) || owner != owner_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stored-object DDL boundary handoff belongs to a different owner or SELECT child");

    consumed = true;
    root.reset();
    select = nullptr;
}

void UDTStoredObjectDDLSelectBoundaryHandoff::consumeForCreate(const ASTCreateQuery & root_, const IAST & select_)
{
    consume(root_, select_, Owner::Create);
}

void UDTStoredObjectDDLSelectBoundaryHandoff::consumeForAlter(const ASTAlterQuery & root_, const IAST & select_)
{
    consume(root_, select_, Owner::AlterModifyQuery);
}

UDTExecutionBoundaryProof::UDTExecutionBoundaryProof(
    ASTPtr root_,
    UDTExecutionBoundaryOptions options_,
    bool has_potential_udt_semantic_sink_candidate_,
    bool has_potential_storage_reference_,
    bool has_observed_storage_reference_,
    bool has_potential_query_result_cache_use_,
    QueryResultCacheContextualSinkCandidateMask potential_query_result_cache_contextual_sink_candidates_,
    const IAST * stored_object_ddl_select_,
    bool stored_object_ddl_select_is_alter_)
    : root(std::move(root_))
    , options(options_)
    , has_potential_udt_semantic_sink_candidate(has_potential_udt_semantic_sink_candidate_)
    , has_potential_storage_reference(has_potential_storage_reference_)
    , has_observed_storage_reference(has_observed_storage_reference_)
    , has_potential_query_result_cache_use(has_potential_query_result_cache_use_)
    , potential_query_result_cache_contextual_sink_candidates(potential_query_result_cache_contextual_sink_candidates_)
{
    if (stored_object_ddl_select_)
    {
        const auto owner = stored_object_ddl_select_is_alter_ ? UDTStoredObjectDDLSelectBoundaryHandoff::Owner::AlterModifyQuery
                                                              : UDTStoredObjectDDLSelectBoundaryHandoff::Owner::Create;
        stored_object_ddl_select_handoff = UDTStoredObjectDDLSelectBoundaryHandoff::Ptr(
            new UDTStoredObjectDDLSelectBoundaryHandoff(root, stored_object_ddl_select_, owner));
    }
}

UDTStoredObjectDDLSelectBoundaryHandoff::Ptr
UDTExecutionBoundaryProof::consumeForDispatch(const IAST & dispatch_root, const UDTExecutionBoundaryOptions & current_options)
{
    if (!root || root.get() != std::addressof(dispatch_root))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT execution-boundary proof belongs to a different AST root");
    if (options.allow_experimental_analyzer != current_options.allow_experimental_analyzer
        || options.allow_experimental_user_defined_types != current_options.allow_experimental_user_defined_types
        /// The parsed AST may have opted a nested query into caching without
        /// changing the top-level Context setting. In that case validation
        /// deliberately used the stricter candidate walk while dispatch can
        /// only reconstruct the Context bits. The reverse transition would
        /// mean candidates were not inspected and remains invalid.
        || (!options.inspect_query_result_cache_candidates && current_options.inspect_query_result_cache_candidates))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UDT execution-boundary settings changed before interpreter dispatch");
    root.reset();
    return std::move(stored_object_ddl_select_handoff);
}

bool requiresUDTExecutionBoundaryValidation(const IAST & root) noexcept
{
    return requiresUDTExecutionBoundaryValidationImpl(root);
}

UDTExecutionBoundaryProof validateUDTExecutionBoundary(const ASTPtr & root, const UDTExecutionBoundaryOptions & options)
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate a null AST at the UDT execution boundary");
    StringCastTargetMemo memo;
    if (requiresUDTExecutionBoundaryValidation(*root))
    {
        validateUDTCastsBeforeSideEffects(*root, options, memo);
        if (options.inspect_query_result_cache_candidates)
            if (const auto * analyzer_owned_child = getAnalyzerOwnedInsertSelect(*root, options))
                inspectRegularASTForBoundaryCandidates(*analyzer_owned_child, memo);
    }
    else if (options.inspect_query_result_cache_candidates)
        /// `max_ast_elements = 0` means unlimited, not "unknown AST". Perform
        /// the same allocation-free candidate inspection as the bounded size
        /// walk, without consuming an element budget or rejecting any size.
        inspectRegularASTForBoundaryCandidates(*root, memo);
    const auto stored_object = getStoredObjectDDLSelectCandidate(*root, options);
    const IAST * stored_object_select = stored_object && memo.hasPotentialSemanticSinkCandidate() ? stored_object->select : nullptr;
    return UDTExecutionBoundaryProof(
        root,
        options,
        memo.hasPotentialSemanticSinkCandidate(),
        memo.hasPotentialStorageReference(),
        memo.hasObservedStorageReference(),
        memo.hasPotentialQueryResultCacheUse(),
        memo.getPotentialContextualSinkCandidates(),
        stored_object_select,
        stored_object && stored_object->is_alter);
}

UDTExecutionBoundaryProof
validateUDTExecutionBoundaryAndSize(const ASTPtr & root, size_t max_ast_elements, const UDTExecutionBoundaryOptions & options)
{
    if (!root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot validate a null AST at the UDT execution boundary");
    if (!requiresUDTExecutionBoundaryValidation(*root))
    {
        StringCastTargetMemo memo;
        ASTElementBudget budget(max_ast_elements);
        checkRegularASTSize(*root, budget, &memo);
        return UDTExecutionBoundaryProof(
            root,
            options,
            memo.hasPotentialSemanticSinkCandidate(),
            memo.hasPotentialStorageReference(),
            memo.hasObservedStorageReference(),
            memo.hasPotentialQueryResultCacheUse(),
            memo.getPotentialContextualSinkCandidates());
    }

    StringCastTargetMemo memo;
    ASTElementBudget budget(max_ast_elements);
    try
    {
        inspectUDTExecutionStatementAndSize(*root, budget, options, true, memo);
    }
    catch (const Exception & exception)
    {
        if (exception.code() == ErrorCodes::TOO_BIG_AST)
            throw;

        /// Preserve the established size-before-semantics error order. Only a
        /// semantic failure needs the fallback; a size failure has already
        /// completed the one required traversal and is propagated directly.
        const auto boundary_error = std::current_exception();
        checkSizeForBoundaryErrorPriority(*root, max_ast_elements);
        std::rethrow_exception(boundary_error);
    }
    catch (...)
    {
        /// Preserve the established `checkSize`-before-semantics error order
        /// without carrying deferred-error state through every successful node.
        const auto boundary_error = std::current_exception();
        checkSizeForBoundaryErrorPriority(*root, max_ast_elements);
        std::rethrow_exception(boundary_error);
    }

    const auto stored_object = getStoredObjectDDLSelectCandidate(*root, options);
    const IAST * stored_object_select = stored_object && memo.hasPotentialSemanticSinkCandidate() ? stored_object->select : nullptr;
    return UDTExecutionBoundaryProof(
        root,
        options,
        memo.hasPotentialSemanticSinkCandidate(),
        memo.hasPotentialStorageReference(),
        memo.hasObservedStorageReference(),
        memo.hasPotentialQueryResultCacheUse(),
        memo.getPotentialContextualSinkCandidates(),
        stored_object_select,
        stored_object && stored_object->is_alter);
}

}
}
