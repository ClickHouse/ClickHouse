#include <Interpreters/UDT/StoredObjectTypeSupport.h>

#include <Interpreters/UDT/StoredObjectTableFunctionSources.h>
#include <Interpreters/UDT/StoredObjectTypeStringSlots.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/QualifiedTypeReferenceCandidate.h>
#include <Parsers/ASTCastTarget.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUDTReference.h>

#include <Common/StringUtils.h>

#include <algorithm>
#include <array>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using RegistryError = StoredObjectPhysicalizationAdapterRegistryError;

constexpr StoredObjectKindMask table_kind = storedObjectKindMask(StoredObjectKind::Table);
constexpr StoredObjectKindMask view_kind = storedObjectKindMask(StoredObjectKind::View);
constexpr StoredObjectKindMask materialized_view_kind = storedObjectKindMask(StoredObjectKind::MaterializedView);
constexpr StoredObjectKindMask dictionary_kind = storedObjectKindMask(StoredObjectKind::Dictionary);
constexpr StoredObjectKindMask all_stored_object_kinds = table_kind | view_kind | materialized_view_kind | dictionary_kind;
constexpr size_t maximum_physicalization_adapter_registrations = 4;
constexpr size_t maximum_stored_object_admission_work_items = 65'536;
/// Fixed fail-closed ceiling for provenance-set validation. The descriptor
/// count alone is not a byte bound because provenance entries contain digests.
constexpr size_t maximum_stored_object_admission_scratch_bytes = 8ULL << 20;
static_assert(all_stored_object_kinds == (StoredObjectKindMask{1} << maximum_physicalization_adapter_registrations) - 1);

constexpr StoredObjectOccurrenceSiteMask table_column_site
    = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableColumnDeclaration);
constexpr StoredObjectOccurrenceSiteMask view_output_site
    = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::ViewOutputDeclaration);
constexpr StoredObjectOccurrenceSiteMask materialized_view_output_site
    = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration);
constexpr StoredObjectOccurrenceSiteMask dictionary_attribute_site
    = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::DictionaryAttribute);

/// This is intentionally independent from query settings. Parser limits are
/// enforced before this point, while this local ceiling keeps manually built
/// or future AST shapes bounded and fail-closed.
constexpr size_t maximum_structured_udt_scan_nodes = 1U << 20;
constexpr size_t maximum_structured_udt_scan_depth = 256;

void recordTypeStringCandidate(StoredObjectOccurrenceSite occurrence_site, StoredObjectCreateQueryClassification & classification) noexcept
{
    classification.qualified_type_reference_candidate_sites |= storedObjectOccurrenceSiteMask(occurrence_site);
    if (occurrence_site == StoredObjectOccurrenceSite::UnclassifiedTypeString)
        classification.has_unclassified_udt_reference = true;
}

void recordUnresolvedTypeStringSlot(
    StoredObjectOccurrenceSite occurrence_site, StoredObjectCreateQueryClassification & classification) noexcept
{
    classification.type_string_scan_complete = false;
    classification.unresolved_type_string_occurrence_sites |= storedObjectOccurrenceSiteMask(occurrence_site);
}

void scanClassifiedTypeStringSlot(
    const StoredObjectTypeStringSlotClassification & slot, StoredObjectCreateQueryClassification & classification) noexcept
{
    switch (slot.status)
    {
        case StoredObjectTypeStringSlotStatus::Unregistered:
        case StoredObjectTypeStringSlotStatus::NoExplicitSchemaString: return;
        case StoredObjectTypeStringSlotStatus::ExactExpression: {
            const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
            if (!literal || literal->value.getType() != Field::Types::String)
            {
                recordUnresolvedTypeStringSlot(slot.occurrence_site, classification);
                return;
            }
            if (hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>()))
                recordTypeStringCandidate(slot.occurrence_site, classification);
            return;
        }
        case StoredObjectTypeStringSlotStatus::ContextRequired:
        case StoredObjectTypeStringSlotStatus::UnclassifiedLayout:
            recordUnresolvedTypeStringSlot(slot.occurrence_site, classification);
            return;
    }
}

void scanTableFunctionTypeStringTree(const IAST * node, StoredObjectCreateQueryClassification & classification) noexcept
{
    const auto * function = node ? node->as<ASTFunction>() : nullptr;
    if (!function)
        return;

    const auto tree = classifyStoredObjectTableFunctionTypeStringTree(*function);
    if (tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete || !tree.schema_owner
        || (tree.nested_depth != 0
            && classifyStoredObjectTableFunctionSource(tree.schema_owner->name) == StoredObjectTableFunctionSourceProvenance::Unclassified))
    {
        recordUnresolvedTypeStringSlot(StoredObjectOccurrenceSite::TableFunctionSchemaString, classification);
        return;
    }
    scanClassifiedTypeStringSlot(tree.schema_slot, classification);
}

void scanStorageEngineTypeStringSlot(const ASTStorage * storage, StoredObjectCreateQueryClassification & classification) noexcept
{
    if (storage && storage->engine)
        scanClassifiedTypeStringSlot(classifyStorageEngineTypeStringSlot(*storage->engine), classification);
}

void scanStoredExpressionTypeStringSlot(
    const ASTFunction & function,
    StoredObjectCreateQueryClassification & classification,
    StoredObjectKind selected_expression_owner = StoredObjectKind::Unclassified) noexcept
{
    auto slot = classifyStoredExpressionTypeStringSlot(function);
    if (selected_expression_owner == StoredObjectKind::Table && equalsCaseInsensitive(function.name, "CAST"))
    {
        if (slot.status == StoredObjectTypeStringSlotStatus::ExactExpression)
        {
            const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
            if (literal && literal->value.getType() == Field::Types::String
                && hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>()))
            {
                /// A Table does not retain its AS SELECT expression AST.  The
                /// exact CAST may establish only the selected output's
                /// declaration identity, captured by the analyzer collector.
                classification.source_query_has_structured_udt_reference = true;
                return;
            }
        }
        else if (
            slot.status == StoredObjectTypeStringSlotStatus::ContextRequired
            || slot.status == StoredObjectTypeStringSlotStatus::UnclassifiedLayout)
        {
            recordUnresolvedTypeStringSlot(StoredObjectOccurrenceSite::UnclassifiedTypeString, classification);
            return;
        }
    }
    if (slot.occurrence_site == StoredObjectOccurrenceSite::UnclassifiedTypeString && equalsCaseInsensitive(function.name, "CAST"))
    {
        if (selected_expression_owner == StoredObjectKind::View)
            slot.occurrence_site = StoredObjectOccurrenceSite::ViewStoredCast;
        else if (selected_expression_owner == StoredObjectKind::MaterializedView)
            slot.occurrence_site = StoredObjectOccurrenceSite::MaterializedViewStoredCast;
    }
    scanClassifiedTypeStringSlot(slot, classification);
}

void scanQueryParameterTypeString(const IAST * node, StoredObjectCreateQueryClassification & classification) noexcept
{
    const auto * parameter = node ? node->as<ASTQueryParameter>() : nullptr;
    if (parameter && hasQualifiedTypeReferenceCandidate(parameter->type))
        recordTypeStringCandidate(StoredObjectOccurrenceSite::UnclassifiedTypeString, classification);
}

struct StructuredUDTScanState;
void scanStoredSettingTypeStringSlots(const IAST * node, StructuredUDTScanState & state) noexcept;

enum class StructuredUDTScanContext : UInt8
{
    Unsupported,
    StoredColumnType,
    DictionaryAttributeType,
    SelectedExpression,
    StructuredCastTarget,
};

struct StructuredUDTScanState
{
    explicit StructuredUDTScanState(StoredObjectCreateQueryClassification & classification_)
        : classification(classification_)
    {
    }

    bool enter(const IAST * node, size_t depth) noexcept
    {
        /// Completeness records whether classification is admissible. Keep
        /// scanning malformed branches within the fixed budget so an embedded
        /// structured UDT reference cannot disappear from a fail-closed result.
        if (!node || work_exhausted)
            return false;
        if (depth > maximum_structured_udt_scan_depth || remaining_nodes == 0)
        {
            complete = false;
            work_exhausted = true;
            return false;
        }
        --remaining_nodes;
        return true;
    }

    bool consumeOpaqueEntry() noexcept
    {
        if (work_exhausted)
            return false;
        if (remaining_nodes == 0)
        {
            complete = false;
            work_exhausted = true;
            return false;
        }
        --remaining_nodes;
        return true;
    }

    StoredObjectCreateQueryClassification & classification;
    size_t remaining_nodes = maximum_structured_udt_scan_nodes;
    bool complete = true;
    bool work_exhausted = false;
};

void scanStoredSettingTypeStringSlots(const IAST * node, StructuredUDTScanState & state) noexcept
{
    const auto * settings = node ? node->as<ASTSetQuery>() : nullptr;
    if (!settings)
        return;

    for (const auto & change : settings->changes)
    {
        if (!state.consumeOpaqueEntry())
            return;
        const auto * contract = tryGetStoredSettingTypeStringSlotContract(change.name);
        if (!contract)
            continue;
        if (change.value.getType() == Field::Types::String)
        {
            if (hasQualifiedTypeReferenceCandidate(change.value.safeGet<String>()))
                recordTypeStringCandidate(contract->occurrence_site, state.classification);
        }
        else if (change.value.getType() == Field::Types::CustomType)
        {
            /// Query parameters and other AST-backed setting values are not
            /// visible in IAST::children and require owner-side evaluation.
            recordUnresolvedTypeStringSlot(contract->occurrence_site, state.classification);
        }
    }
}

bool isDirectChild(const IAST & parent, const IAST * candidate) noexcept
{
    if (!candidate)
        return false;
    return std::any_of(
        parent.children.begin(), parent.children.end(), [candidate](const ASTPtr & child) { return child.get() == candidate; });
}

void recordStructuredUDTReference(
    StructuredUDTScanContext context, StoredObjectKind object_kind, StoredObjectCreateQueryClassification & classification) noexcept
{
    StoredObjectOccurrenceSite site = StoredObjectOccurrenceSite::Unclassified;
    switch (context)
    {
        case StructuredUDTScanContext::StoredColumnType:
            switch (object_kind)
            {
                case StoredObjectKind::Table: site = StoredObjectOccurrenceSite::TableColumnDeclaration; break;
                case StoredObjectKind::View: site = StoredObjectOccurrenceSite::ViewOutputDeclaration; break;
                case StoredObjectKind::MaterializedView: site = StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration; break;
                case StoredObjectKind::Dictionary:
                case StoredObjectKind::Unclassified: break;
            }
            break;
        case StructuredUDTScanContext::DictionaryAttributeType:
            if (object_kind == StoredObjectKind::Dictionary)
                site = StoredObjectOccurrenceSite::DictionaryAttribute;
            break;
        case StructuredUDTScanContext::StructuredCastTarget:
            switch (object_kind)
            {
                case StoredObjectKind::Table: classification.source_query_has_structured_udt_reference = true; return;
                case StoredObjectKind::View: site = StoredObjectOccurrenceSite::ViewStoredCast; break;
                case StoredObjectKind::MaterializedView: site = StoredObjectOccurrenceSite::MaterializedViewStoredCast; break;
                case StoredObjectKind::Dictionary:
                case StoredObjectKind::Unclassified: break;
            }
            break;
        case StructuredUDTScanContext::Unsupported:
        case StructuredUDTScanContext::SelectedExpression: break;
    }

    if (site == StoredObjectOccurrenceSite::Unclassified)
        classification.has_unclassified_udt_reference = true;
    else
        classification.structured_udt_occurrence_sites |= storedObjectOccurrenceSiteMask(site);
}

void scanStructuredUDTReferences(
    const IAST * node,
    StructuredUDTScanContext context,
    StoredObjectKind object_kind,
    StructuredUDTScanState & state,
    size_t depth) noexcept;

void scanStructuredCastTarget(
    const ASTCastTarget & target, StoredObjectKind object_kind, StructuredUDTScanState & state, size_t depth) noexcept
{
    const auto & type = target.getType();
    if (!type || target.children.size() != 1 || target.children.front().get() != type.get())
    {
        state.complete = false;
        scanStructuredUDTReferences(&target, StructuredUDTScanContext::Unsupported, object_kind, state, depth);
        return;
    }

    scanStructuredUDTReferences(&target, StructuredUDTScanContext::StructuredCastTarget, object_kind, state, depth);
}

void scanSelectedExpression(const IAST * node, StoredObjectKind object_kind, StructuredUDTScanState & state, size_t depth) noexcept
{
    if (!state.enter(node, depth))
        return;

    scanQueryParameterTypeString(node, state.classification);
    scanStoredSettingTypeStringSlots(node, state);

    if (const auto * table_expression = node->as<ASTTableExpression>(); table_expression && table_expression->table_function)
    {
        if (!isDirectChild(*table_expression, table_expression->table_function.get()))
            state.complete = false;
        if (const auto * root_function = table_expression->table_function->as<ASTFunction>())
        {
            switch (classifyStoredObjectTableFunctionSource(root_function->name))
            {
                case StoredObjectTableFunctionSourceProvenance::ExactLogicalAuthorityRequired:
                    state.classification.source_query_requires_exact_logical_authority = true;
                    break;
                case StoredObjectTableFunctionSourceProvenance::Unclassified:
                    state.classification.source_query_has_unclassified_table_function = true;
                    break;
                case StoredObjectTableFunctionSourceProvenance::PhysicalInference: break;
            }
        }
        else
        {
            state.classification.source_query_has_unclassified_table_function = true;
        }
        scanTableFunctionTypeStringTree(table_expression->table_function.get(), state.classification);
    }

    if (node->as<ASTUDTReference>())
    {
        recordStructuredUDTReference(StructuredUDTScanContext::SelectedExpression, object_kind, state.classification);
        for (const auto & child : node->children)
        {
            scanSelectedExpression(child.get(), object_kind, state, depth + 1);
            if (state.work_exhausted)
                break;
        }
        return;
    }

    const auto * function = node->as<ASTFunction>();
    const auto * cast_target = function ? function->tryGetStructuredCastTarget() : nullptr;
    if (function)
    {
        const std::array<const IAST *, 3> function_fields{
            function->arguments.get(),
            function->parameters.get(),
            function->window_definition.get(),
        };
        for (const auto * field : function_fields)
        {
            if (field && !isDirectChild(*function, field))
            {
                state.complete = false;
                scanStructuredUDTReferences(field, StructuredUDTScanContext::Unsupported, object_kind, state, depth + 1);
            }
        }
        if (!cast_target)
            scanStoredExpressionTypeStringSlot(*function, state.classification, object_kind);
    }

    const auto * arguments = function ? function->arguments.get() : nullptr;
    if (cast_target && arguments && isDirectChild(*function, arguments))
    {
        if (!state.enter(arguments, depth + 1))
            return;

        for (const auto & argument : arguments->children)
        {
            if (argument.get() == cast_target)
                scanStructuredCastTarget(*cast_target, object_kind, state, depth + 2);
            else
                scanSelectedExpression(argument.get(), object_kind, state, depth + 2);
            if (state.work_exhausted)
                break;
        }

        for (const auto & child : function->children)
        {
            if (child.get() != arguments)
                scanSelectedExpression(child.get(), object_kind, state, depth + 1);
            if (state.work_exhausted)
                break;
        }
        return;
    }

    if (cast_target)
        state.complete = false;
    for (const auto & child : node->children)
    {
        scanSelectedExpression(child.get(), object_kind, state, depth + 1);
        if (state.work_exhausted)
            break;
    }
}

void scanStructuredUDTReferences(
    const IAST * node,
    StructuredUDTScanContext context,
    StoredObjectKind object_kind,
    StructuredUDTScanState & state,
    size_t depth) noexcept
{
    if (context == StructuredUDTScanContext::SelectedExpression)
    {
        scanSelectedExpression(node, object_kind, state, depth);
        return;
    }

    if (!state.enter(node, depth))
        return;

    if (node->as<ASTUDTReference>())
        recordStructuredUDTReference(context, object_kind, state.classification);
    scanQueryParameterTypeString(node, state.classification);
    scanStoredSettingTypeStringSlots(node, state);
    if (const auto * function = node->as<ASTFunction>(); function && !function->tryGetStructuredCastTarget())
        scanStoredExpressionTypeStringSlot(*function, state.classification);

    for (const auto & child : node->children)
    {
        scanStructuredUDTReferences(child.get(), context, object_kind, state, depth + 1);
        if (state.work_exhausted)
            break;
    }
}

void scanStoredColumnDeclarations(const ASTColumns * columns_list, StoredObjectKind object_kind, StructuredUDTScanState & state) noexcept
{
    if (!columns_list || !state.enter(columns_list, 0))
        return;

    const auto * columns = columns_list->columns;
    if (columns)
    {
        if (!isDirectChild(*columns_list, columns))
            state.complete = false;
        if (state.enter(columns, 1))
        {
            for (const auto & child : columns->children)
            {
                const auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
                if (!declaration)
                {
                    state.complete = false;
                    scanStructuredUDTReferences(child.get(), StructuredUDTScanContext::Unsupported, object_kind, state, 2);
                    if (state.work_exhausted)
                        break;
                    continue;
                }

                if (!state.enter(declaration, 2))
                {
                    if (state.work_exhausted)
                        break;
                    continue;
                }
                const auto type = declaration->getType();
                if (type && !isDirectChild(*declaration, type.get()))
                    state.complete = false;
                scanStructuredUDTReferences(type.get(), StructuredUDTScanContext::StoredColumnType, object_kind, state, 3);
                const std::array<const IAST *, 8> declaration_fields{
                    type.get(),
                    declaration->getDefaultExpression().get(),
                    declaration->getComment().get(),
                    declaration->getCodec().get(),
                    declaration->getStatisticsDesc().get(),
                    declaration->getTTL().get(),
                    declaration->getCollation().get(),
                    declaration->getSettings().get(),
                };
                for (size_t field_index = 1; field_index < declaration_fields.size(); ++field_index)
                {
                    const auto * field = declaration_fields[field_index];
                    if (field && !isDirectChild(*declaration, field))
                        state.complete = false;
                    scanStructuredUDTReferences(field, StructuredUDTScanContext::Unsupported, object_kind, state, 3);
                }

                for (const auto & child_node : static_cast<const IAST &>(*declaration).children)
                {
                    const bool known = std::any_of(
                        declaration_fields.begin(),
                        declaration_fields.end(),
                        [&child_node](const IAST * field) { return field && field == child_node.get(); });
                    if (!known)
                    {
                        state.complete = false;
                        scanStructuredUDTReferences(child_node.get(), StructuredUDTScanContext::Unsupported, object_kind, state, 3);
                    }
                    if (state.work_exhausted)
                        break;
                }
                if (state.work_exhausted)
                    break;
            }
        }
    }

    for (const auto & child : columns_list->children)
    {
        if (child.get() != columns)
            scanStructuredUDTReferences(child.get(), StructuredUDTScanContext::Unsupported, object_kind, state, 1);
        if (state.work_exhausted)
            break;
    }
}

void scanDictionaryAttributes(const ASTExpressionList * attributes, StoredObjectKind object_kind, StructuredUDTScanState & state) noexcept
{
    if (!attributes || !state.enter(attributes, 0))
        return;

    for (const auto & child : attributes->children)
    {
        const auto * declaration = child ? child->as<ASTDictionaryAttributeDeclaration>() : nullptr;
        if (!declaration)
        {
            state.complete = false;
            scanStructuredUDTReferences(child.get(), StructuredUDTScanContext::Unsupported, object_kind, state, 1);
            if (state.work_exhausted)
                break;
            continue;
        }

        if (!state.enter(declaration, 1))
        {
            if (state.work_exhausted)
                break;
            continue;
        }
        const std::array<const IAST *, 3> declaration_fields{
            declaration->type.get(),
            declaration->default_value.get(),
            declaration->expression.get(),
        };
        if (!declaration_fields.front() || !isDirectChild(*declaration, declaration_fields.front()))
            state.complete = false;
        scanStructuredUDTReferences(declaration_fields.front(), StructuredUDTScanContext::DictionaryAttributeType, object_kind, state, 2);
        for (size_t field_index = 1; field_index < declaration_fields.size(); ++field_index)
        {
            const auto * field = declaration_fields[field_index];
            if (field && !isDirectChild(*declaration, field))
                state.complete = false;
            scanStructuredUDTReferences(field, StructuredUDTScanContext::Unsupported, object_kind, state, 2);
        }
        for (const auto & declaration_child : declaration->children)
        {
            const bool known = std::any_of(
                declaration_fields.begin(),
                declaration_fields.end(),
                [&declaration_child](const IAST * field) { return field && field == declaration_child.get(); });
            if (!known)
            {
                state.complete = false;
                scanStructuredUDTReferences(declaration_child.get(), StructuredUDTScanContext::Unsupported, object_kind, state, 2);
            }
            if (state.work_exhausted)
                break;
        }
        if (state.work_exhausted)
            break;
    }
}

StoredObjectKind classifyStoredObjectKind(const ASTCreateQuery & create) noexcept
{
    if (!create.table || create.isTemporary())
        return StoredObjectKind::Unclassified;

    const UInt8 tagged_kinds = static_cast<UInt8>(create.is_ordinary_view) + static_cast<UInt8>(create.is_materialized_view)
        + static_cast<UInt8>(create.is_dictionary);
    if (tagged_kinds > 1)
        return StoredObjectKind::Unclassified;
    if (create.is_ordinary_view)
        return StoredObjectKind::View;
    if (create.is_materialized_view)
        return StoredObjectKind::MaterializedView;
    if (create.is_dictionary)
        return StoredObjectKind::Dictionary;
    return StoredObjectKind::Table;
}

StoredObjectSourceMode classifyStoredObjectSourceMode(
    const ASTCreateQuery & create, StoredObjectKind object_kind, bool has_destination_column_declarations, bool metadata_load) noexcept
{
    if (object_kind == StoredObjectKind::Unclassified)
        return StoredObjectSourceMode::Unclassified;

    if (create.has_attach_from_path != !create.attach_from_path.empty())
        return StoredObjectSourceMode::Unclassified;
    if (!create.attach && !metadata_load
        && (create.attach_short_syntax || create.has_attach_from_path || create.attach_as_replicated.has_value()))
        return StoredObjectSourceMode::Unclassified;

    const bool metadata_attach = metadata_load || create.attach;
    if (metadata_attach)
    {
        if (create.is_clone_as || create.is_create_empty || !create.as_database.empty() || !create.as_table.empty()
            || create.as_table_function)
            return StoredObjectSourceMode::Unclassified;
        if ((object_kind == StoredObjectKind::Table || object_kind == StoredObjectKind::Dictionary) && create.select)
            return StoredObjectSourceMode::Unclassified;
        if (object_kind == StoredObjectKind::Dictionary && (create.columns_list || create.storage))
            return StoredObjectSourceMode::Unclassified;
        if (object_kind != StoredObjectKind::Dictionary && (create.dictionary_attributes_list || create.dictionary))
            return StoredObjectSourceMode::Unclassified;
        return StoredObjectSourceMode::AttachMetadata;
    }

    /// A foreign-dialect adapter has already erased its source grammar while
    /// producing this native AST. Preserve its existing physical behavior,
    /// but never reinterpret the translated shape as exact UDT provenance.
    if (create.isUDTDialectAdapterPhysicalOnly())
        return StoredObjectSourceMode::DialectLike;

    if (create.as_database.size() && create.as_table.empty())
        return StoredObjectSourceMode::Unclassified;
    const bool has_source_table = !create.as_table.empty();
    const UInt8 source_count = static_cast<UInt8>(has_source_table) + static_cast<UInt8>(create.select != nullptr)
        + static_cast<UInt8>(create.as_table_function != nullptr);
    if (source_count > 1 || (create.is_clone_as && !has_source_table) || (create.is_create_empty && !create.select)
        || (create.is_clone_as && create.is_create_empty))
        return StoredObjectSourceMode::Unclassified;

    switch (object_kind)
    {
        case StoredObjectKind::Table:
            if (create.dictionary_attributes_list || create.dictionary || create.aliases_list || create.is_populate)
                return StoredObjectSourceMode::Unclassified;
            if (has_source_table)
                return create.is_clone_as ? StoredObjectSourceMode::CloneAsSourceTable : StoredObjectSourceMode::AsSourceTable;
            if (create.select)
                return create.is_create_empty ? StoredObjectSourceMode::EmptyAsSelect : StoredObjectSourceMode::AsSelect;
            if (create.as_table_function)
                return StoredObjectSourceMode::AsTableFunction;
            if (create.columns_list && (!create.columns_list->columns || create.columns_list->columns->children.empty()))
                return StoredObjectSourceMode::Unclassified;
            return has_destination_column_declarations ? StoredObjectSourceMode::ExplicitColumns : StoredObjectSourceMode::SchemaInference;
        case StoredObjectKind::View:
        case StoredObjectKind::MaterializedView:
            if (create.dictionary_attributes_list || create.dictionary || create.as_table_function || has_source_table
                || create.is_clone_as)
                return StoredObjectSourceMode::Unclassified;
            if (!create.select)
                return StoredObjectSourceMode::Unclassified;
            if (object_kind == StoredObjectKind::View && create.is_populate)
                return StoredObjectSourceMode::Unclassified;
            return create.is_create_empty ? StoredObjectSourceMode::EmptyAsSelect : StoredObjectSourceMode::AsSelect;
        case StoredObjectKind::Dictionary:
            if (create.columns_list || create.aliases_list || create.storage || create.select || create.as_table_function
                || has_source_table || create.is_clone_as || create.is_create_empty || create.is_populate
                || !create.dictionary_attributes_list || !create.dictionary)
                return StoredObjectSourceMode::Unclassified;
            return StoredObjectSourceMode::ObjectDefinition;
        case StoredObjectKind::Unclassified: return StoredObjectSourceMode::Unclassified;
    }
    return StoredObjectSourceMode::Unclassified;
}

constexpr std::array occurrence_site_contracts{
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::TableColumnDeclaration,
        .owner_kinds = table_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ColumnType,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::ViewOutputDeclaration,
        .owner_kinds = view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::ViewStoredCast,
        .owner_kinds = view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
        .owner_kinds = materialized_view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::MaterializedViewStoredCast,
        .owner_kinds = materialized_view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::DictionaryAttribute,
        .owner_kinds = dictionary_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::DictionaryAttribute,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::TableFunctionSchemaString,
        .owner_kinds = view_kind | materialized_view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::FormatSchemaString,
        .owner_kinds = view_kind | materialized_view_kind,
        .disposition = StoredObjectOccurrenceDisposition::ExactPersistedPath,
        .persisted_path_section = PersistedTypePathSection::ViewExpression,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::StorageEngineTypeString,
        .owner_kinds = table_kind,
        .disposition = StoredObjectOccurrenceDisposition::PhysicalOnly,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::GlobalSQLUDFBody,
        .owner_kinds = all_stored_object_kinds,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::ExecutableFunctionBody,
        .owner_kinds = all_stored_object_kinds,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::ExternalMutableAuthority,
        .owner_kinds = all_stored_object_kinds,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::TemporaryObject,
        .owner_kinds = all_stored_object_kinds,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::DefaultExpression,
        .owner_kinds = table_kind,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::ConstraintExpression,
        .owner_kinds = table_kind,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::PolicyExpression,
        .owner_kinds = table_kind,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
    StoredObjectOccurrenceSiteContract{
        .site = StoredObjectOccurrenceSite::UnclassifiedTypeString,
        .owner_kinds = all_stored_object_kinds,
        .disposition = StoredObjectOccurrenceDisposition::Unsupported,
        .persisted_path_section = std::nullopt,
    },
};

constexpr std::array source_mode_contracts{
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::ExplicitColumns,
        .implicit_provenance = StoredObjectProvenanceRule::ExplicitTargetBinding,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::AsSourceTable,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSourceSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::CloneAsSourceTable,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSourceSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::AsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::EmptyAsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::AsTableFunction,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalInference,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::SchemaInference,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalInference,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::DialectLike,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalOnly,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = 0,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Table,
        .source_mode = StoredObjectSourceMode::AttachMetadata,
        .implicit_provenance = StoredObjectProvenanceRule::ExactAttachedSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = table_column_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::View,
        .source_mode = StoredObjectSourceMode::AsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::View,
        .source_mode = StoredObjectSourceMode::EmptyAsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::View,
        .source_mode = StoredObjectSourceMode::DialectLike,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalOnly,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = 0,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::View,
        .source_mode = StoredObjectSourceMode::AttachMetadata,
        .implicit_provenance = StoredObjectProvenanceRule::ExactAttachedSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::MaterializedView,
        .source_mode = StoredObjectSourceMode::AsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = materialized_view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::MaterializedView,
        .source_mode = StoredObjectSourceMode::EmptyAsSelect,
        .implicit_provenance = StoredObjectProvenanceRule::ExactSelectedOutput,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = materialized_view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::MaterializedView,
        .source_mode = StoredObjectSourceMode::DialectLike,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalOnly,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = 0,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::MaterializedView,
        .source_mode = StoredObjectSourceMode::AttachMetadata,
        .implicit_provenance = StoredObjectProvenanceRule::ExactAttachedSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = materialized_view_output_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Dictionary,
        .source_mode = StoredObjectSourceMode::ObjectDefinition,
        .implicit_provenance = StoredObjectProvenanceRule::ExplicitTargetBinding,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = true,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = dictionary_attribute_site,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Dictionary,
        .source_mode = StoredObjectSourceMode::DialectLike,
        .implicit_provenance = StoredObjectProvenanceRule::PhysicalOnly,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = 0,
    },
    StoredObjectSourceModeContract{
        .object_kind = StoredObjectKind::Dictionary,
        .source_mode = StoredObjectSourceMode::AttachMetadata,
        .implicit_provenance = StoredObjectProvenanceRule::ExactAttachedSidecar,
        .physical_only_allowed = true,
        .explicit_destination_columns_allowed = false,
        .cross_database_physical_allowed = true,
        .required_logical_occurrence_sites = dictionary_attribute_site,
    },
};

constexpr bool occurrenceSiteContractsAreClosed() noexcept
{
    for (size_t index = 0; index < occurrence_site_contracts.size(); ++index)
    {
        const auto & contract = occurrence_site_contracts[index];
        if (!storedObjectOccurrenceSiteMask(contract.site) || !contract.owner_kinds)
            return false;
        if ((contract.disposition == StoredObjectOccurrenceDisposition::ExactPersistedPath) != contract.persisted_path_section.has_value())
            return false;
        for (size_t previous = 0; previous < index; ++previous)
            if (occurrence_site_contracts[previous].site == contract.site)
                return false;
    }
    return true;
}

constexpr StoredObjectOccurrenceSiteMask exactOccurrenceSitesFor(StoredObjectKind object_kind) noexcept
{
    StoredObjectOccurrenceSiteMask result = 0;
    for (const auto & contract : occurrence_site_contracts)
    {
        if (contract.disposition == StoredObjectOccurrenceDisposition::ExactPersistedPath
            && (contract.owner_kinds & storedObjectKindMask(object_kind)))
            result |= storedObjectOccurrenceSiteMask(contract.site);
    }
    return result;
}

constexpr bool sourceModeContractsAreClosed() noexcept
{
    for (size_t index = 0; index < source_mode_contracts.size(); ++index)
    {
        const auto & contract = source_mode_contracts[index];
        if (!storedObjectKindMask(contract.object_kind) || !storedObjectSourceModeMask(contract.source_mode)
            || !contract.physical_only_allowed
            || (contract.required_logical_occurrence_sites & ~exactOccurrenceSitesFor(contract.object_kind)) != 0)
            return false;
        if (contract.implicit_provenance == StoredObjectProvenanceRule::PhysicalOnly && contract.required_logical_occurrence_sites != 0)
            return false;
        for (size_t previous = 0; previous < index; ++previous)
        {
            if (source_mode_contracts[previous].object_kind == contract.object_kind
                && source_mode_contracts[previous].source_mode == contract.source_mode)
                return false;
        }
    }
    return true;
}

static_assert(occurrenceSiteContractsAreClosed());
static_assert(sourceModeContractsAreClosed());

[[noreturn]] void failRegistry(RegistryError::Code code, std::string_view message)
{
    throw RegistryError(code, message);
}

StoredObjectOccurrenceSite selectedOutputSite(StoredObjectKind object_kind) noexcept
{
    switch (object_kind)
    {
        case StoredObjectKind::Table: return StoredObjectOccurrenceSite::TableColumnDeclaration;
        case StoredObjectKind::View: return StoredObjectOccurrenceSite::ViewOutputDeclaration;
        case StoredObjectKind::MaterializedView: return StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration;
        case StoredObjectKind::Dictionary:
        case StoredObjectKind::Unclassified: return StoredObjectOccurrenceSite::Unclassified;
    }
    return StoredObjectOccurrenceSite::Unclassified;
}

struct DescriptorDefinitionProvenance
{
    UInt64 revision = 0;
    Digest definition_hash{};
    UInt16 checker_abi = 0;
    UInt16 checker_charge_abi = 0;
    UInt16 policy_abi = 0;
    UInt16 function_registry_abi = 0;
    Digest policy_semantic_hash{};
    SemanticCapabilityMask semantic_capabilities = 0;

    bool operator==(const DescriptorDefinitionProvenance &) const = default;
};

struct DescriptorDefinitionProvenanceEntry
{
    UUID type_uuid;
    DescriptorDefinitionProvenance provenance;
};

bool descriptorDefinitionProvenanceEntryLess(
    const DescriptorDefinitionProvenanceEntry & lhs, const DescriptorDefinitionProvenanceEntry & rhs) noexcept
{
    const auto lhs_high = UUIDHelpers::getHighBytes(lhs.type_uuid);
    const auto rhs_high = UUIDHelpers::getHighBytes(rhs.type_uuid);
    if (lhs_high != rhs_high)
        return lhs_high < rhs_high;
    return UUIDHelpers::getLowBytes(lhs.type_uuid) < UUIDHelpers::getLowBytes(rhs.type_uuid);
}

bool reserveDescriptorDefinitionProvenance(std::vector<DescriptorDefinitionProvenanceEntry> & definitions, size_t maximum_entries)
{
    if (maximum_entries > maximum_stored_object_admission_scratch_bytes / sizeof(DescriptorDefinitionProvenanceEntry))
        return false;
    definitions.reserve(maximum_entries);
    return true;
}

StoredObjectAdmissionRejection collectDescriptorDefinitionProvenance(
    const PersistedTypeDescriptor & descriptor,
    const UUID & target_database_uuid,
    std::vector<DescriptorDefinitionProvenanceEntry> & definitions)
{
    const auto & identity = descriptor.getDefinitionIdentity();
    if (identity.database_uuid != target_database_uuid)
        return StoredObjectAdmissionRejection::CrossDatabaseDescriptor;

    const DescriptorDefinitionProvenance provenance{
        .revision = identity.revision,
        .definition_hash = descriptor.getDefinitionHash(),
        .checker_abi = descriptor.getCheckerABI(),
        .checker_charge_abi = descriptor.getCheckerChargeABI(),
        .policy_abi = descriptor.getPolicyABI(),
        .function_registry_abi = descriptor.getFunctionRegistryABI(),
        .policy_semantic_hash = descriptor.getPolicySemanticHash(),
        .semantic_capabilities = descriptor.getSemanticCapabilities(),
    };
    definitions.push_back({.type_uuid = identity.type_uuid, .provenance = provenance});
    return StoredObjectAdmissionRejection::None;
}

StoredObjectAdmissionRejection validateDescriptorDefinitionProvenance(std::vector<DescriptorDefinitionProvenanceEntry> & definitions)
{
    std::sort(definitions.begin(), definitions.end(), descriptorDefinitionProvenanceEntryLess);
    for (size_t index = 1; index < definitions.size(); ++index)
    {
        if (definitions[index - 1].type_uuid == definitions[index].type_uuid
            && definitions[index - 1].provenance != definitions[index].provenance)
            return StoredObjectAdmissionRejection::ConflictingDescriptorIdentity;
    }
    return StoredObjectAdmissionRejection::None;
}

StoredObjectOccurrenceSite firstRequiredOccurrenceSite(StoredObjectOccurrenceSiteMask sites) noexcept
{
    for (const auto & contract : occurrence_site_contracts)
    {
        if (sites & storedObjectOccurrenceSiteMask(contract.site))
            return contract.site;
    }
    return StoredObjectOccurrenceSite::Unclassified;
}

bool sourceModeCanCarryLogicalReferences(const StoredObjectSourceModeContract & contract) noexcept
{
    return contract.required_logical_occurrence_sites != 0;
}

}

StoredObjectCreateQueryClassification classifyStoredObjectCreateQuery(const ASTCreateQuery & create, bool metadata_load) noexcept
{
    StoredObjectCreateQueryClassification result;
    result.object_kind = classifyStoredObjectKind(create);

    const bool has_destination_column_declarations
        = create.columns_list && create.columns_list->columns && !create.columns_list->columns->children.empty();
    result.source_mode = classifyStoredObjectSourceMode(create, result.object_kind, has_destination_column_declarations, metadata_load);
    result.has_explicit_destination_columns
        = has_destination_column_declarations && result.source_mode != StoredObjectSourceMode::AttachMetadata;
    if (const auto * function = create.as_table_function ? create.as_table_function->as<ASTFunction>() : nullptr)
        result.source_table_function_provenance = classifyStoredObjectTableFunctionSource(function->name);

    StructuredUDTScanState scan_state(result);
    scanStoredColumnDeclarations(create.columns_list, result.object_kind, scan_state);
    scanDictionaryAttributes(create.dictionary_attributes_list, result.object_kind, scan_state);
    scanStructuredUDTReferences(create.select, StructuredUDTScanContext::SelectedExpression, result.object_kind, scan_state, 0);
    scanTableFunctionTypeStringTree(create.as_table_function, result);
    scanStorageEngineTypeStringSlot(create.storage, result);

    const std::array<const IAST *, 16> unsupported_roots{
        create.database.get(),
        create.table.get(),
        create.aliases_list,
        create.storage,
        create.as_table_function,
        create.targets,
        create.comment,
        create.sql_security,
        create.table_overrides,
        create.dictionary,
        create.refresh_strategy,
        create.out_file.get(),
        create.format_ast.get(),
        create.settings_ast.get(),
        create.compression.get(),
        create.compression_level.get(),
    };
    for (const auto * root : unsupported_roots)
    {
        scanStructuredUDTReferences(root, StructuredUDTScanContext::Unsupported, result.object_kind, scan_state, 0);
        if (scan_state.work_exhausted)
            break;
    }

    const std::array<const IAST *, 19> known_roots{
        create.database.get(),
        create.table.get(),
        create.columns_list,
        create.aliases_list,
        create.storage,
        create.as_table_function,
        create.select,
        create.targets,
        create.comment,
        create.sql_security,
        create.table_overrides,
        create.dictionary_attributes_list,
        create.dictionary,
        create.refresh_strategy,
        create.out_file.get(),
        create.format_ast.get(),
        create.settings_ast.get(),
        create.compression.get(),
        create.compression_level.get(),
    };
    for (const auto * root : known_roots)
    {
        if (root && !isDirectChild(create, root))
            scan_state.complete = false;
    }
    for (const auto & child : create.children)
    {
        if (scan_state.work_exhausted)
            break;
        const bool known
            = std::any_of(known_roots.begin(), known_roots.end(), [&child](const IAST * root) { return root && root == child.get(); });
        if (!known)
        {
            scan_state.complete = false;
            scanStructuredUDTReferences(child.get(), StructuredUDTScanContext::Unsupported, result.object_kind, scan_state, 0);
        }
    }

    result.structured_udt_scan_complete = scan_state.complete;
    if (!result.structured_udt_scan_complete || result.has_unclassified_udt_reference
        || !tryGetStoredObjectSourceModeContract(result.object_kind, result.source_mode))
        result.source_mode = StoredObjectSourceMode::Unclassified;
    return result;
}

StoredObjectCreatePreparationDecision classifyStoredObjectCreatePreparation(
    const ASTCreateQuery & create, const StoredObjectCreateQueryClassification & classification, bool udt_feature_enabled) noexcept
{
    /// A Table's source query/table-function AST is an execution input, not
    /// part of the durable CREATE metadata.  Once destination columns are
    /// explicit, only those declarations can establish persisted identity;
    /// CASTs and schema strings in the transient source still resolve as
    /// ordinary query syntax.  Do not reject or persist them merely because
    /// the source happens to contain UDT syntax.  Storage-engine type strings
    /// and every unclassified site remain durable fail-closed evidence.
    const bool table_has_transient_explicit_source = classification.object_kind == StoredObjectKind::Table
        && classification.has_explicit_destination_columns
        && (classification.source_mode == StoredObjectSourceMode::AsSelect
            || classification.source_mode == StoredObjectSourceMode::EmptyAsSelect
            || classification.source_mode == StoredObjectSourceMode::AsTableFunction);
    constexpr auto transient_table_source_string_sites
        = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    const auto persistent_qualified_type_reference_candidate_sites = classification.qualified_type_reference_candidate_sites
        & ~(table_has_transient_explicit_source ? transient_table_source_string_sites : StoredObjectOccurrenceSiteMask{0});
    const bool persistent_source_query_has_structured_udt_reference
        = classification.source_query_has_structured_udt_reference && !table_has_transient_explicit_source;
    const bool has_positive_udt_evidence = classification.structured_udt_occurrence_sites != 0
        || persistent_qualified_type_reference_candidate_sites != 0 || persistent_source_query_has_structured_udt_reference
        || classification.has_unclassified_udt_reference;

    const auto unsupported = [&](StoredObjectAdmissionRejection rejection, StoredObjectOccurrenceSite site)
    {
        return StoredObjectCreatePreparationDecision{
            .route = StoredObjectCreatePreparationRoute::Unsupported,
            .rejection = rejection,
            .occurrence_site = site,
            .has_positive_udt_evidence = has_positive_udt_evidence,
        };
    };

    if (!has_positive_udt_evidence)
    {
        if (udt_feature_enabled && !classification.has_explicit_destination_columns
            && (classification.source_mode == StoredObjectSourceMode::AsSelect
                || classification.source_mode == StoredObjectSourceMode::EmptyAsSelect)
            && classification.source_query_has_unclassified_table_function)
        {
            return unsupported(StoredObjectAdmissionRejection::InvalidProvenanceSource, StoredObjectOccurrenceSite::Unclassified);
        }
        if (udt_feature_enabled && classification.object_kind == StoredObjectKind::Table
            && classification.source_mode == StoredObjectSourceMode::AsTableFunction && !classification.has_explicit_destination_columns
            && classification.source_table_function_provenance != StoredObjectTableFunctionSourceProvenance::PhysicalInference)
        {
            return unsupported(StoredObjectAdmissionRejection::InvalidProvenanceSource, StoredObjectOccurrenceSite::Unclassified);
        }
        if (!udt_feature_enabled || (classification.structured_udt_scan_complete && classification.type_string_scan_complete))
            return {.route = StoredObjectCreatePreparationRoute::PhysicalOnly};

        return unsupported(
            classification.structured_udt_scan_complete ? StoredObjectAdmissionRejection::IncompleteTypeStringClassification
                                                        : StoredObjectAdmissionRejection::IncompleteOutputClassification,
            StoredObjectOccurrenceSite::UnclassifiedTypeString);
    }

    if (!classification.structured_udt_scan_complete)
        return unsupported(StoredObjectAdmissionRejection::IncompleteOutputClassification, StoredObjectOccurrenceSite::Unclassified);
    if (!classification.type_string_scan_complete)
        return unsupported(
            StoredObjectAdmissionRejection::IncompleteTypeStringClassification, StoredObjectOccurrenceSite::UnclassifiedTypeString);
    if (classification.object_kind == StoredObjectKind::Unclassified)
        return unsupported(StoredObjectAdmissionRejection::UnclassifiedObjectKind, StoredObjectOccurrenceSite::Unclassified);
    if (classification.source_mode == StoredObjectSourceMode::Unclassified)
        return unsupported(StoredObjectAdmissionRejection::UnclassifiedSourceMode, StoredObjectOccurrenceSite::Unclassified);

    /// Distributed placement is not part of the structural CREATE surface:
    /// retain the exact logical route so InterpreterCreateQuery can reject it
    /// with the dedicated pre-dispatch CREATE ON CLUSTER diagnostic.
    const bool fresh_create_surface = !create.attach && !create.if_not_exists && !create.replace_view && !create.replace_table
        && !create.create_or_replace && !create.has_attach_from_path && !create.attach_short_syntax
        && !create.attach_as_replicated.has_value();
    if (!fresh_create_surface)
        return unsupported(StoredObjectAdmissionRejection::InvalidProvenanceSource, StoredObjectOccurrenceSite::Unclassified);

    /// `CREATE TABLE ... AS table_function()` retains the table-function AST
    /// so the storage can be reconstructed, but its inferred destination
    /// columns are deliberately physical-only.  An exact, literal schema
    /// slot may therefore use qualified UDT syntax only through the dedicated
    /// pre-persistence physicalization route; it never establishes a target
    /// descriptor or sidecar.
    constexpr auto table_function_schema_sites = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
        | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
    if (classification.object_kind == StoredObjectKind::Table && classification.source_mode == StoredObjectSourceMode::AsTableFunction
        && !classification.has_explicit_destination_columns
        && classification.source_table_function_provenance == StoredObjectTableFunctionSourceProvenance::PhysicalInference
        && classification.structured_udt_occurrence_sites == 0 && classification.qualified_type_reference_candidate_sites != 0
        && (classification.qualified_type_reference_candidate_sites & ~table_function_schema_sites) == 0
        && !classification.source_query_has_structured_udt_reference)
    {
        const auto * function = create.as_table_function ? create.as_table_function->as<ASTFunction>() : nullptr;
        const auto tree = function ? classifyStoredObjectTableFunctionTypeStringTree(*function)
                                   : StoredObjectTableFunctionTypeStringTreeClassification{};
        const auto & slot = tree.schema_slot;
        const auto * literal = slot.expression ? slot.expression->as<ASTLiteral>() : nullptr;
        if (!function || tree.status != StoredObjectTableFunctionTypeStringTreeStatus::Complete || !tree.schema_owner
            || slot.status != StoredObjectTypeStringSlotStatus::ExactExpression
            || (slot.occurrence_site != StoredObjectOccurrenceSite::TableFunctionSchemaString
                && slot.occurrence_site != StoredObjectOccurrenceSite::FormatSchemaString)
            || !literal || literal->value.getType() != Field::Types::String
            || !hasQualifiedTypeReferenceCandidate(literal->value.safeGet<String>()))
        {
            return unsupported(
                StoredObjectAdmissionRejection::IncompleteTypeStringClassification, StoredObjectOccurrenceSite::UnclassifiedTypeString);
        }
        return {
            .route = StoredObjectCreatePreparationRoute::PhysicalizeTableFunctionSchema,
            .rejection = StoredObjectAdmissionRejection::None,
            .occurrence_site = slot.occurrence_site,
            .has_positive_udt_evidence = true,
        };
    }

    const auto exact_declaration_route = [&](StoredObjectKind object_kind,
                                             StoredObjectOccurrenceSite site,
                                             StoredObjectCreatePreparationRoute route,
                                             bool require_explicit_columns,
                                             bool allow_materialized_view_targets_and_populate)
    {
        const auto site_mask = storedObjectOccurrenceSiteMask(site);
        StoredObjectOccurrenceSiteMask auxiliary_site_mask = 0;
        if (object_kind == StoredObjectKind::View)
            auxiliary_site_mask |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::ViewStoredCast);
        else if (object_kind == StoredObjectKind::MaterializedView)
            auxiliary_site_mask |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::MaterializedViewStoredCast);
        if (object_kind == StoredObjectKind::View || object_kind == StoredObjectKind::MaterializedView)
        {
            auxiliary_site_mask |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
                | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
        }
        const auto allowed_site_mask = site_mask | auxiliary_site_mask;
        if (classification.object_kind != object_kind)
            return unsupported(StoredObjectAdmissionRejection::UnsupportedObjectSourceMode, site);
        if (require_explicit_columns && !classification.has_explicit_destination_columns)
            return unsupported(StoredObjectAdmissionRejection::InvalidProvenanceSource, site);
        if ((!allow_materialized_view_targets_and_populate && (create.targets || create.is_populate)) || create.refresh_strategy
            || classification.has_unclassified_udt_reference || classification.source_query_has_structured_udt_reference
            || (classification.qualified_type_reference_candidate_sites & ~auxiliary_site_mask) != 0
            || (classification.structured_udt_occurrence_sites | classification.qualified_type_reference_candidate_sites) == 0
            || (classification.structured_udt_occurrence_sites & ~allowed_site_mask) != 0)
            return unsupported(StoredObjectAdmissionRejection::UnsupportedOccurrenceSite, site);
        return StoredObjectCreatePreparationDecision{
            .route = route,
            .rejection = StoredObjectAdmissionRejection::None,
            .occurrence_site = site,
            .has_positive_udt_evidence = true,
        };
    };

    const auto selected_output_route = [&](StoredObjectKind object_kind,
                                           StoredObjectOccurrenceSite output_site,
                                           StoredObjectCreatePreparationRoute route,
                                           bool allow_materialized_view_targets_and_populate)
    {
        StoredObjectOccurrenceSiteMask allowed_auxiliary_sites = 0;
        if (object_kind == StoredObjectKind::View)
            allowed_auxiliary_sites |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::ViewStoredCast);
        else if (object_kind == StoredObjectKind::MaterializedView)
            allowed_auxiliary_sites |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::MaterializedViewStoredCast);
        if (object_kind == StoredObjectKind::View || object_kind == StoredObjectKind::MaterializedView)
        {
            allowed_auxiliary_sites |= storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableFunctionSchemaString)
                | storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::FormatSchemaString);
        }
        if (classification.object_kind != object_kind || classification.has_explicit_destination_columns
            || (classification.source_mode != StoredObjectSourceMode::AsSelect
                && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
            || classification.has_unclassified_udt_reference || classification.source_query_has_unclassified_table_function
            || (classification.structured_udt_occurrence_sites & ~allowed_auxiliary_sites) != 0
            || (classification.qualified_type_reference_candidate_sites & ~allowed_auxiliary_sites) != 0
            || (object_kind != StoredObjectKind::Table && classification.source_query_has_structured_udt_reference)
            || (!allow_materialized_view_targets_and_populate && (create.targets || create.is_populate)) || create.refresh_strategy)
        {
            return unsupported(StoredObjectAdmissionRejection::InvalidProvenanceSource, output_site);
        }
        return StoredObjectCreatePreparationDecision{
            .route = route,
            .rejection = StoredObjectAdmissionRejection::None,
            .occurrence_site = output_site,
            .has_positive_udt_evidence = true,
        };
    };

    if (classification.object_kind == StoredObjectKind::Table)
    {
        const auto table_column_site_mask = storedObjectOccurrenceSiteMask(StoredObjectOccurrenceSite::TableColumnDeclaration);
        const auto * source_contract = tryGetStoredObjectSourceModeContract(classification.object_kind, classification.source_mode);
        if (source_contract && source_contract->explicit_destination_columns_allowed && classification.has_explicit_destination_columns
            && classification.structured_udt_occurrence_sites == table_column_site_mask
            && persistent_qualified_type_reference_candidate_sites == 0 && !classification.has_unclassified_udt_reference
            && !create.targets)
        {
            return {
                .route = StoredObjectCreatePreparationRoute::TableExplicitColumns,
                .rejection = StoredObjectAdmissionRejection::None,
                .occurrence_site = StoredObjectOccurrenceSite::TableColumnDeclaration,
                .has_positive_udt_evidence = true,
            };
        }
        if (!classification.has_explicit_destination_columns
            && (classification.source_mode == StoredObjectSourceMode::AsSelect
                || classification.source_mode == StoredObjectSourceMode::EmptyAsSelect))
        {
            return selected_output_route(
                StoredObjectKind::Table,
                StoredObjectOccurrenceSite::TableColumnDeclaration,
                StoredObjectCreatePreparationRoute::PrepareTableSelectedOutputs,
                false);
        }
        return unsupported(StoredObjectAdmissionRejection::UnsupportedOccurrenceSite, StoredObjectOccurrenceSite::TableColumnDeclaration);
    }

    if (classification.object_kind == StoredObjectKind::View)
    {
        if (classification.source_mode != StoredObjectSourceMode::AsSelect
            && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
            return unsupported(
                StoredObjectAdmissionRejection::UnsupportedObjectSourceMode, StoredObjectOccurrenceSite::ViewOutputDeclaration);
        return classification.has_explicit_destination_columns ? exact_declaration_route(
                                                                     StoredObjectKind::View,
                                                                     StoredObjectOccurrenceSite::ViewOutputDeclaration,
                                                                     StoredObjectCreatePreparationRoute::PrepareViewExplicitOutputs,
                                                                     true,
                                                                     false)
                                                               : selected_output_route(
                                                                     StoredObjectKind::View,
                                                                     StoredObjectOccurrenceSite::ViewOutputDeclaration,
                                                                     StoredObjectCreatePreparationRoute::PrepareViewSelectedOutputs,
                                                                     false);
    }
    if (classification.object_kind == StoredObjectKind::MaterializedView)
    {
        if (classification.source_mode != StoredObjectSourceMode::AsSelect
            && classification.source_mode != StoredObjectSourceMode::EmptyAsSelect)
            return unsupported(
                StoredObjectAdmissionRejection::UnsupportedObjectSourceMode, StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration);
        return classification.has_explicit_destination_columns
            ? exact_declaration_route(
                  StoredObjectKind::MaterializedView,
                  StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
                  StoredObjectCreatePreparationRoute::PrepareMaterializedViewExplicitOutputs,
                  true,
                  true)
            : selected_output_route(
                  StoredObjectKind::MaterializedView,
                  StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
                  StoredObjectCreatePreparationRoute::PrepareMaterializedViewSelectedOutputs,
                  true);
    }
    if (classification.object_kind == StoredObjectKind::Dictionary)
    {
        if (classification.source_mode != StoredObjectSourceMode::ObjectDefinition)
            return unsupported(
                StoredObjectAdmissionRejection::UnsupportedObjectSourceMode, StoredObjectOccurrenceSite::DictionaryAttribute);
        return exact_declaration_route(
            StoredObjectKind::Dictionary,
            StoredObjectOccurrenceSite::DictionaryAttribute,
            StoredObjectCreatePreparationRoute::PrepareDictionaryAttributes,
            false,
            false);
    }
    return unsupported(StoredObjectAdmissionRejection::UnclassifiedObjectKind, StoredObjectOccurrenceSite::Unclassified);
}

std::span<const StoredObjectOccurrenceSiteContract> getStoredObjectOccurrenceSiteContracts() noexcept
{
    return occurrence_site_contracts;
}

std::span<const StoredObjectSourceModeContract> getStoredObjectSourceModeContracts() noexcept
{
    return source_mode_contracts;
}

const StoredObjectOccurrenceSiteContract * tryGetStoredObjectOccurrenceSiteContract(StoredObjectOccurrenceSite site) noexcept
{
    const auto it = std::find_if(
        occurrence_site_contracts.begin(),
        occurrence_site_contracts.end(),
        [site](const auto & contract) { return contract.site == site; });
    return it == occurrence_site_contracts.end() ? nullptr : &*it;
}

const StoredObjectSourceModeContract *
tryGetStoredObjectSourceModeContract(StoredObjectKind object_kind, StoredObjectSourceMode source_mode) noexcept
{
    const auto it = std::find_if(
        source_mode_contracts.begin(),
        source_mode_contracts.end(),
        [object_kind, source_mode](const auto & contract)
        { return contract.object_kind == object_kind && contract.source_mode == source_mode; });
    return it == source_mode_contracts.end() ? nullptr : &*it;
}

std::optional<SchemaObjectKind> tryGetSchemaObjectKind(StoredObjectKind object_kind) noexcept
{
    switch (object_kind)
    {
        case StoredObjectKind::Table: return SchemaObjectKind::Table;
        case StoredObjectKind::View:
        case StoredObjectKind::MaterializedView: return SchemaObjectKind::View;
        case StoredObjectKind::Dictionary: return SchemaObjectKind::Dictionary;
        case StoredObjectKind::Unclassified: return std::nullopt;
    }
    return std::nullopt;
}

StoredObjectSelectedOutput StoredObjectSelectedOutput::physical()
{
    return StoredObjectSelectedOutput(Kind::Physical, std::nullopt);
}

StoredObjectSelectedOutput StoredObjectSelectedOutput::exactDescriptor(PersistedTypeDescriptor descriptor)
{
    return StoredObjectSelectedOutput(Kind::ExactDescriptor, std::move(descriptor));
}

const PersistedTypeDescriptor * StoredObjectSelectedOutput::tryGetExactDescriptor() const noexcept
{
    return descriptor ? &*descriptor : nullptr;
}

StoredObjectSelectedOutput::StoredObjectSelectedOutput(Kind kind_, std::optional<PersistedTypeDescriptor> descriptor_)
    : kind(kind_)
    , descriptor(std::move(descriptor_))
{
}

StoredObjectPhysicalizationDispatch::StoredObjectPhysicalizationDispatch(
    SchemaObjectKind schema_object_kind_,
    const IPhysicalizationObjectProvider & object_provider_,
    const IPhysicalizationRewriteAdapter & rewrite_adapter_) noexcept
    : schema_object_kind(schema_object_kind_)
    , object_provider(&object_provider_)
    , rewrite_adapter(&rewrite_adapter_)
{
}

StoredObjectPhysicalizationAdapterRegistryError::StoredObjectPhysicalizationAdapterRegistryError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

StoredObjectPhysicalizationAdapterRegistry
StoredObjectPhysicalizationAdapterRegistry::create(std::span<const StoredObjectPhysicalizationAdapterRegistration> registrations)
{
    if (registrations.size() > maximum_physicalization_adapter_registrations)
        failRegistry(RegistryError::Code::LimitExceeded, "stored-object physicalization registry exceeds the closed object-kind count");

    std::vector<StoredObjectPhysicalizationAdapterRegistration> validated;
    validated.reserve(registrations.size());

    for (const auto & registration : registrations)
    {
        const auto expected_schema_kind = tryGetSchemaObjectKind(registration.object_kind);
        if (!expected_schema_kind || *expected_schema_kind != registration.schema_object_kind || !registration.object_provider
            || !registration.rewrite_adapter || !registration.source_modes || !registration.occurrence_sites)
            failRegistry(RegistryError::Code::InvalidRegistration, "stored-object physicalization registration is incomplete");

        StoredObjectSourceModeMask allowed_source_modes = 0;
        StoredObjectOccurrenceSiteMask allowed_occurrence_sites = 0;
        for (const auto & contract : source_mode_contracts)
        {
            if (contract.object_kind != registration.object_kind || !sourceModeCanCarryLogicalReferences(contract))
                continue;
            allowed_source_modes |= storedObjectSourceModeMask(contract.source_mode);
        }
        for (const auto & contract : occurrence_site_contracts)
        {
            if (contract.disposition == StoredObjectOccurrenceDisposition::ExactPersistedPath
                && (contract.owner_kinds & storedObjectKindMask(registration.object_kind)))
                allowed_occurrence_sites |= storedObjectOccurrenceSiteMask(contract.site);
        }

        if ((registration.source_modes & ~allowed_source_modes) != 0 || (registration.occurrence_sites & ~allowed_occurrence_sites) != 0)
            failRegistry(
                RegistryError::Code::InvalidRegistration, "stored-object physicalization registration is outside the closed inventory");

        for (const auto & contract : source_mode_contracts)
        {
            if (contract.object_kind != registration.object_kind
                || !(registration.source_modes & storedObjectSourceModeMask(contract.source_mode)))
                continue;
            if ((registration.occurrence_sites & contract.required_logical_occurrence_sites) != contract.required_logical_occurrence_sites)
                failRegistry(
                    RegistryError::Code::IncompleteRegistration,
                    "stored-object physicalization registration omits a required occurrence site");
        }

        const auto duplicate = std::find_if(
            validated.begin(), validated.end(), [&](const auto & current) { return current.object_kind == registration.object_kind; });
        if (duplicate != validated.end())
            failRegistry(RegistryError::Code::DuplicateRegistration, "stored-object physicalization object kind is registered twice");

        const auto same_schema_kind = std::find_if(
            validated.begin(),
            validated.end(),
            [&](const auto & current) { return current.schema_object_kind == registration.schema_object_kind; });
        if (same_schema_kind != validated.end()
            && (same_schema_kind->object_provider != registration.object_provider
                || same_schema_kind->rewrite_adapter != registration.rewrite_adapter))
            failRegistry(
                RegistryError::Code::ConflictingDispatch,
                "stored-object kinds sharing a durable schema kind require one physicalization dispatch pair");

        validated.push_back(registration);
    }

    std::sort(
        validated.begin(),
        validated.end(),
        [](const auto & lhs, const auto & rhs)
        {
            if (lhs.schema_object_kind != rhs.schema_object_kind)
                return static_cast<UInt8>(lhs.schema_object_kind) < static_cast<UInt8>(rhs.schema_object_kind);
            return static_cast<UInt8>(lhs.object_kind) < static_cast<UInt8>(rhs.object_kind);
        });
    return StoredObjectPhysicalizationAdapterRegistry(std::move(validated));
}

const StoredObjectPhysicalizationAdapterRegistration *
StoredObjectPhysicalizationAdapterRegistry::tryGet(StoredObjectKind object_kind, StoredObjectSourceMode source_mode) const noexcept
{
    const auto source_mode_mask = storedObjectSourceModeMask(source_mode);
    const auto it = std::find_if(
        registrations.begin(),
        registrations.end(),
        [object_kind, source_mode_mask](const auto & registration)
        { return registration.object_kind == object_kind && (registration.source_modes & source_mode_mask); });
    return it == registrations.end() ? nullptr : &*it;
}

std::optional<StoredObjectPhysicalizationDispatch> StoredObjectPhysicalizationAdapterRegistry::tryGetDispatch(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    StoredObjectOccurrenceSiteMask required_occurrence_sites) const noexcept
{
    const auto * registration = tryGet(object_kind, source_mode);
    if (!registration || (registration->occurrence_sites & required_occurrence_sites) != required_occurrence_sites)
        return std::nullopt;
    return StoredObjectPhysicalizationDispatch(
        registration->schema_object_kind, *registration->object_provider, *registration->rewrite_adapter);
}

std::optional<StoredObjectPhysicalizationDispatch>
StoredObjectPhysicalizationAdapterRegistry::tryGetDispatch(SchemaObjectKind schema_object_kind) const noexcept
{
    const auto it = std::find_if(
        registrations.begin(),
        registrations.end(),
        [schema_object_kind](const auto & registration) { return registration.schema_object_kind == schema_object_kind; });
    if (it == registrations.end())
        return std::nullopt;
    return StoredObjectPhysicalizationDispatch(it->schema_object_kind, *it->object_provider, *it->rewrite_adapter);
}

bool StoredObjectPhysicalizationAdapterRegistry::hasCompleteAdapter(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    StoredObjectOccurrenceSiteMask required_occurrence_sites) const noexcept
{
    return tryGetDispatch(object_kind, source_mode, required_occurrence_sites).has_value();
}

StoredObjectPhysicalizationAdapterRegistry::StoredObjectPhysicalizationAdapterRegistry(
    std::vector<StoredObjectPhysicalizationAdapterRegistration> registrations_)
    : registrations(std::move(registrations_))
{
}

StoredObjectAdmissionResult::StoredObjectAdmissionResult(
    StoredObjectAdmissionStatus status_,
    StoredObjectAdmissionRejection rejection_,
    StoredObjectKind object_kind_,
    StoredObjectSourceMode source_mode_,
    StoredObjectOccurrenceSite occurrence_site_,
    UInt64 exact_descriptor_count_)
    : status(status_)
    , rejection(rejection_)
    , object_kind(object_kind_)
    , source_mode(source_mode_)
    , occurrence_site(occurrence_site_)
    , exact_descriptor_count(exact_descriptor_count_)
{
}

StoredObjectAdmissionDispatch::StoredObjectAdmissionDispatch(
    StoredObjectAdmissionResult admission_, std::optional<StoredObjectPhysicalizationDispatch> physicalization_dispatch_)
    : admission(std::move(admission_))
    , physicalization_dispatch(std::move(physicalization_dispatch_))
{
    if (admission.hasLogicalReferences() != physicalization_dispatch.has_value())
        throw std::logic_error("logical stored-object admission and physicalization dispatch disagree");
}

StoredObjectAdmissionResult rejectUnsupportedStoredObjectContext(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    StoredObjectOccurrenceSite occurrence_site,
    StoredObjectAdmissionRejection rejection) noexcept
{
    if (rejection == StoredObjectAdmissionRejection::None)
        rejection = StoredObjectAdmissionRejection::UnsupportedOccurrenceSite;
    return StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus::UnsupportedContext, rejection, object_kind, source_mode, occurrence_site, 0);
}

StoredObjectAdmissionResult
admitStoredObjectPhysicalOnly(StoredObjectKind object_kind, StoredObjectSourceMode source_mode, const UUID & target_database_uuid) noexcept
{
    if (!storedObjectKindMask(object_kind))
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, StoredObjectOccurrenceSite::Unclassified, StoredObjectAdmissionRejection::UnclassifiedObjectKind);
    if (!storedObjectSourceModeMask(source_mode))
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, StoredObjectOccurrenceSite::Unclassified, StoredObjectAdmissionRejection::UnclassifiedSourceMode);
    const auto * contract = tryGetStoredObjectSourceModeContract(object_kind, source_mode);
    if (!contract || !contract->physical_only_allowed)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            StoredObjectOccurrenceSite::Unclassified,
            StoredObjectAdmissionRejection::UnsupportedObjectSourceMode);
    if (target_database_uuid == UUIDHelpers::Nil)
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, StoredObjectOccurrenceSite::Unclassified, StoredObjectAdmissionRejection::InvalidTargetDatabase);
    return StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus::PhysicalOnly,
        StoredObjectAdmissionRejection::None,
        object_kind,
        source_mode,
        StoredObjectOccurrenceSite::Unclassified,
        0);
}

StoredObjectAdmissionResult admitStoredObjectExplicitDestination(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    const auto physical_result = admitStoredObjectPhysicalOnly(object_kind, source_mode, target_database_uuid);
    if (!physical_result.isAccepted())
        return physical_result;

    const auto * contract = tryGetStoredObjectSourceModeContract(object_kind, source_mode);
    if (!contract->explicit_destination_columns_allowed)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::InvalidProvenanceSource);
    if (exact_descriptors.empty())
        return physical_result;
    if (exact_descriptors.size() > maximum_stored_object_admission_work_items)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::LimitExceeded);

    std::vector<DescriptorDefinitionProvenanceEntry> definitions;
    if (!reserveDescriptorDefinitionProvenance(definitions, exact_descriptors.size()))
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::LimitExceeded);
    for (const auto & descriptor : exact_descriptors)
    {
        const auto rejection = collectDescriptorDefinitionProvenance(descriptor, target_database_uuid, definitions);
        if (rejection != StoredObjectAdmissionRejection::None)
            return rejectUnsupportedStoredObjectContext(
                object_kind, source_mode, firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites), rejection);
    }
    if (const auto rejection = validateDescriptorDefinitionProvenance(definitions); rejection != StoredObjectAdmissionRejection::None)
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites), rejection);
    if (!adapter_registry.hasCompleteAdapter(object_kind, source_mode, contract->required_logical_occurrence_sites))
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::MissingPhysicalizationAdapter);

    return StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus::Logical,
        StoredObjectAdmissionRejection::None,
        object_kind,
        source_mode,
        firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
        static_cast<UInt64>(exact_descriptors.size()));
}

StoredObjectAdmissionResult admitStoredObjectSourceSidecar(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    const PersistedTypeReferences & source_references,
    const BoundObjectTypeReferences & bound_source_references,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    const auto physical_result = admitStoredObjectPhysicalOnly(object_kind, source_mode, target_database_uuid);
    if (!physical_result.isAccepted())
        return physical_result;

    const auto * contract = tryGetStoredObjectSourceModeContract(object_kind, source_mode);
    if (contract->implicit_provenance != StoredObjectProvenanceRule::ExactSourceSidecar
        && contract->implicit_provenance != StoredObjectProvenanceRule::ExactAttachedSidecar)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::InvalidProvenanceSource);

    const auto expected_schema_kind = tryGetSchemaObjectKind(object_kind);
    if (!expected_schema_kind || source_references.object.kind != *expected_schema_kind)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::SourceSidecarMismatch);
    if (source_references.object.database_uuid != target_database_uuid)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::CrossDatabaseDescriptor);
    if (bound_source_references.getObject() != source_references.object
        || bound_source_references.getObjectSchemaRevision() != source_references.object_schema_revision
        || bound_source_references.getPhysicalSchemaFingerprint() != source_references.physical_schema_fingerprint
        || bound_source_references.getDescriptors().size() != source_references.descriptors.size()
        || bound_source_references.getUses().size() != source_references.uses.size())
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::SourceSidecarMismatch);
    if (source_references.descriptors.size() > maximum_stored_object_admission_work_items
        || source_references.occurrence_paths.size() > maximum_stored_object_admission_work_items
        || source_references.uses.size() > maximum_stored_object_admission_work_items)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::LimitExceeded);

    try
    {
        if (computePersistedTypeReferencesSidecarHash(source_references) != bound_source_references.getSidecarHash())
            return rejectUnsupportedStoredObjectContext(
                object_kind,
                source_mode,
                firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
                StoredObjectAdmissionRejection::SourceSidecarMismatch);
    }
    catch (const PersistedTypeReferencesError &)
    {
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::SourceSidecarMismatch);
    }

    std::vector<DescriptorDefinitionProvenanceEntry> definitions;
    if (!reserveDescriptorDefinitionProvenance(definitions, source_references.descriptors.size()))
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::LimitExceeded);
    for (const auto & descriptor : source_references.descriptors)
    {
        const auto rejection = collectDescriptorDefinitionProvenance(descriptor, target_database_uuid, definitions);
        if (rejection != StoredObjectAdmissionRejection::None)
            return rejectUnsupportedStoredObjectContext(
                object_kind, source_mode, firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites), rejection);
    }
    if (const auto rejection = validateDescriptorDefinitionProvenance(definitions); rejection != StoredObjectAdmissionRejection::None)
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites), rejection);
    if (!adapter_registry.hasCompleteAdapter(object_kind, source_mode, contract->required_logical_occurrence_sites))
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::MissingPhysicalizationAdapter);

    return StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus::Logical,
        StoredObjectAdmissionRejection::None,
        object_kind,
        source_mode,
        firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
        static_cast<UInt64>(source_references.descriptors.size()));
}

StoredObjectAdmissionResult admitStoredObjectSelectedOutputs(
    StoredObjectKind object_kind,
    StoredObjectSourceMode source_mode,
    const UUID & target_database_uuid,
    std::span<const StoredObjectSelectedOutput> selected_outputs,
    std::span<const StoredObjectExactOccurrence> exact_stored_occurrences,
    bool classification_complete,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    const auto physical_result = admitStoredObjectPhysicalOnly(object_kind, source_mode, target_database_uuid);
    if (!physical_result.isAccepted())
        return physical_result;

    const auto * contract = tryGetStoredObjectSourceModeContract(object_kind, source_mode);
    if (contract->implicit_provenance != StoredObjectProvenanceRule::ExactSelectedOutput)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::InvalidProvenanceSource);
    if (!classification_complete)
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            StoredObjectOccurrenceSite::UnclassifiedTypeString,
            StoredObjectAdmissionRejection::IncompleteOutputClassification);
    if (selected_outputs.size() > maximum_stored_object_admission_work_items
        || exact_stored_occurrences.size() > maximum_stored_object_admission_work_items - selected_outputs.size())
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::LimitExceeded);

    const auto output_site = selectedOutputSite(object_kind);
    const auto * output_site_contract = tryGetStoredObjectOccurrenceSiteContract(output_site);
    if (!output_site_contract || output_site_contract->disposition != StoredObjectOccurrenceDisposition::ExactPersistedPath
        || !(output_site_contract->owner_kinds & storedObjectKindMask(object_kind)))
        return rejectUnsupportedStoredObjectContext(
            object_kind, source_mode, output_site, StoredObjectAdmissionRejection::UnsupportedOccurrenceSite);

    UInt64 descriptor_count = 0;
    StoredObjectOccurrenceSiteMask required_logical_sites = 0;
    std::vector<DescriptorDefinitionProvenanceEntry> definitions;
    const size_t maximum_provenance_entries = selected_outputs.size() + exact_stored_occurrences.size();
    if (!reserveDescriptorDefinitionProvenance(definitions, maximum_provenance_entries))
        return rejectUnsupportedStoredObjectContext(object_kind, source_mode, output_site, StoredObjectAdmissionRejection::LimitExceeded);
    for (const auto & output : selected_outputs)
    {
        const auto * descriptor = output.tryGetExactDescriptor();
        if (!descriptor)
            continue;
        const auto rejection = collectDescriptorDefinitionProvenance(*descriptor, target_database_uuid, definitions);
        if (rejection != StoredObjectAdmissionRejection::None)
            return rejectUnsupportedStoredObjectContext(object_kind, source_mode, output_site, rejection);
        ++descriptor_count;
        required_logical_sites |= storedObjectOccurrenceSiteMask(output_site);
    }

    for (const auto & occurrence : exact_stored_occurrences)
    {
        const auto * occurrence_contract = tryGetStoredObjectOccurrenceSiteContract(occurrence.site);
        if (!occurrence_contract || occurrence_contract->disposition != StoredObjectOccurrenceDisposition::ExactPersistedPath)
            return rejectUnsupportedStoredObjectContext(
                object_kind, source_mode, occurrence.site, StoredObjectAdmissionRejection::UnsupportedOccurrenceSite);
        if (!(occurrence_contract->owner_kinds & storedObjectKindMask(object_kind)))
            return rejectUnsupportedStoredObjectContext(
                object_kind, source_mode, occurrence.site, StoredObjectAdmissionRejection::OccurrenceOwnerMismatch);
        const auto rejection = collectDescriptorDefinitionProvenance(occurrence.descriptor, target_database_uuid, definitions);
        if (rejection != StoredObjectAdmissionRejection::None)
            return rejectUnsupportedStoredObjectContext(object_kind, source_mode, occurrence.site, rejection);
        ++descriptor_count;
        required_logical_sites |= storedObjectOccurrenceSiteMask(occurrence.site);
    }

    if (!descriptor_count)
        return physical_result;
    if (const auto rejection = validateDescriptorDefinitionProvenance(definitions); rejection != StoredObjectAdmissionRejection::None)
        return rejectUnsupportedStoredObjectContext(object_kind, source_mode, output_site, rejection);
    if (!required_logical_sites || !adapter_registry.hasCompleteAdapter(object_kind, source_mode, required_logical_sites))
        return rejectUnsupportedStoredObjectContext(
            object_kind,
            source_mode,
            firstRequiredOccurrenceSite(required_logical_sites ? required_logical_sites : contract->required_logical_occurrence_sites),
            StoredObjectAdmissionRejection::MissingPhysicalizationAdapter);

    return StoredObjectAdmissionResult(
        StoredObjectAdmissionStatus::Logical,
        StoredObjectAdmissionRejection::None,
        object_kind,
        source_mode,
        output_site,
        descriptor_count);
}

namespace
{

struct ExactDeclarationCreateAdmissionContract
{
    StoredObjectKind object_kind{};
    StoredObjectOccurrenceSite occurrence_site{};
    StoredObjectSourceModeMask source_modes = 0;
    bool requires_explicit_destination_columns = false;
    bool allows_materialized_view_targets_and_populate = false;
};

struct ExactDeclarationCreateAdmission
{
    StoredObjectAdmissionResult admission;
    std::optional<StoredObjectPhysicalizationDispatch> physicalization_dispatch;
};

ExactDeclarationCreateAdmission admitStoredObjectExactDeclarationCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_descriptors,
    const ExactDeclarationCreateAdmissionContract & admission_contract,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    const auto classification = classifyStoredObjectCreateQuery(create);
    const auto occurrence_site_mask = storedObjectOccurrenceSiteMask(admission_contract.occurrence_site);

    StoredObjectAdmissionRejection rejection = StoredObjectAdmissionRejection::None;
    if (!classification.structured_udt_scan_complete)
        rejection = StoredObjectAdmissionRejection::IncompleteOutputClassification;
    else if (!classification.type_string_scan_complete)
        rejection = StoredObjectAdmissionRejection::IncompleteTypeStringClassification;
    else if (classification.object_kind == StoredObjectKind::Unclassified)
        rejection = StoredObjectAdmissionRejection::UnclassifiedObjectKind;
    else if (classification.object_kind != admission_contract.object_kind)
        rejection = StoredObjectAdmissionRejection::UnsupportedObjectSourceMode;
    else if (classification.source_mode == StoredObjectSourceMode::Unclassified)
        rejection = StoredObjectAdmissionRejection::UnclassifiedSourceMode;
    else if (!(admission_contract.source_modes & storedObjectSourceModeMask(classification.source_mode)))
        rejection = StoredObjectAdmissionRejection::UnsupportedObjectSourceMode;
    else if (
        (admission_contract.requires_explicit_destination_columns && !classification.has_explicit_destination_columns) || create.attach
        || create.if_not_exists || create.replace_view || create.replace_table || create.create_or_replace || create.has_attach_from_path
        || create.attach_short_syntax || create.attach_as_replicated.has_value() || !create.cluster.empty()
        || ((!admission_contract.allows_materialized_view_targets_and_populate) && (create.targets || create.is_populate))
        || create.refresh_strategy)
        rejection = StoredObjectAdmissionRejection::InvalidProvenanceSource;
    else if (
        classification.has_unclassified_udt_reference || classification.source_query_has_structured_udt_reference
        || classification.qualified_type_reference_candidate_sites != 0
        || (classification.structured_udt_occurrence_sites & ~occurrence_site_mask) != 0)
        rejection = StoredObjectAdmissionRejection::UnsupportedOccurrenceSite;
    else if ((classification.structured_udt_occurrence_sites != 0) != !exact_descriptors.empty())
        rejection = StoredObjectAdmissionRejection::InvalidProvenanceSource;

    if (rejection != StoredObjectAdmissionRejection::None)
    {
        return {
            .admission = rejectUnsupportedStoredObjectContext(
                classification.object_kind, classification.source_mode, admission_contract.occurrence_site, rejection),
            .physicalization_dispatch = std::nullopt,
        };
    }

    auto admission = admitStoredObjectExplicitDestination(
        classification.object_kind, classification.source_mode, target_database_uuid, exact_descriptors, adapter_registry);
    if (!admission.hasLogicalReferences())
        return {.admission = std::move(admission), .physicalization_dispatch = std::nullopt};

    const auto * contract = tryGetStoredObjectSourceModeContract(classification.object_kind, classification.source_mode);
    auto dispatch = contract ? adapter_registry.tryGetDispatch(
                                   classification.object_kind, classification.source_mode, contract->required_logical_occurrence_sites)
                             : std::nullopt;
    if (!dispatch)
    {
        return {
            .admission = rejectUnsupportedStoredObjectContext(
                classification.object_kind,
                classification.source_mode,
                admission_contract.occurrence_site,
                StoredObjectAdmissionRejection::MissingPhysicalizationAdapter),
            .physicalization_dispatch = std::nullopt,
        };
    }
    return {.admission = std::move(admission), .physicalization_dispatch = std::move(dispatch)};
}

}

StoredObjectAdmissionDispatch admitStoredObjectExplicitViewOutputCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_output_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    constexpr ExactDeclarationCreateAdmissionContract contract{
        .object_kind = StoredObjectKind::View,
        .occurrence_site = StoredObjectOccurrenceSite::ViewOutputDeclaration,
        .source_modes
        = storedObjectSourceModeMask(StoredObjectSourceMode::AsSelect) | storedObjectSourceModeMask(StoredObjectSourceMode::EmptyAsSelect),
        .requires_explicit_destination_columns = true,
    };
    auto result
        = admitStoredObjectExactDeclarationCreate(create, target_database_uuid, exact_output_descriptors, contract, adapter_registry);
    return StoredObjectAdmissionDispatch(std::move(result.admission), std::move(result.physicalization_dispatch));
}

StoredObjectAdmissionDispatch admitStoredObjectExplicitMaterializedViewOutputCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_output_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    constexpr ExactDeclarationCreateAdmissionContract contract{
        .object_kind = StoredObjectKind::MaterializedView,
        .occurrence_site = StoredObjectOccurrenceSite::MaterializedViewOutputDeclaration,
        .source_modes
        = storedObjectSourceModeMask(StoredObjectSourceMode::AsSelect) | storedObjectSourceModeMask(StoredObjectSourceMode::EmptyAsSelect),
        .requires_explicit_destination_columns = true,
        .allows_materialized_view_targets_and_populate = true,
    };
    auto result
        = admitStoredObjectExactDeclarationCreate(create, target_database_uuid, exact_output_descriptors, contract, adapter_registry);
    return StoredObjectAdmissionDispatch(std::move(result.admission), std::move(result.physicalization_dispatch));
}

StoredObjectAdmissionDispatch admitStoredObjectDictionaryAttributeCreate(
    const ASTCreateQuery & create,
    const UUID & target_database_uuid,
    std::span<const PersistedTypeDescriptor> exact_attribute_descriptors,
    const StoredObjectPhysicalizationAdapterRegistry & adapter_registry)
{
    constexpr ExactDeclarationCreateAdmissionContract contract{
        .object_kind = StoredObjectKind::Dictionary,
        .occurrence_site = StoredObjectOccurrenceSite::DictionaryAttribute,
        .source_modes = storedObjectSourceModeMask(StoredObjectSourceMode::ObjectDefinition),
    };
    auto result
        = admitStoredObjectExactDeclarationCreate(create, target_database_uuid, exact_attribute_descriptors, contract, adapter_registry);
    return StoredObjectAdmissionDispatch(std::move(result.admission), std::move(result.physicalization_dispatch));
}

std::string_view getStoredObjectAdmissionRejectionName(StoredObjectAdmissionRejection rejection) noexcept
{
    switch (rejection)
    {
        case StoredObjectAdmissionRejection::None: return "none";
        case StoredObjectAdmissionRejection::UnclassifiedObjectKind: return "unclassified object kind";
        case StoredObjectAdmissionRejection::UnclassifiedSourceMode: return "unclassified source mode";
        case StoredObjectAdmissionRejection::UnsupportedObjectSourceMode: return "unsupported object/source-mode pair";
        case StoredObjectAdmissionRejection::UnsupportedOccurrenceSite: return "unsupported persisted occurrence site";
        case StoredObjectAdmissionRejection::OccurrenceOwnerMismatch: return "occurrence site belongs to another object kind";
        case StoredObjectAdmissionRejection::InvalidTargetDatabase: return "invalid target database identity";
        case StoredObjectAdmissionRejection::InvalidProvenanceSource: return "invalid logical provenance source";
        case StoredObjectAdmissionRejection::IncompleteOutputClassification: return "incomplete selected-output classification";
        case StoredObjectAdmissionRejection::IncompleteTypeStringClassification: return "incomplete type-string classification";
        case StoredObjectAdmissionRejection::CrossDatabaseDescriptor: return "logical descriptor belongs to another database";
        case StoredObjectAdmissionRejection::ConflictingDescriptorIdentity:
            return "one type identity has conflicting definition provenance";
        case StoredObjectAdmissionRejection::SourceSidecarMismatch: return "source sidecar and bound snapshot differ";
        case StoredObjectAdmissionRejection::MissingPhysicalizationAdapter: return "physicalization adapter is not completely registered";
        case StoredObjectAdmissionRejection::LimitExceeded: return "stored-object admission input exceeds its work limit";
    }
    return "unknown stored-object admission rejection";
}
}
