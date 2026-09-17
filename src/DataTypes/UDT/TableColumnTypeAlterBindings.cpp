#include <DataTypes/UDT/TableColumnTypeAlterBindings.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNested.h>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = TableColumnTypeBindingError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

bool binaryLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool descriptorLess(const PersistedTypeDescriptor & lhs, const PersistedTypeDescriptor & rhs) noexcept
{
    if (lhs.stableLess(rhs))
        return true;
    if (rhs.stableLess(lhs))
        return false;
    return binaryLess(lhs.getLastKnownQualifiedName(), rhs.getLastKnownQualifiedName());
}

bool pathLess(const PersistedTypeOccurrencePath & lhs, const PersistedTypeOccurrencePath & rhs) noexcept
{
    if (lhs.section != rhs.section)
        return static_cast<UInt8>(lhs.section) < static_cast<UInt8>(rhs.section);
    if (lhs.site != rhs.site)
        return static_cast<UInt8>(lhs.site) < static_cast<UInt8>(rhs.site);
    if (lhs.object_ordinal != rhs.object_ordinal)
        return lhs.object_ordinal < rhs.object_ordinal;
    if (lhs.occurrence_ordinal != rhs.occurrence_ordinal)
        return lhs.occurrence_ordinal < rhs.occurrence_ordinal;
    return std::lexicographical_compare(
        lhs.type_child_ordinals.begin(),
        lhs.type_child_ordinals.end(),
        rhs.type_child_ordinals.begin(),
        rhs.type_child_ordinals.end());
}

struct LogicalOccurrence
{
    PersistedTypeOccurrencePath path;
    PersistedTypeDescriptor descriptor;
};

using ColumnLogicalState = std::map<String, std::vector<LogicalOccurrence>, std::less<>>;

PersistedTypeReferences reconstructBoundReferences(
    const NamesAndTypesList & physical_columns,
    const BoundObjectTypeReferences & bound,
    const SidecarExpectationRecord & expectation,
    UInt64 revision,
    const TableColumnTypeBindingLimits & limits)
{
    if (!revision)
        fail(Error::Code::InvalidObject, "table-column ALTER successor revision is zero");
    if (expectation.object != bound.getObject() || expectation.object_schema_revision != bound.getObjectSchemaRevision()
        || expectation.sidecar_hash != bound.getSidecarHash()
        || expectation.physical_schema_fingerprint != bound.getPhysicalSchemaFingerprint())
    {
        fail(Error::Code::SidecarMismatch, "table-column ALTER retained binding differs from its expectation");
    }

    PersistedTypeReferences references;
    references.format_version = bound.getFormatVersion();
    references.path_dictionary_version = bound.getPathDictionaryVersion();
    references.object = bound.getObject();
    references.object_schema_revision = revision;
    references.physical_schema_fingerprint = computeTableColumnPhysicalSchemaFingerprint(physical_columns, limits);
    references.semantic_extension_version = expectation.semantic_extension_version;
    references.semantic_extension_flags = expectation.semantic_extension_flags;
    const auto descriptors = bound.getDescriptors();
    references.descriptors.reserve(descriptors.size());
    for (const auto & descriptor : descriptors)
    {
        if (!descriptor)
            fail(Error::Code::SidecarMismatch, "table-column ALTER retained a null descriptor");
        references.descriptors.push_back(descriptor->getPersistedDescriptor());
    }
    const auto uses = bound.getUses();
    references.occurrence_paths.reserve(uses.size());
    references.uses.reserve(uses.size());
    for (size_t index = 0; index < uses.size(); ++index)
    {
        if (uses[index].getDescriptorIndex() >= descriptors.size())
            fail(Error::Code::SidecarMismatch, "table-column ALTER retained an out-of-range descriptor use");
        references.occurrence_paths.push_back(uses[index].getPath());
        references.uses.push_back({.path_id = static_cast<UInt64>(index), .descriptor_id = uses[index].getDescriptorIndex()});
    }
    return references;
}

ColumnLogicalState splitByColumn(
    const NamesAndTypesList & physical_columns,
    const PersistedTypeReferences & references)
{
    ColumnLogicalState state;
    for (size_t index = 0; index < references.uses.size(); ++index)
    {
        const auto & use = references.uses[index];
        if (use.path_id >= references.occurrence_paths.size() || use.descriptor_id >= references.descriptors.size())
            fail(Error::Code::SidecarMismatch, "table-column ALTER sidecar use is out of range");
        const auto & path = references.occurrence_paths[use.path_id];
        if (path.section != PersistedTypePathSection::ColumnType || path.site != PersistedTypeOccurrenceSite::Declaration
            || path.object_ordinal >= physical_columns.size())
            fail(Error::Code::PathMismatch, "table-column ALTER sidecar path is outside the physical columns");
        auto column = physical_columns.begin();
        std::advance(column, static_cast<std::ptrdiff_t>(path.object_ordinal));
        state[column->name].push_back({
            .path = path,
            .descriptor = references.descriptors[use.descriptor_id],
        });
    }
    return state;
}

bool isNestedName(std::string_view name, std::string_view prefix) noexcept
{
    return name.size() > prefix.size() && name.starts_with(prefix) && name[prefix.size()] == '.';
}

void eraseColumnAndNested(ColumnLogicalState & state, std::string_view name)
{
    std::erase_if(state, [&](const auto & item) { return item.first == name || isNestedName(item.first, name); });
}

void renameColumnAndNested(ColumnLogicalState & state, std::string_view from, std::string_view to)
{
    std::vector<std::pair<String, std::vector<LogicalOccurrence>>> moved;
    for (auto it = state.begin(); it != state.end();)
    {
        if (it->first == from || isNestedName(it->first, from))
        {
            String target(to);
            target.append(it->first.substr(from.size()));
            moved.emplace_back(std::move(target), std::move(it->second));
            it = state.erase(it);
        }
        else
            ++it;
    }
    for (auto & [name, occurrences] : moved)
    {
        if (!state.emplace(std::move(name), std::move(occurrences)).second)
            fail(Error::Code::InvalidColumn, "table-column ALTER rename collides with retained logical state");
    }
}

ColumnLogicalState fragmentOccurrences(
    const PersistedTypeReferences & fragment,
    const TableColumnTypeAlterOperation & operation,
    const NamesAndTypesList & after_columns)
{
    ColumnLogicalState result;
    const auto & materialized_columns = operation.physical_columns_after_operation.empty()
        ? after_columns
        : operation.physical_columns_after_operation;
    const bool exact_column = std::any_of(
        materialized_columns.begin(),
        materialized_columns.end(),
        [&](const auto & column) { return column.name == operation.column_name; });
    const auto * nested = operation.replacement_physical_type
        ? typeid_cast<const DataTypeNestedCustomName *>(operation.replacement_physical_type->getCustomName())
        : nullptr;
    if (!exact_column && !nested)
        fail(Error::Code::PathMismatch, "table-column ALTER replacement did not materialize its declared physical column");

    for (const auto & use : fragment.uses)
    {
        if (use.path_id >= fragment.occurrence_paths.size() || use.descriptor_id >= fragment.descriptors.size())
            fail(Error::Code::SidecarMismatch, "table-column ALTER replacement fragment is malformed");
        auto path = fragment.occurrence_paths[use.path_id];
        if (path.section != PersistedTypePathSection::ColumnType || path.site != PersistedTypeOccurrenceSite::Declaration
            || path.object_ordinal != 0)
            fail(Error::Code::PathMismatch, "table-column ALTER replacement is not a one-column fragment");
        String target_name = operation.column_name;
        if (!exact_column)
        {
            /// ColumnsDescription::flattenNested changes a custom Nested root
            /// into one Array(element) column per named child. The permanent
            /// normalized path of a Nested child starts directly with that
            /// child ordinal; the flattened Array contributes a new leading
            /// child-0 locator.
            if (path.type_child_ordinals.empty() || path.type_child_ordinals.front() >= nested->getNames().size())
            {
                fail(
                    Error::Code::PathMismatch,
                    "a root-level Nested UDT occurrence cannot be represented after flatten_nested expansion");
            }
            const size_t child_ordinal = static_cast<size_t>(path.type_child_ordinals.front());
            target_name.append(".");
            target_name.append(nested->getNames()[child_ordinal]);
            path.type_child_ordinals.erase(path.type_child_ordinals.begin());
            path.type_child_ordinals.insert(path.type_child_ordinals.begin(), 0);
            if (std::none_of(
                    materialized_columns.begin(),
                    materialized_columns.end(),
                    [&](const auto & column) { return column.name == target_name; }))
            {
                fail(Error::Code::PathMismatch, "flatten_nested removed a reference-bearing Nested field");
            }
        }
        result[target_name].push_back({.path = std::move(path), .descriptor = fragment.descriptors[use.descriptor_id]});
    }
    return result;
}

void applyLogicalOperations(
    ColumnLogicalState & state,
    const SchemaObjectID & object,
    UInt16 semantic_extension_version,
    UInt16 semantic_extension_flags,
    const NamesAndTypesList & after_columns,
    std::span<const TableColumnTypeAlterOperation> operations)
{
    for (const auto & operation : operations)
    {
        if (operation.column_name.empty())
            fail(Error::Code::InvalidColumn, "table-column ALTER operation has an empty column name");
        switch (operation.kind)
        {
            case TableColumnTypeAlterOperationKind::Drop:
                eraseColumnAndNested(state, operation.column_name);
                break;
            case TableColumnTypeAlterOperationKind::Rename:
                if (operation.target_name.empty() || operation.target_name == operation.column_name)
                    fail(Error::Code::InvalidColumn, "table-column ALTER rename target is invalid");
                renameColumnAndNested(state, operation.column_name, operation.target_name);
                break;
            case TableColumnTypeAlterOperationKind::Replace:
            {
                eraseColumnAndNested(state, operation.column_name);
                if (!operation.replacement_column_references)
                    break;
                const auto & fragment = *operation.replacement_column_references;
                if (fragment.object != object || fragment.semantic_extension_version != semantic_extension_version
                    || fragment.semantic_extension_flags != semantic_extension_flags)
                {
                    fail(Error::Code::SidecarMismatch, "table-column ALTER replacement fragment belongs to another table or semantic domain");
                }
                auto replacement = fragmentOccurrences(fragment, operation, after_columns);
                if (replacement.empty())
                    fail(Error::Code::SidecarMismatch, "logical table-column ALTER replacement has no occurrences");
                for (auto & [name, occurrences] : replacement)
                {
                    if (!state.emplace(std::move(name), std::move(occurrences)).second)
                        fail(Error::Code::InvalidColumn, "table-column ALTER replacement collides with retained logical state");
                }
                break;
            }
        }
    }
}

std::optional<PersistedTypeReferences> canonicalizeDesiredReferences(
    const SchemaObjectID & object,
    UInt64 successor_revision,
    UInt16 semantic_extension_version,
    UInt16 semantic_extension_flags,
    ColumnLogicalState state,
    const NamesAndTypesList & after_columns,
    const TableColumnTypeBindingLimits & limits)
{
    std::map<String, UInt64, std::less<>> after_ordinals;
    UInt64 after_ordinal = 0;
    for (const auto & column : after_columns)
    {
        if (!after_ordinals.emplace(column.name, after_ordinal).second)
            fail(Error::Code::InvalidColumn, "table-column ALTER final physical columns contain a duplicate name");
        ++after_ordinal;
    }
    for (const auto & [name, occurrences] : state)
    {
        static_cast<void>(occurrences);
        if (!after_ordinals.contains(name))
            fail(Error::Code::PathMismatch, "table-column ALTER would lose a logical occurrence without an explicit operation");
    }
    if (state.empty())
        return std::nullopt;

    std::vector<PersistedTypeDescriptor> descriptors;
    std::vector<LogicalOccurrence> occurrences;
    for (auto & [name, column_occurrences] : state)
    {
        const UInt64 ordinal = after_ordinals.at(name);
        for (auto & occurrence : column_occurrences)
        {
            occurrence.path.object_ordinal = ordinal;
            descriptors.push_back(occurrence.descriptor);
            occurrences.push_back(std::move(occurrence));
        }
    }
    std::sort(descriptors.begin(), descriptors.end(), descriptorLess);
    descriptors.erase(
        std::unique(
            descriptors.begin(),
            descriptors.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.hasSameInstantiation(rhs); }),
        descriptors.end());
    std::sort(occurrences.begin(), occurrences.end(), [](const auto & lhs, const auto & rhs) { return pathLess(lhs.path, rhs.path); });
    for (size_t index = 1; index < occurrences.size(); ++index)
        if (!pathLess(occurrences[index - 1].path, occurrences[index].path))
            fail(Error::Code::PathMismatch, "table-column ALTER produced duplicate logical occurrence paths");

    PersistedTypeReferences desired;
    desired.object = object;
    desired.object_schema_revision = successor_revision;
    desired.physical_schema_fingerprint = computeTableColumnPhysicalSchemaFingerprint(after_columns, limits);
    desired.semantic_extension_version = semantic_extension_version;
    desired.semantic_extension_flags = semantic_extension_flags;
    desired.descriptors = descriptors;
    desired.occurrence_paths.reserve(occurrences.size());
    desired.uses.reserve(occurrences.size());
    for (size_t index = 0; index < occurrences.size(); ++index)
    {
        auto descriptor_it = std::find_if(
            descriptors.begin(),
            descriptors.end(),
            [&](const auto & descriptor) { return descriptor.hasSameInstantiation(occurrences[index].descriptor); });
        if (descriptor_it == descriptors.end())
            fail(Error::Code::ConflictingDescriptor, "table-column ALTER occurrence descriptor was not interned");
        desired.occurrence_paths.push_back(std::move(occurrences[index].path));
        desired.uses.push_back({
            .path_id = static_cast<UInt64>(index),
            .descriptor_id = static_cast<UInt64>(descriptor_it - descriptors.begin()),
        });
    }

    static_cast<void>(encodePersistedTypeReferences(desired, limits.persisted));
    static_cast<void>(reconstructTableColumnPhysicalSchema(
        desired.object, desired.object_schema_revision, after_columns, desired, limits));
    return desired;
}

std::optional<PersistedTypeReferences> composeDesiredReferences(
    const NamesAndTypesList & before_columns,
    const BoundObjectTypeReferences & before_bound,
    const SidecarExpectationRecord & before_expectation,
    const NamesAndTypesList & after_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    UInt64 successor_revision,
    const TableColumnTypeBindingLimits & limits)
{
    auto before = reconstructBoundReferences(
        before_columns, before_bound, before_expectation, before_bound.getObjectSchemaRevision(), limits);
    if (computePersistedTypeReferencesSidecarHash(before, limits.persisted) != before_bound.getSidecarHash())
        fail(Error::Code::SidecarMismatch, "table-column ALTER cannot reconstruct its retained sidecar exactly");
    auto state = splitByColumn(before_columns, before);
    applyLogicalOperations(
        state,
        before_bound.getObject(),
        before_expectation.semantic_extension_version,
        before_expectation.semantic_extension_flags,
        after_columns,
        operations);
    return canonicalizeDesiredReferences(
        before_bound.getObject(),
        successor_revision,
        before_expectation.semantic_extension_version,
        before_expectation.semantic_extension_flags,
        std::move(state),
        after_columns,
        limits);
}

PersistedTypeReferences reconstructStoredObjectReferences(
    const BoundObjectTypeReferences & bound,
    const SidecarExpectationRecord & expectation,
    UInt64 object_schema_revision,
    const PersistedTypeReferencesLimits & limits)
{
    if (!object_schema_revision || !bound.getObject().isValid()
        || (bound.getObject().kind != SchemaObjectKind::View && bound.getObject().kind != SchemaObjectKind::Dictionary)
        || expectation.object != bound.getObject() || expectation.object_schema_revision != bound.getObjectSchemaRevision()
        || expectation.sidecar_hash != bound.getSidecarHash()
        || expectation.physical_schema_fingerprint != bound.getPhysicalSchemaFingerprint())
    {
        fail(Error::Code::SidecarMismatch, "stored-object ALTER retained binding differs from its durable expectation");
    }

    PersistedTypeReferences references;
    references.format_version = bound.getFormatVersion();
    references.path_dictionary_version = bound.getPathDictionaryVersion();
    references.object = bound.getObject();
    references.object_schema_revision = object_schema_revision;
    references.physical_schema_fingerprint = bound.getPhysicalSchemaFingerprint();
    references.semantic_extension_version = expectation.semantic_extension_version;
    references.semantic_extension_flags = expectation.semantic_extension_flags;
    references.descriptors.reserve(bound.getDescriptors().size());
    for (const auto & descriptor : bound.getDescriptors())
    {
        if (!descriptor)
            fail(Error::Code::SidecarMismatch, "stored-object ALTER retained a null descriptor");
        references.descriptors.push_back(descriptor->getPersistedDescriptor());
    }
    references.occurrence_paths.reserve(bound.getUses().size());
    references.uses.reserve(bound.getUses().size());
    for (size_t index = 0; index < bound.getUses().size(); ++index)
    {
        const auto & use = bound.getUses()[index];
        if (use.getDescriptorIndex() >= references.descriptors.size())
            fail(Error::Code::SidecarMismatch, "stored-object ALTER retained an out-of-range descriptor use");
        references.occurrence_paths.push_back(use.getPath());
        references.uses.push_back({
            .path_id = static_cast<UInt64>(index),
            .descriptor_id = use.getDescriptorIndex(),
        });
    }
    static_cast<void>(encodePersistedTypeReferences(references, limits));
    return references;
}
}

PreparedTableColumnTypeAlter::PreparedTableColumnTypeAlter(
    SchemaObjectID object_,
    UInt64 before_object_schema_revision_,
    std::optional<PersistedTypeReferences> desired_references_,
    NamesAndTypesList after_physical_columns_)
    : object(object_)
    , before_object_schema_revision(before_object_schema_revision_)
    , desired_references(std::move(desired_references_))
    , after_physical_columns(std::move(after_physical_columns_))
{
}

void PreparedTableColumnTypeAlter::completePublication(
    BoundObjectTypeReferences::Ptr bound_references,
    std::optional<SidecarExpectationRecord> expectation,
    std::shared_ptr<const AuthorityVerificationStamp> verification_stamp)
{
    if (static_cast<bool>(bound_references) != static_cast<bool>(expectation)
        || static_cast<bool>(bound_references) != static_cast<bool>(desired_references)
        || static_cast<bool>(bound_references) != static_cast<bool>(verification_stamp))
    {
        fail(Error::Code::SidecarMismatch, "table-column ALTER publication package has inconsistent logical state");
    }
    if (bound_references
        && (bound_references->getObject() != object
            || bound_references->getObjectSchemaRevision() != desired_references->object_schema_revision
            || bound_references->getPhysicalSchemaFingerprint() != desired_references->physical_schema_fingerprint
            || expectation->object != object
            || expectation->object_schema_revision != desired_references->object_schema_revision
            || expectation->sidecar_hash != bound_references->getSidecarHash()
            || expectation->physical_schema_fingerprint != bound_references->getPhysicalSchemaFingerprint()))
    {
        fail(Error::Code::SidecarMismatch, "table-column ALTER publication differs from its prepared desired sidecar");
    }

    std::lock_guard lock(completion_mutex);
    if (completion_ready)
        fail(Error::Code::SidecarMismatch, "table-column ALTER publication was completed twice");
    completion = {
        .bound_references = std::move(bound_references),
        .expectation = std::move(expectation),
        .verification_stamp = std::move(verification_stamp),
    };
    completion_ready = true;
}

std::optional<CompletedTableColumnTypeAlterPublication> PreparedTableColumnTypeAlter::getCompletedPublication() const
{
    std::lock_guard lock(completion_mutex);
    if (!completion_ready)
        return std::nullopt;
    return completion;
}

std::shared_ptr<PreparedTableColumnTypeAlter> prepareTableColumnTypeAlter(
    const NamesAndTypesList & before_physical_columns,
    const BoundObjectTypeReferences & before_bound_references,
    const SidecarExpectationRecord & before_expectation,
    const NamesAndTypesList & after_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits)
{
    if (before_bound_references.getObjectSchemaRevision() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "table-column ALTER object revision cannot advance");
    auto desired = composeDesiredReferences(
        before_physical_columns,
        before_bound_references,
        before_expectation,
        after_physical_columns,
        operations,
        before_bound_references.getObjectSchemaRevision() + 1,
        limits);
    return std::shared_ptr<PreparedTableColumnTypeAlter>(new PreparedTableColumnTypeAlter(
        before_bound_references.getObject(),
        before_bound_references.getObjectSchemaRevision(),
        std::move(desired),
        after_physical_columns));
}

std::shared_ptr<PreparedTableColumnTypeAlter> prepareInitialTableColumnTypeAlter(
    const NamesAndTypesList & before_physical_columns,
    const NamesAndTypesList & after_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits)
{
    /// Validate both ordinary metadata images even when the logical operations
    /// cancel out and no durable admission is ultimately needed.
    static_cast<void>(computeTableColumnPhysicalSchemaFingerprint(before_physical_columns, limits));
    static_cast<void>(computeTableColumnPhysicalSchemaFingerprint(after_physical_columns, limits));

    const PersistedTypeReferences * identity_fragment = nullptr;
    for (const auto & operation : operations)
    {
        if (!operation.replacement_column_references)
            continue;
        const auto & fragment = *operation.replacement_column_references;
        if (!fragment.object.isValid() || fragment.object.kind != SchemaObjectKind::Table || !fragment.object_schema_revision)
            fail(Error::Code::InvalidObject, "initial table-column ALTER fragment has an invalid table identity");
        if (!identity_fragment)
            identity_fragment = &fragment;
        else if (fragment.object != identity_fragment->object
            || fragment.semantic_extension_version != identity_fragment->semantic_extension_version
            || fragment.semantic_extension_flags != identity_fragment->semantic_extension_flags)
        {
            fail(Error::Code::SidecarMismatch, "initial table-column ALTER fragments do not share one table identity");
        }
    }
    if (!identity_fragment)
        fail(Error::Code::InvalidObject, "initial table-column ALTER has no logical binder fragment");

    ColumnLogicalState state;
    applyLogicalOperations(
        state,
        identity_fragment->object,
        identity_fragment->semantic_extension_version,
        identity_fragment->semantic_extension_flags,
        after_physical_columns,
        operations);
    auto desired = canonicalizeDesiredReferences(
        identity_fragment->object,
        2,
        identity_fragment->semantic_extension_version,
        identity_fragment->semantic_extension_flags,
        std::move(state),
        after_physical_columns,
        limits);
    return std::shared_ptr<PreparedTableColumnTypeAlter>(new PreparedTableColumnTypeAlter(
        identity_fragment->object,
        1,
        std::move(desired),
        after_physical_columns));
}

std::shared_ptr<PreparedTableColumnTypeAlter> prepareStoredObjectTypeAlter(
    const BoundObjectTypeReferences & before_bound_references,
    const SidecarExpectationRecord & before_expectation,
    const NamesAndTypesList & after_physical_columns,
    std::optional<PersistedTypeReferences> desired_references,
    const PersistedTypeReferencesLimits & limits)
{
    if (before_bound_references.getObjectSchemaRevision() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "stored-object ALTER object revision cannot advance");

    auto exact_before = reconstructStoredObjectReferences(
        before_bound_references, before_expectation, before_bound_references.getObjectSchemaRevision(), limits);
    if (computePersistedTypeReferencesSidecarHash(exact_before, limits) != before_bound_references.getSidecarHash())
        fail(Error::Code::SidecarMismatch, "stored-object ALTER cannot reconstruct its retained sidecar exactly");

    if (desired_references)
    {
        if (desired_references->object != before_bound_references.getObject()
            || desired_references->object_schema_revision != before_bound_references.getObjectSchemaRevision() + 1
            || desired_references->descriptors.empty() || desired_references->uses.empty())
        {
            fail(Error::Code::SidecarMismatch, "stored-object ALTER successor sidecar has invalid identity or revision");
        }
        static_cast<void>(encodePersistedTypeReferences(*desired_references, limits));
    }

    return std::shared_ptr<PreparedTableColumnTypeAlter>(new PreparedTableColumnTypeAlter(
        before_bound_references.getObject(),
        before_bound_references.getObjectSchemaRevision(),
        std::move(desired_references),
        after_physical_columns));
}

PersistedTypeReferences rebaseBoundStoredObjectTypeReferences(
    const BoundObjectTypeReferences & retained_bound_references,
    const SidecarExpectationRecord & retained_expectation,
    const PersistedTypeReferencesLimits & limits)
{
    if (retained_bound_references.getObjectSchemaRevision() == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "stored-object ALTER object revision cannot advance");
    auto exact_before = reconstructStoredObjectReferences(
        retained_bound_references, retained_expectation, retained_bound_references.getObjectSchemaRevision(), limits);
    if (computePersistedTypeReferencesSidecarHash(exact_before, limits) != retained_bound_references.getSidecarHash())
        fail(Error::Code::SidecarMismatch, "stored-object ALTER cannot reconstruct its retained sidecar exactly");
    exact_before.object_schema_revision = retained_bound_references.getObjectSchemaRevision() + 1;
    static_cast<void>(encodePersistedTypeReferences(exact_before, limits));
    return exact_before;
}

PreparedTableColumnTypeBindings rebaseInitialTableColumnTypeBindingsAfterNormalization(
    PreparedTableColumnTypeBindings initial_bindings,
    const NamesAndTypesList & normalized_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits)
{
    if (!initial_bindings.persisted_references || !initial_bindings.bound_physical_schema
        || !initial_bindings.sidecar_expectation)
    {
        fail(Error::Code::SidecarMismatch, "CREATE normalization requires one complete logical table-binding package");
    }

    const auto initial_references = *initial_bindings.persisted_references;
    if (initial_references.object_schema_revision == 0
        || initial_references.physical_schema_fingerprint != initial_bindings.physical_schema_fingerprint
        || initial_bindings.sidecar_expectation->object != initial_references.object
        || initial_bindings.sidecar_expectation->object_schema_revision != initial_references.object_schema_revision)
    {
        fail(Error::Code::SidecarMismatch, "CREATE normalization received an inconsistent initial binding package");
    }
    if (operations.size() != initial_bindings.physical_columns.size())
        fail(Error::Code::InvalidColumn, "CREATE normalization operation count differs from its declared physical columns");

    /// The CREATE binder delegates only ordinary flatten_nested=1 to this
    /// composer. Prove the complete normalized schema is exactly that
    /// deterministic transformation before rebasing any occurrence. This
    /// prevents unrelated name/type/order drift in physical-only columns from
    /// being hidden by a valid UDT fragment elsewhere in the table.
    NamesAndTypesList expected_flattened_columns;
    for (const auto & column : initial_bindings.physical_columns)
    {
        const auto * nested = column.type
            ? typeid_cast<const DataTypeNestedCustomName *>(column.type->getCustomName())
            : nullptr;
        if (!nested)
        {
            expected_flattened_columns.push_back(column);
            continue;
        }

        const auto & names = nested->getNames();
        const auto & elements = nested->getElements();
        if (names.empty() || names.size() != elements.size())
            fail(Error::Code::InvalidColumn, "CREATE normalization received a malformed root Nested physical type");
        for (size_t child_index = 0; child_index < names.size(); ++child_index)
        {
            expected_flattened_columns.emplace_back(
                column.name + "." + names[child_index],
                std::make_shared<DataTypeArray>(elements[child_index]));
        }
    }
    if (normalized_physical_columns != expected_flattened_columns)
        fail(Error::Code::InvalidColumn, "CREATE normalization differs from the exact flatten_nested physical schema");

    size_t operation_index = 0;
    for (const auto & column : initial_bindings.physical_columns)
    {
        const auto & operation = operations[operation_index++];
        if (operation.kind != TableColumnTypeAlterOperationKind::Replace || operation.column_name != column.name
            || !operation.replacement_physical_type || !operation.replacement_physical_type->equals(*column.type))
        {
            fail(Error::Code::InvalidColumn, "CREATE normalization operation does not match its declared physical column");
        }
    }

    ColumnLogicalState state;
    applyLogicalOperations(
        state,
        initial_references.object,
        initial_references.semantic_extension_version,
        initial_references.semantic_extension_flags,
        normalized_physical_columns,
        operations);
    auto desired = canonicalizeDesiredReferences(
        initial_references.object,
        initial_references.object_schema_revision,
        initial_references.semantic_extension_version,
        initial_references.semantic_extension_flags,
        std::move(state),
        normalized_physical_columns,
        limits);
    if (!desired)
        fail(Error::Code::SidecarMismatch, "CREATE normalization lost every logical table-column occurrence");

    initial_bindings.physical_columns = normalized_physical_columns;
    initial_bindings.physical_schema_fingerprint = desired->physical_schema_fingerprint;
    initial_bindings.bound_physical_schema = reconstructTableColumnPhysicalSchema(
        desired->object,
        desired->object_schema_revision,
        normalized_physical_columns,
        *desired,
        limits);
    initial_bindings.sidecar_expectation = SidecarExpectationRecord{
        .object = desired->object,
        .object_schema_revision = desired->object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(*desired, limits.persisted),
        .physical_schema_fingerprint = desired->physical_schema_fingerprint,
        .semantic_extension_version = desired->semantic_extension_version,
        .semantic_extension_flags = desired->semantic_extension_flags,
    };

    std::set<SchemaObjectID> dependency_objects;
    for (const auto & descriptor : desired->descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        dependency_objects.insert({
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = identity.database_uuid,
            .object_uuid = identity.type_uuid,
        });
    }
    initial_bindings.dependency_edges.clear();
    initial_bindings.dependency_edges.reserve(dependency_objects.size());
    for (const auto & dependency : dependency_objects)
    {
        initial_bindings.dependency_edges.push_back({
            .dependent = desired->object,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }
    initial_bindings.persisted_references = std::move(*desired);
    return initial_bindings;
}

std::optional<PersistedTypeReferences> rebaseBoundTableColumnTypeReferences(
    const NamesAndTypesList & physical_columns,
    const BoundObjectTypeReferences & retained_bound_references,
    const SidecarExpectationRecord & retained_expectation,
    UInt64 successor_of_revision,
    const TableColumnTypeBindingLimits & limits)
{
    if (successor_of_revision == std::numeric_limits<UInt64>::max())
        fail(Error::Code::LimitExceeded, "table-column ALTER rollback revision cannot advance");
    auto references = reconstructBoundReferences(
        physical_columns, retained_bound_references, retained_expectation, successor_of_revision + 1, limits);
    static_cast<void>(encodePersistedTypeReferences(references, limits.persisted));
    static_cast<void>(reconstructTableColumnPhysicalSchema(
        references.object, references.object_schema_revision, physical_columns, references, limits));
    return references.uses.empty() ? std::nullopt : std::optional<PersistedTypeReferences>(std::move(references));
}

}
