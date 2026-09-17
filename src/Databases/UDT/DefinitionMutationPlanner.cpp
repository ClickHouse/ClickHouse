#include <Databases/UDT/DefinitionMutationPlanner.h>

#include <Databases/UDT/AtomicAuthority.h>

#include <Core/Field.h>

#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using MutationError = DefinitionMutationPlannerError;
using MutationKind = DefinitionMutationKind;

constexpr UInt64 canonical_attach_maximum_parser_depth = 256;
constexpr UInt64 canonical_attach_maximum_parser_backtracks = 100'000;

[[noreturn]] void fail(MutationError::Code code, std::string_view message)
{
    throw MutationError(code, message);
}

String lowerHexDigest(const Digest & digest)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(digest.size() * 2, '\0');
    for (size_t index = 0; index < digest.size(); ++index)
    {
        const UInt8 value = digest[index];
        result[2 * index] = digits[value >> 4];
        result[2 * index + 1] = digits[value & 0x0f];
    }
    return result;
}

int compareUUID(const UUID & lhs, const UUID & rhs) noexcept
{
    const auto lhs_bytes = uuidToCanonicalBytes(lhs);
    const auto rhs_bytes = uuidToCanonicalBytes(rhs);
    if (lhs_bytes == rhs_bytes)
        return 0;
    return lhs_bytes < rhs_bytes ? -1 : 1;
}

int compareIdentity(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    if (const int database_result = compareUUID(lhs.database_uuid, rhs.database_uuid))
        return database_result;
    if (const int type_result = compareUUID(lhs.type_uuid, rhs.type_uuid))
        return type_result;
    if (lhs.revision == rhs.revision)
        return 0;
    return lhs.revision < rhs.revision ? -1 : 1;
}

bool identityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    return compareIdentity(lhs, rhs) < 0;
}

SchemaObjectID definitionObjectID(UUID database_uuid, UUID type_uuid)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = type_uuid,
    };
}

AuthorityInventoryKey definitionInventoryKey(UUID type_uuid)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = type_uuid,
    };
}

DatabaseSchemaWALAuthorityRecordState recordState(const Record & record, const RecordLimits & limits)
{
    return {
        .object_revision = record.identity.revision,
        .canonical_record_hash = computeRecordHash(record, limits),
    };
}

bool sameExecutableBody(const Definition & lhs, const Definition & rhs) noexcept
{
    return lhs.getParameters() == rhs.getParameters() && lhs.getDecreasingParameter() == rhs.getDecreasingParameter()
        && lhs.getNodes() == rhs.getNodes() && lhs.getRoot() == rhs.getRoot() && lhs.isPolicyBearing() == rhs.isPolicyBearing()
        && lhs.getSemanticCapabilities() == rhs.getSemanticCapabilities() && lhs.getCheckerABI() == rhs.getCheckerABI()
        && lhs.getCheckerChargeABI() == rhs.getCheckerChargeABI() && lhs.getPolicyABI() == rhs.getPolicyABI()
        && lhs.getFunctionRegistryABI() == rhs.getFunctionRegistryABI() && lhs.getPolicySemanticHash() == rhs.getPolicySemanticHash()
        && lhs.getDependencies() == rhs.getDependencies();
}

bool sameCreateNoOpExecutableDefinition(const Definition & existing, const Definition & requested) noexcept
{
    return sameExecutableBody(existing, requested) && existing.getDefinitionHash() == requested.getDefinitionHash();
}

void validateMutationKindAndFlags(const DefinitionMutationRequest & request)
{
    const bool is_known_kind = request.kind == MutationKind::Create || request.kind == MutationKind::ReplaceSemantic
        || request.kind == MutationKind::Rename || request.kind == MutationKind::Comment || request.kind == MutationKind::Drop;
    if (!is_known_kind)
        fail(MutationError::Code::InvalidRequest, "definition mutation kind is unknown");
    if (request.kind != MutationKind::Create && request.if_not_exists)
        fail(MutationError::Code::InvalidRequest, "IF NOT EXISTS is valid only for CREATE definition mutation");
    if (request.require_exact_type_uuid_on_noop && !request.if_not_exists)
        fail(MutationError::Code::InvalidRequest, "exact no-op UUID binding requires IF NOT EXISTS");
    if (request.kind != MutationKind::Rename && !request.rename_dependent_record_rewrites.empty())
        fail(MutationError::Code::InvalidRequest, "dependent presentation rewrites are valid only for RENAME");
}

bool sameSemanticReplacementAdministrativeMetadata(const Record & before, const Record & after) noexcept
{
    return before.owner_uuid == after.owner_uuid && before.owner_display_name == after.owner_display_name && before.comment == after.comment
        && before.creation_time_us_utc == after.creation_time_us_utc && before.storage_backend == after.storage_backend
        && before.semantic_extension_version == after.semantic_extension_version
        && before.semantic_extension_flags == after.semantic_extension_flags;
}

void validateAfterPair(
    const Definition::Ptr & definition, const std::optional<Record> & record, UUID database_uuid, const RecordLimits & limits)
{
    if (!definition || !record)
        fail(MutationError::Code::InvalidRequest, "definition mutation requires a complete after image");
    const auto & identity = definition->getIdentity();
    if (identity.database_uuid != database_uuid || record->identity.database_uuid != database_uuid)
        fail(MutationError::Code::DatabaseMismatch, "definition mutation after image belongs to another database");
    if (identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
        fail(MutationError::Code::InvalidRequest, "definition mutation after identity is invalid");
    if (!recordMatchesCheckedDefinition(*record, *definition))
        fail(MutationError::Code::DefinitionRecordMismatch, "definition mutation record does not exactly match its checked definition");
    try
    {
        static_cast<void>(encodeRecord(*record, limits));
    }
    catch (const RecordError & error)
    {
        if (error.code == RecordError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "definition mutation record exceeds its limit");
        fail(MutationError::Code::DefinitionRecordMismatch, "definition mutation record is not canonical V1");
    }
}

void validateCommentChange(
    const Definition & before_definition,
    const Record & before_record,
    const Definition & after_definition,
    const Record & after_record,
    const RecordLimits & limits)
{
    if (!before_definition.hasSameCheckedSemantics(after_definition)
        || before_definition.getNormalizedName() != after_definition.getNormalizedName()
        || before_definition.getNormalizedLocalName() != after_definition.getNormalizedLocalName())
    {
        fail(MutationError::Code::MutationKindMismatch, "comment mutation changes the checked definition or its name");
    }
    if (before_record.comment == after_record.comment)
        fail(MutationError::Code::MutationKindMismatch, "comment mutation does not change the comment");
    if (before_record.canonical_definition_sql == after_record.canonical_definition_sql)
        fail(MutationError::Code::MutationKindMismatch, "comment mutation does not synchronize its canonical ATTACH SQL");
    if (before_record.canonical_physical_template_sql != after_record.canonical_physical_template_sql)
        fail(MutationError::Code::MutationKindMismatch, "comment mutation changes the canonical physical template");
    auto normalized_after = after_record;
    normalized_after.comment = before_record.comment;
    normalized_after.canonical_definition_sql = before_record.canonical_definition_sql;
    if (normalized_after != before_record)
        fail(MutationError::Code::MutationKindMismatch, "comment mutation changes bytes outside comment and canonical ATTACH presentation");

    try
    {
        ParserCreateTypeQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            after_record.canonical_definition_sql,
            "user-defined type comment mutation canonical ATTACH record",
            limits.maximum_canonical_sql_bytes,
            canonical_attach_maximum_parser_depth,
            canonical_attach_maximum_parser_backtracks);
        const auto * query = ast->as<ASTCreateTypeQuery>();
        if (!query || !query->attach || !query->database || !query->uuid || !query->revision || !query->definition_hash
            || !query->definition || query->if_not_exists || !query->cluster.empty())
        {
            fail(
                MutationError::Code::DefinitionRecordMismatch, "comment mutation canonical SQL is not a complete local ATTACH TYPE record");
        }
        if (query->getDatabase().empty() || query->getTypeName() != after_record.normalized_local_name
            || query->getDatabase() + "." + query->getTypeName() != after_record.normalized_name)
        {
            fail(MutationError::Code::DefinitionRecordMismatch, "comment mutation canonical ATTACH name disagrees with its record");
        }
        if (*query->uuid != after_record.identity.type_uuid || *query->revision != after_record.identity.revision
            || *query->definition_hash != lowerHexDigest(after_record.definition_hash))
        {
            fail(MutationError::Code::DefinitionRecordMismatch, "comment mutation canonical ATTACH identity disagrees with its record");
        }
        const auto * comment = query->comment ? query->comment->as<ASTLiteral>() : nullptr;
        if ((comment == nullptr) != after_record.comment.empty()
            || (comment && (comment->value.getType() != Field::Types::String || comment->value.safeGet<String>() != after_record.comment)))
        {
            fail(MutationError::Code::DefinitionRecordMismatch, "comment mutation canonical ATTACH comment disagrees with its record");
        }
        if (query->definition->formatWithSecretsOneLine() != after_record.canonical_physical_template_sql)
        {
            fail(
                MutationError::Code::DefinitionRecordMismatch,
                "comment mutation canonical ATTACH body disagrees with its physical-template presentation");
        }
        if (ast->formatWithSecretsOneLine() != after_record.canonical_definition_sql)
            fail(MutationError::Code::DefinitionRecordMismatch, "comment mutation ATTACH SQL is not in canonical one-line form");
    }
    catch (const Exception &)
    {
        fail(MutationError::Code::DefinitionRecordMismatch, "comment mutation canonical ATTACH SQL cannot be parsed");
    }
}

void validateRenameChange(
    const Definition & before_definition, const Record & before_record, const Definition & after_definition, const Record & after_record)
{
    if (!before_definition.hasSameCheckedSemantics(after_definition)
        || before_definition.getDefinitionHash() != after_definition.getDefinitionHash())
    {
        fail(MutationError::Code::MutationKindMismatch, "rename mutation changes the checked executable definition");
    }
    if (before_definition.getNormalizedName() == after_definition.getNormalizedName()
        && before_definition.getNormalizedLocalName() == after_definition.getNormalizedLocalName())
    {
        fail(MutationError::Code::MutationKindMismatch, "rename mutation does not change the normalized name");
    }
    auto normalized_after = after_record;
    normalized_after.normalized_name = before_record.normalized_name;
    normalized_after.normalized_local_name = before_record.normalized_local_name;
    normalized_after.canonical_definition_sql = before_record.canonical_definition_sql;
    normalized_after.canonical_physical_template_sql = before_record.canonical_physical_template_sql;
    if (normalized_after != before_record)
        fail(MutationError::Code::MutationKindMismatch, "rename mutation changes bytes outside names and canonical presentation");
}

void validateDependentPresentationRewrite(
    const Record & before_record, const Record & after_record, const Definition & definition, const RecordLimits & limits)
{
    if (after_record.identity != before_record.identity || !recordMatchesCheckedDefinition(after_record, definition))
        fail(MutationError::Code::DefinitionRecordMismatch, "rename dependent rewrite changes its exact checked identity or semantics");
    if (after_record.canonical_definition_sql == before_record.canonical_definition_sql
        || after_record.canonical_physical_template_sql == before_record.canonical_physical_template_sql)
    {
        fail(MutationError::Code::MutationKindMismatch, "rename dependent rewrite does not synchronize both canonical SQL fields");
    }

    auto normalized_after = after_record;
    normalized_after.canonical_definition_sql = before_record.canonical_definition_sql;
    normalized_after.canonical_physical_template_sql = before_record.canonical_physical_template_sql;
    if (normalized_after != before_record)
        fail(MutationError::Code::MutationKindMismatch, "rename dependent rewrite changes bytes outside canonical SQL presentation");
    try
    {
        static_cast<void>(encodeRecord(after_record, limits));
    }
    catch (const RecordError & error)
    {
        if (error.code == RecordError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "rename dependent rewrite exceeds the definition-record limit");
        fail(MutationError::Code::DefinitionRecordMismatch, "rename dependent rewrite is not a canonical V1 record");
    }
}

void validateSemanticReplacement(
    const Definition & before_definition, const Record & before_record, const Definition & after_definition, const Record & after_record)
{
    const auto & before_identity = before_definition.getIdentity();
    const auto & after_identity = after_definition.getIdentity();
    if (before_identity.revision == std::numeric_limits<UInt64>::max() || after_identity.type_uuid != before_identity.type_uuid
        || after_identity.revision != before_identity.revision + 1)
    {
        fail(MutationError::Code::InvalidRevision, "semantic replacement must advance the same type identity by exactly one revision");
    }
    if (before_definition.getNormalizedName() != after_definition.getNormalizedName()
        || before_definition.getNormalizedLocalName() != after_definition.getNormalizedLocalName())
    {
        fail(MutationError::Code::MutationKindMismatch, "semantic replacement changes the definition name");
    }
    if (sameExecutableBody(before_definition, after_definition)
        || before_definition.getDefinitionHash() == after_definition.getDefinitionHash())
    {
        fail(MutationError::Code::MutationKindMismatch, "semantic replacement does not change the checked executable body and hash");
    }
    if (!sameSemanticReplacementAdministrativeMetadata(before_record, after_record))
        fail(MutationError::Code::MutationKindMismatch, "semantic replacement changes administrative metadata");
}

struct DefinitionSet
{
    std::vector<Definition::Ptr> definitions;
    std::vector<Record> records;
};

DefinitionSet copyDefinitionSet(const AuthorityRoot * root)
{
    DefinitionSet result;
    if (!root)
        return result;
    const auto records = root->getDefinitionRecords();
    result.definitions.reserve(records.size());
    result.records.reserve(records.size());
    for (const auto & record : records)
    {
        auto definition = root->findByIdentity(record.identity);
        if (!definition || !recordMatchesCheckedDefinition(record, *definition))
            fail(MutationError::Code::InvalidBase, "authority root contains an unmatched definition record");
        result.definitions.push_back(std::move(definition));
        result.records.push_back(record);
    }
    return result;
}

std::optional<size_t> findIdentity(const DefinitionSet & definitions, const DefinitionIdentity & identity)
{
    for (size_t index = 0; index < definitions.records.size(); ++index)
        if (definitions.records[index].identity == identity)
            return index;
    return std::nullopt;
}

bool containsTypeUUID(const DefinitionSet & definitions, UUID type_uuid)
{
    return std::any_of(
        definitions.records.begin(),
        definitions.records.end(),
        [&](const Record & record) { return record.identity.type_uuid == type_uuid; });
}

struct PresentationRewritePair
{
    Record before;
    Record after;
};

std::vector<PresentationRewritePair> applyRenameDependentPresentationRewrites(
    DefinitionSet & definitions,
    const DefinitionMutationRequest & request,
    const DefinitionIdentity & renamed_identity,
    const SchemaObjectDependencyGraph & base_graph,
    const RecordLimits & record_limits)
{
    if (request.kind != MutationKind::Rename)
        return {};

    const SchemaObjectID renamed_object = definitionObjectID(renamed_identity.database_uuid, renamed_identity.type_uuid);
    std::vector<UUID> expected_type_uuids;
    for (const auto & dependent : base_graph.getDependents(renamed_object))
    {
        if (dependent.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition || dependent.object == renamed_object)
            continue;
        if (dependent.object.kind != SchemaObjectKind::TypeDefinition || dependent.object.database_uuid != request.database_uuid)
            fail(MutationError::Code::InvalidBase, "definition dependency graph has an invalid RENAME dependent");
        expected_type_uuids.push_back(dependent.object.object_uuid);
    }
    std::sort(
        expected_type_uuids.begin(),
        expected_type_uuids.end(),
        [](const UUID & lhs, const UUID & rhs) { return compareUUID(lhs, rhs) < 0; });

    std::vector<PresentationRewritePair> rewrites;
    rewrites.reserve(request.rename_dependent_record_rewrites.size());
    std::vector<UUID> supplied_type_uuids;
    supplied_type_uuids.reserve(request.rename_dependent_record_rewrites.size());
    for (const auto & after_record : request.rename_dependent_record_rewrites)
    {
        if (after_record.identity.database_uuid != request.database_uuid || after_record.identity.type_uuid == renamed_identity.type_uuid)
            fail(MutationError::Code::DatabaseMismatch, "rename dependent rewrite has an invalid database or target identity");
        const auto index = findIdentity(definitions, after_record.identity);
        if (!index)
            fail(MutationError::Code::DefinitionNotFound, "rename dependent rewrite exact identity is absent");
        const auto & before_record = definitions.records[*index];
        const auto & definition = definitions.definitions[*index];
        validateDependentPresentationRewrite(before_record, after_record, *definition, record_limits);
        rewrites.push_back({.before = before_record, .after = after_record});
        definitions.records[*index] = after_record;
        supplied_type_uuids.push_back(after_record.identity.type_uuid);
    }

    std::sort(
        supplied_type_uuids.begin(),
        supplied_type_uuids.end(),
        [](const UUID & lhs, const UUID & rhs) { return compareUUID(lhs, rhs) < 0; });
    if (std::adjacent_find(supplied_type_uuids.begin(), supplied_type_uuids.end()) != supplied_type_uuids.end()
        || supplied_type_uuids != expected_type_uuids)
    {
        fail(MutationError::Code::InvalidRequest, "rename dependent rewrites are not the complete exact direct-dependent set");
    }

    std::sort(
        rewrites.begin(),
        rewrites.end(),
        [](const PresentationRewritePair & lhs, const PresentationRewritePair & rhs)
        { return identityLess(lhs.before.identity, rhs.before.identity); });
    return rewrites;
}

void sortDefinitionSet(DefinitionSet & definitions)
{
    std::vector<size_t> order(definitions.records.size());
    for (size_t index = 0; index < order.size(); ++index)
        order[index] = index;
    std::sort(
        order.begin(),
        order.end(),
        [&](size_t lhs, size_t rhs) { return identityLess(definitions.records[lhs].identity, definitions.records[rhs].identity); });

    DefinitionSet canonical;
    canonical.definitions.reserve(order.size());
    canonical.records.reserve(order.size());
    for (const size_t index : order)
    {
        canonical.definitions.push_back(std::move(definitions.definitions[index]));
        canonical.records.push_back(std::move(definitions.records[index]));
    }
    definitions = std::move(canonical);
}

void validateDependencyClosure(const DefinitionSet & definitions)
{
    for (const auto & definition : definitions.definitions)
    {
        for (const auto & dependency : definition->getDependencies())
        {
            const auto target = std::lower_bound(
                definitions.definitions.begin(),
                definitions.definitions.end(),
                dependency.type_uuid,
                [](const Definition::Ptr & candidate, const UUID & type_uuid)
                { return compareUUID(candidate->getIdentity().type_uuid, type_uuid) < 0; });
            if (target == definitions.definitions.end() || (*target)->getIdentity().type_uuid != dependency.type_uuid
                || (*target)->getIdentity().revision != dependency.revision
                || (*target)->getDefinitionHash() != dependency.target_definition_hash)
            {
                fail(MutationError::Code::InvalidTransition, "definition mutation leaves an unresolved exact dependency identity");
            }
        }
    }
}

void validateAtomicPublicationLimits(const DefinitionSet & definitions, const DefinitionMutationPlannerLimits & limits)
{
    for (const auto & definition : definitions.definitions)
    {
        if (!tryCountLogicalRetainedDefinitionBytes(*definition, limits.maximum_definition_retained_bytes))
            fail(MutationError::Code::LimitExceeded, "definition mutation exceeds the Atomic retained-definition byte limit");
    }
}

std::vector<SchemaObjectDependencyEdge> definitionEdges(const Definition & definition)
{
    std::vector<SchemaObjectDependencyEdge> result;
    result.reserve(definition.getDependencies().size());
    const auto & identity = definition.getIdentity();
    const SchemaObjectID dependent = definitionObjectID(identity.database_uuid, identity.type_uuid);
    for (const auto & dependency : definition.getDependencies())
    {
        result.push_back({
            .dependent = dependent,
            .dependency = definitionObjectID(identity.database_uuid, dependency.type_uuid),
            .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition,
        });
    }
    std::sort(result.begin(), result.end());
    return result;
}

SchemaObjectDependencyGraphMutation makeGraphDelta(
    MutationKind kind,
    const Definition * before_definition,
    const Definition * after_definition,
    const SchemaObjectDependencyGraph & base_graph)
{
    SchemaObjectDependencyGraphMutation result;
    if (kind == MutationKind::Comment || kind == MutationKind::Rename)
        return result;

    if (before_definition && (kind == MutationKind::Drop || kind == MutationKind::ReplaceSemantic))
    {
        const auto & identity = before_definition->getIdentity();
        const SchemaObjectID object = definitionObjectID(identity.database_uuid, identity.type_uuid);
        const auto dependents = base_graph.getDependents(object);
        const bool has_external_dependent = std::any_of(
            dependents.begin(),
            dependents.end(),
            [&](const SchemaObjectDependencyNeighbor & dependent)
            { return dependent.object != object || dependent.kind != SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition; });
        if (has_external_dependent)
            fail(MutationError::Code::ReferencedDefinition, "definition mutation is restricted by a graph dependent");
    }

    if (kind == MutationKind::Create)
    {
        const auto & identity = after_definition->getIdentity();
        result.node_additions.push_back(definitionObjectID(identity.database_uuid, identity.type_uuid));
        result.edge_additions = definitionEdges(*after_definition);
        return result;
    }

    if (kind == MutationKind::Drop)
    {
        const auto & identity = before_definition->getIdentity();
        result.node_removals.push_back(definitionObjectID(identity.database_uuid, identity.type_uuid));
        result.edge_removals = definitionEdges(*before_definition);
        return result;
    }

    const auto before_edges = definitionEdges(*before_definition);
    const auto after_edges = definitionEdges(*after_definition);
    std::set_difference(
        before_edges.begin(), before_edges.end(), after_edges.begin(), after_edges.end(), std::back_inserter(result.edge_removals));
    std::set_difference(
        after_edges.begin(), after_edges.end(), before_edges.begin(), before_edges.end(), std::back_inserter(result.edge_additions));
    return result;
}

AuthorityInventory::Ptr buildProspectiveInventory(
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    const DatabaseSchemaWALLimits & limits)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    if (expectation_records.size() > std::numeric_limits<size_t>::max() - definition_records.size())
        fail(MutationError::Code::LimitExceeded, "definition mutation authority record count overflows size_t");
    leaves.reserve(definition_records.size() + expectation_records.size());
    for (const auto & record : definition_records)
    {
        leaves.push_back({
            .key = definitionInventoryKey(record.identity.type_uuid),
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record, limits.definition_record),
        });
    }
    for (const auto & record : expectation_records)
    {
        leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = record.object.object_uuid,
            },
            .object_revision = record.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(record),
        });
    }
    std::sort(leaves.begin(), leaves.end(), authorityInventoryLeafLess);
    try
    {
        const auto summary = buildAuthorityInventorySummary(leaves, limits.inventory_snapshot.inventory);
        return AuthorityInventory::create(summary, std::move(leaves), limits.inventory_snapshot.inventory);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "definition mutation inventory exceeds its limit");
        fail(MutationError::Code::InvalidTransition, "definition mutation inventory is invalid");
    }
}

AuthorityInventory::Ptr buildEmptyInventory(const DatabaseSchemaWALLimits & limits)
{
    return buildProspectiveInventory({}, {}, limits);
}

DatabaseSchemaWALStagedArtifact
definitionArtifact(DatabaseSchemaWALStagedArtifactImage image, const Record & record, const RecordLimits & limits)
{
    return {
        .kind = DatabaseSchemaWALStagedArtifactKind::TypeDefinitionRecord,
        .image = image,
        .object = definitionObjectID(record.identity.database_uuid, record.identity.type_uuid),
        .revision = record.identity.revision,
        .canonical_bytes = encodeRecord(record, limits),
    };
}

}

DefinitionMutationPlannerError::DefinitionMutationPlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

DefinitionMutationPlannerLimits::DefinitionMutationPlannerLimits()
{
    const auto atomic_limits = atomicDatabaseAuthorityCapabilities().limits;
    authority_root.type_catalog.maximum_definitions = atomic_limits.maximum_definitions;
    authority_root.maximum_definition_records = atomic_limits.maximum_definitions;
    maximum_definition_retained_bytes = atomic_limits.maximum_definition_bytes;
}

PreparedDefinitionMutation::PreparedDefinitionMutation(
    AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_)
    : replacement_root(std::move(replacement_root_))
    , transition(std::move(transition_))
{
}

const AuthorityRoot & PreparedDefinitionMutation::getReplacementRoot() const
{
    if (!replacement_root)
        fail(MutationError::Code::InvalidRequest, "prepared definition mutation has no replacement root");
    return *replacement_root;
}

const DatabaseSchemaWALValidatedTransition & PreparedDefinitionMutation::getValidatedTransition() const
{
    if (!transition)
        fail(MutationError::Code::InvalidRequest, "prepared definition mutation has no validated WAL transition");
    return *transition;
}

PreparedDefinitionMutation DefinitionMutationPlanner::plan(
    const AuthorityRoot * current_root, DefinitionMutationRequest request, const DefinitionMutationPlannerLimits & limits)
{
    if (current_root && limits.initial_effective_database_limits)
        fail(MutationError::Code::InvalidRequest, "a successor definition mutation cannot replace database resource limits");
    if (!current_root && !limits.initial_effective_database_limits)
        fail(MutationError::Code::InvalidRequest, "the first definition mutation has no reconciled database resource limits");

    validateMutationKindAndFlags(request);
    if (request.database_uuid == UUIDHelpers::Nil)
        fail(MutationError::Code::InvalidRequest, "definition mutation database UUID must be nonzero");
    if (current_root && current_root->getDatabaseUUID() != request.database_uuid)
        fail(MutationError::Code::DatabaseMismatch, "definition mutation base root belongs to another database");

    const UInt64 actual_epoch = current_root ? current_root->getDatabaseCatalogEpoch() : 0;
    if (request.expected_database_catalog_epoch && *request.expected_database_catalog_epoch != actual_epoch)
        fail(MutationError::Code::ExpectedEpochMismatch, "definition mutation expected database epoch is stale");
    if (!current_root && request.kind != MutationKind::Create)
        fail(MutationError::Code::InvalidRequest, "only CREATE may perform first authority activation");

    const bool has_after = request.kind != MutationKind::Drop;
    const bool has_before = request.kind != MutationKind::Create;
    if (has_after)
        validateAfterPair(request.after_definition, request.after_record, request.database_uuid, limits.schema_wal.definition_record);
    else if (request.after_definition || request.after_record)
        fail(MutationError::Code::InvalidRequest, "DROP definition mutation contains an after image");
    if (has_before != request.expected_before_identity.has_value())
        fail(MutationError::Code::InvalidRequest, "definition mutation before identity presence disagrees with its kind");
    if (request.expected_before_identity && request.expected_before_identity->database_uuid != request.database_uuid)
        fail(MutationError::Code::DatabaseMismatch, "definition mutation before identity belongs to another database");

    DefinitionSet definitions = copyDefinitionSet(current_root);
    const Definition * before_definition = nullptr;
    std::optional<Record> before_record_snapshot;
    const Record * before_record = nullptr;
    std::optional<size_t> before_index;
    if (request.expected_before_identity)
    {
        before_index = findIdentity(definitions, *request.expected_before_identity);
        if (!before_index)
            fail(MutationError::Code::DefinitionNotFound, "definition mutation exact before identity is absent");
        before_definition = definitions.definitions[*before_index].get();
        before_record_snapshot = definitions.records[*before_index];
        before_record = &*before_record_snapshot;
    }

    if (request.kind == MutationKind::Create)
    {
        const auto & after_identity = request.after_definition->getIdentity();
        if (current_root)
        {
            const auto name_collision = current_root->findByName(request.after_definition->getNormalizedLocalName());
            if (name_collision)
            {
                if (!request.if_not_exists)
                    fail(MutationError::Code::DuplicateName, "CREATE definition mutation collides with a normalized local name");
                if (!sameCreateNoOpExecutableDefinition(*name_collision, *request.after_definition)
                    || (request.require_exact_type_uuid_on_noop && name_collision->getIdentity().type_uuid != after_identity.type_uuid))
                {
                    fail(
                        MutationError::Code::DefinitionConflict,
                        "CREATE IF NOT EXISTS collision is not the requested exact executable definition");
                }
                return PreparedDefinitionMutation();
            }
        }
        if (after_identity.revision != 1)
            fail(MutationError::Code::InvalidRevision, "material CREATE definition mutation must start at revision one");
        if (containsTypeUUID(definitions, after_identity.type_uuid))
            fail(MutationError::Code::DuplicateTypeUUID, "CREATE definition mutation collides with a stable type UUID");
    }
    else
    {
        const auto & expected = *request.expected_before_identity;
        if (before_definition->getIdentity() != expected || before_record->identity != expected)
            fail(MutationError::Code::InvalidBase, "definition mutation before image is internally inconsistent");
        if (has_after)
        {
            const auto & after_identity = request.after_definition->getIdentity();
            if (after_identity.type_uuid != expected.type_uuid)
                fail(MutationError::Code::DuplicateTypeUUID, "definition mutation changes the stable type UUID");
            const auto name_collision = current_root->findByName(request.after_definition->getNormalizedLocalName());
            if (name_collision && name_collision->getIdentity().type_uuid != expected.type_uuid)
                fail(MutationError::Code::DuplicateName, "definition mutation collides with another normalized local name");
        }
    }

    if (request.transaction_id == 0)
        fail(MutationError::Code::InvalidRequest, "durable definition mutation transaction ID must be nonzero");
    if (current_root && actual_epoch == std::numeric_limits<UInt64>::max())
        fail(MutationError::Code::LimitExceeded, "definition mutation database epoch cannot advance");

    UInt64 next_generation = 1;
    if (current_root)
    {
        next_generation = current_root->getTypeIndexGeneration();
        if (request.kind != MutationKind::Comment)
        {
            if (next_generation == std::numeric_limits<UInt64>::max())
                fail(MutationError::Code::LimitExceeded, "definition mutation type-index generation cannot advance");
            ++next_generation;
        }
    }

    switch (request.kind)
    {
        case MutationKind::Create: break;
        case MutationKind::ReplaceSemantic:
            validateSemanticReplacement(*before_definition, *before_record, *request.after_definition, *request.after_record);
            break;
        case MutationKind::Rename:
            validateRenameChange(*before_definition, *before_record, *request.after_definition, *request.after_record);
            break;
        case MutationKind::Comment:
            validateCommentChange(
                *before_definition, *before_record, *request.after_definition, *request.after_record, limits.schema_wal.definition_record);
            break;
        case MutationKind::Drop: break;
    }

    AuthorityInventory::Ptr base_inventory;
    SchemaObjectDependencyGraph::Ptr base_graph;
    std::vector<SidecarExpectationRecord> expectations;
    std::optional<AuthorityState> before_authority_state;
    if (current_root)
    {
        base_inventory = current_root->pinAuthorityInventory();
        base_graph = current_root->pinSchemaObjectDependencyGraph();
        expectations.assign(current_root->getExpectationRecords().begin(), current_root->getExpectationRecords().end());
        before_authority_state = current_root->getAuthorityState();
    }
    else
    {
        base_inventory = buildEmptyInventory(limits.schema_wal);
        try
        {
            base_graph = SchemaObjectDependencyGraph::createEmpty(request.database_uuid, limits.schema_wal.schema_graph);
        }
        catch (const SchemaObjectDependencyGraphError & error)
        {
            if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
                fail(MutationError::Code::LimitExceeded, "definition mutation empty graph exceeds its limit");
            fail(MutationError::Code::InvalidRequest, "definition mutation graph limits are invalid");
        }
    }

    const auto graph_delta = makeGraphDelta(request.kind, before_definition, request.after_definition.get(), *base_graph);
    const auto dependent_presentation_rewrites = request.kind == MutationKind::Rename
        ? applyRenameDependentPresentationRewrites(
              definitions, request, *request.expected_before_identity, *base_graph, limits.schema_wal.definition_record)
        : std::vector<PresentationRewritePair>{};

    if (request.kind == MutationKind::Create)
    {
        definitions.definitions.push_back(request.after_definition);
        definitions.records.push_back(*request.after_record);
    }
    else if (request.kind == MutationKind::Drop)
    {
        definitions.definitions.erase(definitions.definitions.begin() + static_cast<std::ptrdiff_t>(*before_index));
        definitions.records.erase(definitions.records.begin() + static_cast<std::ptrdiff_t>(*before_index));
    }
    else
    {
        definitions.definitions[*before_index] = request.after_definition;
        definitions.records[*before_index] = *request.after_record;
    }
    sortDefinitionSet(definitions);
    validateDependencyClosure(definitions);
    validateAtomicPublicationLimits(definitions, limits);

    SchemaObjectDependencyGraph::Ptr after_graph;
    try
    {
        after_graph = SchemaObjectDependencyGraph::applyMutation(base_graph, graph_delta);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "definition mutation graph exceeds its limit");
        fail(MutationError::Code::InvalidTransition, "definition mutation graph delta is invalid");
    }

    AuthorityInventory::Ptr after_inventory;
    try
    {
        after_inventory = buildProspectiveInventory(definitions.records, expectations, limits.schema_wal);
    }
    catch (const RecordError & error)
    {
        if (error.code == RecordError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "definition mutation record exceeds its limit");
        fail(MutationError::Code::DefinitionRecordMismatch, "definition mutation record is not canonical V1");
    }

    const UInt64 next_epoch = current_root ? actual_epoch + 1 : 1;
    const UInt64 persistent_capabilities
        = current_root ? current_root->getPersistentCapabilityMask() : definition_authority_capability_mask;
    AuthorityState after_state;
    try
    {
        const auto & summary = after_inventory->getSummary();
        after_state = makeAuthorityState(
            request.database_uuid,
            next_epoch,
            persistent_capabilities,
            summary.leaf_count,
            summary.merkle_radix_root,
            after_graph->computeRoot(),
            limits.schema_wal.authority_state);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, "definition mutation authority state exceeds its limit");
        fail(MutationError::Code::InvalidTransition, "definition mutation authority state is invalid");
    }

    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas;
    authority_deltas.push_back({
        .key
        = definitionInventoryKey(before_record ? before_record->identity.type_uuid : request.after_definition->getIdentity().type_uuid),
        .before = before_record ? std::optional(recordState(*before_record, limits.schema_wal.definition_record)) : std::nullopt,
        .after
        = request.after_record ? std::optional(recordState(*request.after_record, limits.schema_wal.definition_record)) : std::nullopt,
    });
    for (const auto & rewrite : dependent_presentation_rewrites)
    {
        authority_deltas.push_back({
            .key = definitionInventoryKey(rewrite.before.identity.type_uuid),
            .before = recordState(rewrite.before, limits.schema_wal.definition_record),
            .after = recordState(rewrite.after, limits.schema_wal.definition_record),
        });
    }
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts;
    if (before_record)
        staged_artifacts.push_back(
            definitionArtifact(DatabaseSchemaWALStagedArtifactImage::Before, *before_record, limits.schema_wal.definition_record));
    if (request.after_record)
        staged_artifacts.push_back(
            definitionArtifact(DatabaseSchemaWALStagedArtifactImage::After, *request.after_record, limits.schema_wal.definition_record));
    for (const auto & rewrite : dependent_presentation_rewrites)
    {
        staged_artifacts.push_back(
            definitionArtifact(DatabaseSchemaWALStagedArtifactImage::Before, rewrite.before, limits.schema_wal.definition_record));
        staged_artifacts.push_back(
            definitionArtifact(DatabaseSchemaWALStagedArtifactImage::After, rewrite.after, limits.schema_wal.definition_record));
    }

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                request.transaction_id,
                {
                    .authority_state = before_authority_state,
                    .authority_inventory = base_inventory,
                    .schema_graph = base_graph,
                },
                after_state,
                std::move(authority_deltas),
                {},
                graph_delta,
                std::move(staged_artifacts),
                limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(MutationError::Code::LimitExceeded, "definition mutation WAL transition exceeds its limit");
            fail(MutationError::Code::InvalidTransition, "definition mutation WAL transition is invalid");
        }
    }();

    AuthorityRoot::Ptr replacement_root;
    try
    {
        if (current_root)
        {
            replacement_root = AuthorityRootBuilder::buildReplacement(
                *current_root,
                transition.getPrepare().after_authority_state,
                next_generation,
                definitions.definitions,
                definitions.records,
                expectations,
                transition.pinAfterGraph(),
                limits.authority_root);
        }
        else
        {
            replacement_root = AuthorityRootBuilder::buildInitialAdmission(
                transition.getPrepare().after_authority_state,
                next_generation,
                definitions.definitions,
                definitions.records,
                expectations,
                transition.pinAfterGraph(),
                *limits.initial_effective_database_limits,
                limits.authority_root);
        }
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(MutationError::Code::LimitExceeded, error.what());
        fail(MutationError::Code::InvalidTransition, "definition mutation replacement root is invalid");
    }

    if (current_root && request.kind == MutationKind::Comment
        && (replacement_root->getTypeIndexGeneration() != current_root->getTypeIndexGeneration()
            || replacement_root->getTypeIndexContentDigest() != current_root->getTypeIndexContentDigest()))
    {
        fail(MutationError::Code::InvalidTransition, "comment mutation changed the in-memory type index");
    }
    if (replacement_root->getExpectationRecords().size() != expectations.size()
        || !std::equal(
            replacement_root->getExpectationRecords().begin(), replacement_root->getExpectationRecords().end(), expectations.begin()))
    {
        fail(MutationError::Code::InvalidTransition, "definition mutation changed dependent-object-capable expectation records");
    }

    return PreparedDefinitionMutation(std::move(replacement_root), std::move(transition));
}

}
