#include <Databases/UDT/AuthorityRecovery.h>

#include <Databases/UDT/AtomicAuthority.h>

#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/ResourceLimitAdapters.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int CANNOT_SCHEDULE_TASK;
extern const int DEADLOCK_AVOIDED;
extern const int LIMIT_EXCEEDED;
extern const int MEMORY_LIMIT_EXCEEDED;
extern const int QUERY_WAS_CANCELLED;
extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
extern const int TIMEOUT_EXCEEDED;
extern const int TOO_MANY_BYTES;
}

namespace DB::FailPoints
{
extern const char udt_authority_recovery_parse_failure[];
}

namespace DB::UDT
{
namespace
{

using RecoveryError = AuthorityRecoveryError;

constexpr UInt64 maximum_inventory_snapshot_bytes = 32ULL << 20;
constexpr UInt64 maximum_inventory_leaf_bytes = 128;
constexpr UInt64 maximum_authority_state_bytes = 256;
constexpr UInt64 maximum_schema_graph_retained_bytes = resource_implementation_maximum_deterministic_catalog_bytes;
constexpr UInt64 maximum_checker_definition_input_bytes = 1ULL << 20;
constexpr UInt64 maximum_checker_catalog_bytes = resource_implementation_maximum_deterministic_catalog_bytes;

static_assert(atomicDatabaseAuthorityCapabilities().limits.maximum_definitions == authority_recovery_maximum_definitions);
static_assert(atomicDatabaseAuthorityCapabilities().limits.maximum_template_nodes == 4'096);
static_assert(atomicDatabaseAuthorityCapabilities().limits.maximum_direct_dependencies == 256);
static_assert(atomicDatabaseAuthorityCapabilities().limits.maximum_checker_work == 65'536);

[[noreturn]] void fail(RecoveryError::Code code, std::string_view message, std::optional<AuthorityInventoryKey> record_key = std::nullopt)
{
    throw RecoveryError(code, message, std::move(record_key));
}

UInt64 checkedSize(std::size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(RecoveryError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedSize(std::size_t value, std::string_view message, const AuthorityInventoryKey & record_key)
{
    if (!std::in_range<UInt64>(value))
        fail(RecoveryError::Code::LimitExceeded, message, record_key);
    return static_cast<UInt64>(value);
}

void addProspectively(UInt64 & total, UInt64 amount, UInt64 maximum, std::string_view message, const AuthorityInventoryKey & record_key)
{
    if (amount > maximum || total > maximum - amount)
        fail(RecoveryError::Code::LimitExceeded, message, record_key);
    total += amount;
}

/// One local pool is reused by all recovery waves. Each wave schedules at
/// most `parallel_workers` jobs, and callers retain at most
/// `maximum_parallel_batch_records` result slots. Worker callbacks capture
/// every record-local error into its canonical slot; the caller then reports
/// the lowest canonical failing index, independent of scheduling order.
class BoundedRecoveryExecutor final
{
public:
    explicit BoundedRecoveryExecutor(UInt64 parallel_workers_)
        : parallel_workers(static_cast<std::size_t>(parallel_workers_))
    {
        if (parallel_workers > 1)
        {
            pool = std::make_unique<ThreadPool>(CurrentMetrics::end(), CurrentMetrics::end(), CurrentMetrics::end(), parallel_workers);
        }
    }

    template <typename Callback>
    void runBatch(std::size_t begin, std::size_t end, Callback && callback)
    {
        if (begin >= end)
            return;
        if (!pool || end - begin == 1)
        {
            for (std::size_t index = begin; index < end; ++index)
                callback(index);
            return;
        }

        std::atomic<std::size_t> next{begin};
        const std::size_t workers = std::min(parallel_workers, end - begin);
        try
        {
            for (std::size_t worker = 0; worker < workers; ++worker)
            {
                pool->scheduleOrThrowOnError(
                    [&]
                    {
                        for (;;)
                        {
                            const std::size_t index = next.fetch_add(1, std::memory_order_relaxed);
                            if (index >= end)
                                return;
                            callback(index);
                        }
                    });
            }
            pool->wait();
        }
        catch (...)
        {
            const auto original = std::current_exception();
            try
            {
                pool->wait();
            }
            catch (...)
            {
            }
            std::rethrow_exception(original);
        }
    }

private:
    const std::size_t parallel_workers;
    std::unique_ptr<ThreadPool> pool;
};

bool isLimitError(const Exception & error) noexcept
{
    return error.code() == ErrorCodes::LIMIT_EXCEEDED || error.code() == ErrorCodes::TOO_MANY_BYTES;
}

/// Durable mismatch diagnostics are authoritative only for deterministic
/// parser/lowering/checker failures. Host resource pressure and query/process
/// control must retain their original retry/cancellation taxonomy.
bool mustRethrowRecoveryResourceOrControlException(const Exception & error) noexcept
{
    const int code = error.code();
    return code == ErrorCodes::ABORTED || code == ErrorCodes::CANNOT_ALLOCATE_MEMORY || code == ErrorCodes::CANNOT_SCHEDULE_TASK
        || code == ErrorCodes::DEADLOCK_AVOIDED || code == ErrorCodes::MEMORY_LIMIT_EXCEEDED || code == ErrorCodes::QUERY_WAS_CANCELLED
        || code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT || code == ErrorCodes::TIMEOUT_EXCEEDED;
}

void validateRecoveryLimits(const AuthorityRecoveryLimits & limits)
{
    if (limits.maximum_record_images == 0 || limits.maximum_total_record_bytes == 0 || limits.maximum_parser_depth == 0
        || limits.maximum_parser_backtracks == 0 || limits.parallel_workers == 0 || limits.maximum_parallel_batch_records == 0
        || limits.root.maximum_definition_records == 0 || limits.root.maximum_expectation_records == 0
        || limits.root.maximum_canonical_record_bytes == 0 || limits.root.type_catalog.maximum_definitions == 0
        || limits.root.inventory.maximum_leaves == 0 || limits.root.inventory.maximum_leaf_bytes == 0
        || limits.root.authority_state.maximum_leaves == 0 || limits.root.authority_state.maximum_encoded_bytes == 0
        || limits.inventory_snapshot.inventory.maximum_leaves == 0 || limits.inventory_snapshot.inventory.maximum_leaf_bytes == 0
        || limits.inventory_snapshot.maximum_snapshot_bytes == 0 || limits.schema_graph.maximum_retained_bytes == 0
        || limits.lowering.maximum_definitions == 0 || limits.lowering.maximum_catalog_string_bytes == 0
        || limits.lowering.maximum_total_string_bytes == 0 || limits.checker.maximum_definitions == 0
        || limits.checker.maximum_definition_input_bytes == 0 || limits.checker.maximum_catalog_input_bytes == 0
        || limits.checker.maximum_canonical_catalog_bytes == 0 || limits.checker.maximum_catalog_nodes == 0
        || limits.checker.maximum_catalog_edges == 0 || limits.checker.maximum_catalog_checker_work == 0
        || limits.checker.maximum_scratch_bytes == 0)
        fail(RecoveryError::Code::InvalidConfiguration, "user-defined type authority recovery limit is zero");

    if (limits.maximum_record_images > authority_recovery_maximum_record_images
        || limits.maximum_total_record_bytes > authority_recovery_maximum_total_record_bytes
        || limits.maximum_parser_depth > authority_recovery_maximum_parser_depth
        || limits.maximum_parser_backtracks > authority_recovery_maximum_parser_backtracks
        || limits.parallel_workers > authority_recovery_maximum_parallel_workers
        || limits.maximum_parallel_batch_records > authority_recovery_maximum_parallel_batch_records
        || limits.maximum_parser_depth > std::numeric_limits<std::uint32_t>::max()
        || limits.maximum_parser_backtracks > std::numeric_limits<std::uint32_t>::max())
        fail(RecoveryError::Code::InvalidConfiguration, "a recovery limit exceeds the frozen implementation maximum");

    if (limits.root.maximum_definition_records > authority_recovery_maximum_definitions
        || limits.root.type_catalog.maximum_definitions > authority_recovery_maximum_definitions
        || limits.lowering.maximum_definitions > authority_recovery_maximum_definitions
        || limits.checker.maximum_definitions > authority_recovery_maximum_definitions
        || limits.root.maximum_expectation_records > authority_recovery_maximum_expectations
        || limits.root.maximum_canonical_record_bytes > authority_recovery_maximum_total_record_bytes
        || limits.root.inventory.maximum_leaves > authority_recovery_maximum_record_images
        || limits.root.authority_state.maximum_leaves > authority_recovery_maximum_record_images
        || limits.inventory_snapshot.inventory.maximum_leaves > authority_recovery_maximum_record_images)
        fail(RecoveryError::Code::InvalidConfiguration, "an authority count limit exceeds the supported recovery profile");

    if (limits.root.inventory.maximum_leaf_bytes > maximum_inventory_leaf_bytes
        || limits.inventory_snapshot.inventory.maximum_leaf_bytes > maximum_inventory_leaf_bytes
        || limits.root.authority_state.maximum_encoded_bytes > maximum_authority_state_bytes
        || limits.inventory_snapshot.maximum_snapshot_bytes > maximum_inventory_snapshot_bytes
        || limits.schema_graph.maximum_retained_bytes > maximum_schema_graph_retained_bytes
        || limits.lowering.maximum_catalog_string_bytes > maximum_checker_catalog_bytes
        || limits.lowering.maximum_total_string_bytes > maximum_checker_definition_input_bytes
        || limits.checker.maximum_definition_input_bytes > maximum_checker_definition_input_bytes
        || limits.checker.maximum_catalog_input_bytes > maximum_checker_catalog_bytes
        || limits.checker.maximum_canonical_catalog_bytes > maximum_checker_catalog_bytes
        || limits.checker.maximum_catalog_nodes > template_checker_implementation_maximum_catalog_nodes
        || limits.checker.maximum_catalog_edges > template_checker_implementation_maximum_catalog_edges
        || limits.checker.maximum_catalog_checker_work > template_checker_implementation_maximum_catalog_work
        || limits.checker.maximum_scratch_bytes > template_checker_implementation_maximum_scratch_bytes)
        fail(RecoveryError::Code::InvalidConfiguration, "an authority byte or work limit exceeds the supported recovery profile");
}

void validateAuthorityState(const AuthorityState & authority_state, const AuthorityStateLimits & limits)
{
    try
    {
        static_cast<void>(encodeAuthorityState(authority_state, limits));
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RecoveryError::Code::LimitExceeded, "authority state exceeds its recovery limit");
        fail(RecoveryError::Code::AuthorityStateMismatch, "authority state is not a canonical anchored V1 state");
    }
}

AuthorityInventoryKey definitionRecordKey(const Record & record)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = record.identity.type_uuid,
    };
}

StructuredDefinitionName structuredNameFromRecord(const Record & record, const AuthorityInventoryKey & record_key)
{
    if (record.normalized_local_name.empty() || record.normalized_name.size() <= record.normalized_local_name.size())
        fail(RecoveryError::Code::RecordMismatch, "definition record has no structured database-qualified name", record_key);

    const std::size_t database_size = record.normalized_name.size() - record.normalized_local_name.size() - 1;
    if (record.normalized_name[database_size] != '.'
        || std::string_view(record.normalized_name).substr(database_size + 1) != record.normalized_local_name)
        fail(RecoveryError::Code::RecordMismatch, "definition record qualified and local names disagree", record_key);

    return {
        .normalized_database_name = record.normalized_name.substr(0, database_size),
        .normalized_qualified_name = record.normalized_name,
        .normalized_local_name = record.normalized_local_name,
    };
}

String lowerHexDigest(const Digest & digest)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result;
    result.resize(digest.size() * 2);
    for (std::size_t index = 0; index < digest.size(); ++index)
    {
        const UInt8 byte = digest[index];
        result[2 * index] = digits[byte >> 4];
        result[2 * index + 1] = digits[byte & 0x0f];
    }
    return result;
}

ASTPtr parseCanonicalAttach(const Record & record, const AuthorityInventoryKey & record_key, const AuthorityRecoveryLimits & limits)
{
    fiu_do_on(DB::FailPoints::udt_authority_recovery_parse_failure, {
        fail(RecoveryError::Code::CanonicalSQLMismatch, "fault injected before authority-record parse", record_key);
    });
    try
    {
        ParserCreateTypeQuery parser;
        ASTPtr ast = parseQuery(
            parser,
            record.canonical_definition_sql,
            "user-defined type authority record",
            limits.root.definition_record.maximum_canonical_sql_bytes,
            limits.maximum_parser_depth,
            limits.maximum_parser_backtracks);
        const auto * query = ast->as<ASTCreateTypeQuery>();
        if (!query || !query->attach || !query->database || !query->uuid || !query->revision || !query->definition_hash)
            fail(
                RecoveryError::Code::CanonicalSQLMismatch,
                "definition record does not contain a complete database-qualified canonical ATTACH TYPE query",
                record_key);
        if (query->if_not_exists || !query->cluster.empty())
            fail(RecoveryError::Code::CanonicalSQLMismatch, "canonical ATTACH TYPE record contains an execution-only clause", record_key);
        if (*query->uuid != record.identity.type_uuid || *query->revision != record.identity.revision
            || *query->definition_hash != lowerHexDigest(record.definition_hash))
            fail(
                RecoveryError::Code::CanonicalSQLMismatch,
                "canonical ATTACH TYPE identity fields disagree with the definition record",
                record_key);
        const auto * comment = query->comment ? query->comment->as<ASTLiteral>() : nullptr;
        if ((comment == nullptr) != record.comment.empty()
            || (comment && (comment->value.getType() != Field::Types::String || comment->value.safeGet<String>() != record.comment)))
            fail(
                RecoveryError::Code::CanonicalSQLMismatch,
                "canonical ATTACH TYPE comment disagrees with the definition record",
                record_key);
        if (!query->definition || query->definition->formatWithSecretsOneLine() != record.canonical_physical_template_sql)
            fail(
                RecoveryError::Code::CanonicalSQLMismatch,
                "canonical physical template SQL disagrees with the ATTACH TYPE body",
                record_key);
        if (ast->formatWithSecretsOneLine() != record.canonical_definition_sql)
            fail(RecoveryError::Code::CanonicalSQLMismatch, "definition record SQL is not in canonical one-line form", record_key);
        return ast;
    }
    catch (const Exception & error)
    {
        if (mustRethrowRecoveryResourceOrControlException(error))
            throw;
        fail(RecoveryError::Code::CanonicalSQLMismatch, "canonical ATTACH TYPE SQL cannot be parsed", record_key);
    }
}

UInt64 countDefinitionInputBytes(const DefinitionInput & input, UInt64 maximum, const AuthorityInventoryKey & record_key)
{
    UInt64 total = 0;
    const auto add = [&](UInt64 amount)
    { addProspectively(total, amount, maximum, "definition accepted-input bytes exceed their limit", record_key); };
    const auto add_string = [&](std::string_view value)
    {
        add(sizeof(UInt64));
        add(checkedSize(value.size(), "definition accepted-input string size does not fit UInt64", record_key));
    };

    add(2 * sizeof(UUID) + sizeof(UInt64));
    add_string(input.normalized_name);
    add_string(
        input.normalized_local_name.empty() ? std::string_view(input.normalized_name) : std::string_view(input.normalized_local_name));
    add(sizeof(UInt64));
    for (const auto & parameter : input.parameters)
    {
        add(sizeof(UInt8));
        add_string(parameter.normalized_name);
    }
    add(sizeof(UInt8) + sizeof(UInt16));
    add(sizeof(UInt64) + sizeof(TemplateNodeID));
    for (const auto & node : input.nodes)
    {
        add(5 * sizeof(UInt8) + 2 * sizeof(UInt16) + 2 * sizeof(UInt64) + sizeof(Int64));
        add_string(node.atom);
        add_string(node.text);
        add_string(node.field_value.payload);
        add_string(node.field_value.name);
        add(sizeof(UInt64));
        for (const auto & entry : node.enum_entries)
        {
            add_string(entry.name);
            add(sizeof(Int64));
        }
        add(sizeof(UInt64));
        for (const auto & child : node.children)
        {
            add(sizeof(TemplateNodeID));
            add_string(child.label);
        }
    }
    add(2 * sizeof(UInt8) + 4 * sizeof(UInt16) + sizeof(Digest));
    add(sizeof(UInt64));
    for (std::size_t index = 0; index < input.dependencies.size(); ++index)
        add(sizeof(UUID) + sizeof(UInt64) + sizeof(Digest));
    return total;
}

class RetainedDefinitionInputBudget final
{
public:
    explicit RetainedDefinitionInputBudget(const TemplateCheckerLimits & limits_)
        : limits(limits_)
    {
    }

    void charge(const DefinitionInput & input, const AuthorityInventoryKey & record_key)
    {
        const UInt64 input_bytes = countDefinitionInputBytes(input, limits.maximum_definition_input_bytes, record_key);
        addProspectively(
            accepted_input_bytes,
            input_bytes,
            limits.maximum_catalog_input_bytes,
            "catalog accepted-input bytes exceed their limit",
            record_key);

        const UInt64 parameter_count = checkedSize(input.parameters.size(), "formal count does not fit UInt64", record_key);
        if (parameter_count > limits.maximum_formals)
            fail(RecoveryError::Code::LimitExceeded, "formal count exceeds its limit", record_key);
        for (const auto & parameter : input.parameters)
        {
            if (checkedSize(parameter.normalized_name.size(), "formal name size does not fit UInt64", record_key)
                > limits.maximum_formal_name_bytes)
                fail(RecoveryError::Code::LimitExceeded, "formal name bytes exceed their limit", record_key);
        }

        const UInt64 node_count = checkedSize(input.nodes.size(), "template node count does not fit UInt64", record_key);
        if (node_count > limits.maximum_template_nodes)
            fail(RecoveryError::Code::LimitExceeded, "template node count exceeds its limit", record_key);
        addProspectively(catalog_nodes, node_count, limits.maximum_catalog_nodes, "catalog template nodes exceed their limit", record_key);

        const UInt64 dependency_count = checkedSize(input.dependencies.size(), "dependency count does not fit UInt64", record_key);
        if (dependency_count > limits.maximum_direct_dependencies)
            fail(RecoveryError::Code::LimitExceeded, "direct dependency count exceeds its limit", record_key);

        UInt64 definition_edges = 0;
        UInt64 ir_atom_bytes = 0;
        UInt64 ir_literal_bytes = 0;
        UInt64 ir_enum_entries = 0;
        for (const auto & node : input.nodes)
        {
            addProspectively(
                definition_edges,
                checkedSize(node.children.size(), "template child count does not fit UInt64", record_key),
                limits.maximum_template_edges,
                "template edges exceed their limit",
                record_key);
            if (node.kind == TemplateNodeKind::BuiltIn)
            {
                addProspectively(
                    ir_atom_bytes,
                    checkedSize(node.atom.size(), "canonical IR atom size does not fit UInt64", record_key),
                    limits.maximum_ir_atom_bytes,
                    "canonical IR atom bytes exceed their limit",
                    record_key);
                for (const auto & child : node.children)
                {
                    addProspectively(
                        ir_atom_bytes,
                        checkedSize(child.label.size(), "canonical IR field-label size does not fit UInt64", record_key),
                        limits.maximum_ir_atom_bytes,
                        "canonical IR atom bytes exceed their limit",
                        record_key);
                }
            }
            if (node.kind == TemplateNodeKind::StringLiteral || node.kind == TemplateNodeKind::Identifier
                || node.kind == TemplateNodeKind::AggregateFunction || node.kind == TemplateNodeKind::DynamicSetting
                || node.kind == TemplateNodeKind::ObjectSetting || node.kind == TemplateNodeKind::ObjectTypedPath
                || node.kind == TemplateNodeKind::ObjectSkipPath || node.kind == TemplateNodeKind::ObjectSkipRegexp)
            {
                const UInt64 text_bytes = checkedSize(node.text.size(), "canonical IR literal size does not fit UInt64", record_key);
                if ((node.kind == TemplateNodeKind::Identifier || node.kind == TemplateNodeKind::AggregateFunction
                     || node.kind == TemplateNodeKind::DynamicSetting || node.kind == TemplateNodeKind::ObjectSetting)
                    && text_bytes > limits.maximum_ir_identifier_bytes)
                    fail(RecoveryError::Code::LimitExceeded, "canonical IR identifier bytes exceed their limit", record_key);
                addProspectively(
                    ir_literal_bytes,
                    text_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical IR literal bytes exceed their limit",
                    record_key);
            }
            if (node.kind == TemplateNodeKind::FieldValue)
            {
                addProspectively(
                    ir_literal_bytes,
                    checkedSize(node.field_value.payload.size(), "canonical Field payload size does not fit UInt64", record_key),
                    limits.maximum_ir_literal_bytes,
                    "canonical IR literal bytes exceed their limit",
                    record_key);
                addProspectively(
                    ir_literal_bytes,
                    checkedSize(node.field_value.name.size(), "canonical Field name size does not fit UInt64", record_key),
                    limits.maximum_ir_literal_bytes,
                    "canonical IR literal bytes exceed their limit",
                    record_key);
                if (node.field_value.kind == CanonicalFieldKind::Object)
                {
                    for (const auto & child : node.children)
                    {
                        addProspectively(
                            ir_literal_bytes,
                            checkedSize(child.label.size(), "canonical Object Field key size does not fit UInt64", record_key),
                            limits.maximum_ir_literal_bytes,
                            "canonical IR literal bytes exceed their limit",
                            record_key);
                    }
                }
            }
            if (node.kind == TemplateNodeKind::SpecializedEnum)
            {
                addProspectively(
                    ir_enum_entries,
                    checkedSize(node.enum_entries.size(), "canonical IR Enum entry count does not fit UInt64", record_key),
                    limits.maximum_ir_enum_entries,
                    "canonical IR Enum entry count exceeds its limit",
                    record_key);
                for (const auto & entry : node.enum_entries)
                {
                    addProspectively(
                        ir_literal_bytes,
                        checkedSize(entry.name.size(), "canonical IR Enum label size does not fit UInt64", record_key),
                        limits.maximum_ir_literal_bytes,
                        "canonical IR literal bytes exceed their limit",
                        record_key);
                }
            }
        }
        addProspectively(
            catalog_dependency_edges,
            dependency_count,
            limits.maximum_catalog_edges,
            "catalog dependency edges exceed their limit",
            record_key);
    }

private:
    const TemplateCheckerLimits & limits;
    UInt64 accepted_input_bytes = 0;
    UInt64 catalog_nodes = 0;
    UInt64 catalog_dependency_edges = 0;
};

}

AuthorityRecoveryError::AuthorityRecoveryError(Code code_, std::string_view message, std::optional<AuthorityInventoryKey> record_key_)
    : std::runtime_error(String(message))
    , code(code_)
    , record_key(std::move(record_key_))
{
}

AuthorityRoot::Ptr recoverAuthorityRoot(
    AuthorityState authority_state,
    UInt64 expected_type_index_generation,
    std::string_view inventory_snapshot_bytes,
    std::string_view schema_graph_snapshot_bytes,
    std::span<const AuthorityRecordImage> record_images,
    const AuthorityRecoveryLimits & requested_limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    AuthorityRecoveryLimits limits = requested_limits;
    if (limits.effective_database_limits)
    {
        /// Restart validates already-durable bytes under the immutable decoder
        /// envelope. A lowered mutable quota must not make its own previously
        /// valid root undecodable; the exact effective tuple is applied only
        /// when AuthorityRootBuilder creates the recovered quota snapshot,
        /// which retains excessive usage as OVER_QUOTA.
        /// Reset the complete aggregates, rather than only their database
        /// counters: this also restores the frozen Record V1, inventory/state,
        /// catalog, graph-snapshot and PersistedTypeReferences sidecar decoder
        /// bounds before any durable byte is inspected.
        limits.root = AuthorityRootBuildLimits{};
        limits.inventory_snapshot = AuthorityInventorySnapshotLimits{};
        limits.schema_graph = SchemaObjectDependencyGraphLimits{};
        limits.lowering = DefinitionLoweringLimits{};
        const auto implementation_limits = calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer>{});
        limits.root.maximum_definition_records = authority_recovery_maximum_definitions;
        limits.root.maximum_expectation_records = authority_recovery_maximum_expectations;
        limits.root.maximum_canonical_record_bytes = authority_recovery_maximum_total_record_bytes;
        limits.root.type_catalog.maximum_definitions = authority_recovery_maximum_definitions;
        limits.root.inventory.maximum_leaves = authority_recovery_maximum_record_images;
        limits.root.authority_state.maximum_leaves = authority_recovery_maximum_record_images;
        limits.inventory_snapshot.inventory.maximum_leaves = authority_recovery_maximum_record_images;
        limits.lowering.maximum_definitions = authority_recovery_maximum_definitions;
        limits.lowering.maximum_catalog_string_bytes = maximum_checker_catalog_bytes;
        limits.checker = makeTemplateCheckerLimits(implementation_limits);
        limits.maximum_record_images = authority_recovery_maximum_record_images;
        limits.maximum_total_record_bytes = authority_recovery_maximum_total_record_bytes;
        limits.maximum_parser_depth = authority_recovery_maximum_parser_depth;
        limits.maximum_parser_backtracks = authority_recovery_maximum_parser_backtracks;
    }
    validateRecoveryLimits(limits);
    validateAuthorityState(authority_state, limits.root.authority_state);
    const UInt64 recovered_type_index_generation = recoveredTypeIndexGeneration(authority_state);
    if (expected_type_index_generation != recovered_type_index_generation)
        fail(RecoveryError::Code::InvalidConfiguration, "caller type-index generation differs from the deterministic restart generation");

    const UInt64 record_image_count = checkedSize(record_images.size(), "authority record image count does not fit UInt64");
    const UInt64 maximum_record_images = std::min(
        {limits.maximum_record_images,
         limits.root.inventory.maximum_leaves,
         limits.root.authority_state.maximum_leaves,
         limits.inventory_snapshot.inventory.maximum_leaves});
    if (record_image_count > maximum_record_images)
        fail(RecoveryError::Code::LimitExceeded, "authority record image count exceeds its limit");
    if (record_image_count != authority_state.leaf_count)
        fail(RecoveryError::Code::InventoryMismatch, "authority record image count differs from the anchored authority state");

    UInt64 total_record_bytes = 0;
    const UInt64 maximum_total_record_bytes = std::min(limits.maximum_total_record_bytes, limits.root.maximum_canonical_record_bytes);
    for (const auto & image : record_images)
        addProspectively(
            total_record_bytes,
            checkedSize(image.canonical_bytes.size(), "authority record image size does not fit UInt64"),
            maximum_total_record_bytes,
            "authority record image bytes exceed their limit",
            image.key);

    AuthorityInventorySnapshot inventory_snapshot;
    AuthorityInventorySummary inventory_summary;
    try
    {
        inventory_snapshot = decodeAuthorityInventorySnapshot(inventory_snapshot_bytes, limits.inventory_snapshot);
        inventory_summary = buildAuthorityInventorySummary(inventory_snapshot.leaves, limits.inventory_snapshot.inventory);
    }
    catch (const AuthorityInventorySnapshotError & error)
    {
        if (error.code == AuthorityInventorySnapshotError::Code::InvalidConfiguration)
            fail(RecoveryError::Code::InvalidConfiguration, "authority inventory snapshot limits are invalid");
        if (error.code == AuthorityInventorySnapshotError::Code::LimitExceeded)
            fail(RecoveryError::Code::LimitExceeded, "authority inventory snapshot exceeds its recovery limit");
        fail(RecoveryError::Code::InventoryMismatch, "authority inventory snapshot is not canonical anchored V1 bytes");
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(RecoveryError::Code::LimitExceeded, "authority inventory exceeds its recovery limit");
        fail(RecoveryError::Code::InventoryMismatch, "authority inventory contains an invalid canonical leaf");
    }
    if (inventory_snapshot.database_uuid != authority_state.database_uuid)
        fail(RecoveryError::Code::InventoryMismatch, "authority inventory snapshot belongs to another database");
    if (inventory_summary.leaf_count != authority_state.leaf_count || inventory_summary.merkle_radix_root != authority_state.inventory_root)
        fail(RecoveryError::Code::InventoryMismatch, "authority inventory snapshot differs from the authority state");

    if (record_images.size() != inventory_snapshot.leaves.size())
        fail(RecoveryError::Code::InventoryMismatch, "authority record image count differs from the inventory snapshot");
    std::vector<std::size_t> image_order(record_images.size());
    std::iota(image_order.begin(), image_order.end(), 0);
    std::sort(
        image_order.begin(),
        image_order.end(),
        [&](std::size_t lhs, std::size_t rhs) { return authorityInventoryKeyLess(record_images[lhs].key, record_images[rhs].key); });
    for (std::size_t index = 0; index < image_order.size(); ++index)
    {
        if (record_images[image_order[index]].key != inventory_snapshot.leaves[index].key)
            fail(RecoveryError::Code::InventoryMismatch, "authority record images do not exactly match the inventory keys");
        if (index != 0 && record_images[image_order[index - 1]].key == record_images[image_order[index]].key)
            fail(RecoveryError::Code::InventoryMismatch, "authority record images contain a duplicate key");
    }

    UInt64 definition_count = 0;
    UInt64 expectation_count = 0;
    for (const auto & leaf : inventory_snapshot.leaves)
    {
        switch (leaf.key.record_kind)
        {
            case AuthorityInventoryRecordKind::TypeDefinition: ++definition_count; break;
            case AuthorityInventoryRecordKind::SidecarExpectation: ++expectation_count; break;
        }
    }
    const UInt64 maximum_definitions = std::min(
        {authority_recovery_maximum_definitions,
         limits.root.maximum_definition_records,
         limits.root.type_catalog.maximum_definitions,
         limits.lowering.maximum_definitions,
         limits.checker.maximum_definitions});
    if (definition_count > maximum_definitions)
        fail(RecoveryError::Code::LimitExceeded, "authority definition count exceeds the implementation recovery limit");
    const UInt64 maximum_expectations = std::min(authority_recovery_maximum_expectations, limits.root.maximum_expectation_records);
    if (expectation_count > maximum_expectations)
        fail(RecoveryError::Code::LimitExceeded, "authority expectation count exceeds the effective recovery limit");

    SchemaObjectDependencyGraph::Ptr schema_graph;
    try
    {
        schema_graph = SchemaObjectDependencyGraph::decodeSnapshot(schema_graph_snapshot_bytes, limits.schema_graph);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::InvalidConfiguration)
            fail(RecoveryError::Code::InvalidConfiguration, "schema graph recovery limits are invalid");
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(RecoveryError::Code::LimitExceeded, "schema graph snapshot exceeds its recovery limit");
        fail(RecoveryError::Code::SchemaGraphMismatch, "schema graph snapshot is not canonical anchored V1 bytes");
    }
    if (schema_graph->getDatabaseUUID() != authority_state.database_uuid
        || schema_graph->computeRoot() != authority_state.schema_graph_root)
        fail(RecoveryError::Code::SchemaGraphMismatch, "schema graph snapshot differs from the authority state");

    std::vector<Record> definition_records;
    std::vector<SidecarExpectationRecord> expectation_records;
    definition_records.reserve(static_cast<std::size_t>(definition_count));
    expectation_records.reserve(static_cast<std::size_t>(expectation_count));
    BoundedRecoveryExecutor recovery_executor(limits.parallel_workers);
    struct DecodedRecordSlot
    {
        std::optional<Record> definition;
        std::optional<SidecarExpectationRecord> expectation;
        std::exception_ptr error;
    };
    const std::size_t parallel_batch_records = static_cast<std::size_t>(limits.maximum_parallel_batch_records);
    for (std::size_t batch_begin = 0; batch_begin < image_order.size();)
    {
        const std::size_t batch_end = std::min(image_order.size(), batch_begin + parallel_batch_records);
        std::vector<DecodedRecordSlot> decoded(batch_end - batch_begin);
        recovery_executor.runBatch(
            batch_begin,
            batch_end,
            [&](std::size_t index) noexcept
            {
                auto & slot = decoded[index - batch_begin];
                try
                {
                    const auto & image = record_images[image_order[index]];
                    const auto & leaf = inventory_snapshot.leaves[index];
                    switch (image.key.record_kind)
                    {
                        case AuthorityInventoryRecordKind::TypeDefinition: {
                            auto record = decodeRecord(image.canonical_bytes, limits.root.definition_record);
                            if (record.identity.database_uuid != authority_state.database_uuid
                                || record.identity.type_uuid != image.key.object_uuid || record.identity.revision != leaf.object_revision
                                || computeRecordHash(record, limits.root.definition_record) != leaf.canonical_record_hash)
                            {
                                fail(
                                    RecoveryError::Code::RecordMismatch,
                                    "definition record does not match its authority inventory leaf",
                                    image.key);
                            }
                            slot.definition.emplace(std::move(record));
                            break;
                        }
                        case AuthorityInventoryRecordKind::SidecarExpectation: {
                            auto record = decodeSidecarExpectationRecord(image.canonical_bytes);
                            if (record.object.database_uuid != authority_state.database_uuid
                                || record.object.object_uuid != image.key.object_uuid
                                || record.object_schema_revision != leaf.object_revision
                                || computeSidecarExpectationRecordHash(record) != leaf.canonical_record_hash)
                            {
                                fail(
                                    RecoveryError::Code::RecordMismatch,
                                    "sidecar expectation does not match its authority inventory leaf",
                                    image.key);
                            }
                            slot.expectation.emplace(std::move(record));
                            break;
                        }
                    }
                }
                catch (...)
                {
                    slot.error = std::current_exception();
                }
            });

        for (std::size_t index = batch_begin; index < batch_end; ++index)
        {
            auto & slot = decoded[index - batch_begin];
            const auto & image = record_images[image_order[index]];
            if (slot.error)
            {
                try
                {
                    std::rethrow_exception(slot.error);
                }
                catch (const RecoveryError &)
                {
                    throw;
                }
                catch (const RecordError & error)
                {
                    if (error.code == RecordError::Code::LimitExceeded)
                        fail(RecoveryError::Code::LimitExceeded, "definition record exceeds its recovery limit", image.key);
                    fail(RecoveryError::Code::RecordMismatch, "definition record is not canonical V1 bytes", image.key);
                }
                catch (const SidecarExpectationRecordError &)
                {
                    fail(RecoveryError::Code::RecordMismatch, "sidecar expectation is not canonical V1 bytes", image.key);
                }
            }

            switch (image.key.record_kind)
            {
                case AuthorityInventoryRecordKind::TypeDefinition:
                    if (!slot.definition || slot.expectation)
                        fail(RecoveryError::Code::RecordMismatch, "definition record decoder produced an invalid result", image.key);
                    definition_records.push_back(std::move(*slot.definition));
                    break;
                case AuthorityInventoryRecordKind::SidecarExpectation:
                    if (!slot.expectation || slot.definition)
                        fail(RecoveryError::Code::RecordMismatch, "sidecar expectation decoder produced an invalid result", image.key);
                    expectation_records.push_back(std::move(*slot.expectation));
                    break;
            }
        }
        batch_begin = batch_end;
    }

    std::vector<AvailableDefinitionBinding> bindings;
    bindings.reserve(definition_records.size());
    for (const auto & record : definition_records)
    {
        std::vector<ParameterKind> parameter_kinds;
        parameter_kinds.reserve(record.parameters.size());
        for (const auto & parameter : record.parameters)
            parameter_kinds.push_back(parameter.kind);
        bindings.push_back({
            .name = structuredNameFromRecord(record, definitionRecordKey(record)),
            .identity = record.identity,
            .definition_hash = record.definition_hash,
            .parameter_kinds = std::move(parameter_kinds),
        });
    }

    std::vector<DefinitionInput> lowered;
    lowered.reserve(definition_records.size());
    if (!definition_records.empty())
    {
        const String normalized_database_name = bindings.front().name.normalized_database_name;
        std::optional<PreparedDefinitionLoweringBindings> prepared_bindings;
        try
        {
            prepared_bindings.emplace(prepareDefinitionLoweringBindings(
                authority_state.database_uuid, normalized_database_name, std::move(bindings), limits.lowering));
        }
        catch (const Exception & error)
        {
            if (mustRethrowRecoveryResourceOrControlException(error))
                throw;
            if (isLimitError(error))
                fail(RecoveryError::Code::LimitExceeded, "definition binding catalog exceeds its recovery limit");
            fail(RecoveryError::Code::RecordMismatch, "definition records cannot form one canonical same-database binding catalog");
        }

        RetainedDefinitionInputBudget retained_budget(limits.checker);
        struct LoweredDefinitionSlot
        {
            std::optional<DefinitionInput> input;
            std::exception_ptr error;
        };
        for (std::size_t batch_begin = 0; batch_begin < definition_records.size();)
        {
            const std::size_t batch_end = std::min(definition_records.size(), batch_begin + parallel_batch_records);
            std::vector<LoweredDefinitionSlot> batch(batch_end - batch_begin);
            recovery_executor.runBatch(
                batch_begin,
                batch_end,
                [&](std::size_t index) noexcept
                {
                    auto & slot = batch[index - batch_begin];
                    const auto & record = definition_records[index];
                    const AuthorityInventoryKey record_key = definitionRecordKey(record);
                    try
                    {
                        const auto * binding = prepared_bindings->findByIdentity(record.identity);
                        if (!binding)
                        {
                            fail(
                                RecoveryError::Code::RecordMismatch,
                                "definition record is absent from its prepared binding catalog",
                                record_key);
                        }
                        ASTPtr ast = parseCanonicalAttach(record, record_key, limits);
                        slot.input.emplace(lowerCreateTypeQueryToDefinitionInput(
                            ast->as<ASTCreateTypeQuery &>(), record.identity, binding->name, *prepared_bindings));
                    }
                    catch (...)
                    {
                        slot.error = std::current_exception();
                    }
                });

            for (std::size_t index = batch_begin; index < batch_end; ++index)
            {
                auto & slot = batch[index - batch_begin];
                const AuthorityInventoryKey record_key = definitionRecordKey(definition_records[index]);
                if (slot.error)
                {
                    try
                    {
                        std::rethrow_exception(slot.error);
                    }
                    catch (const RecoveryError &)
                    {
                        throw;
                    }
                    catch (const Exception & error)
                    {
                        if (mustRethrowRecoveryResourceOrControlException(error))
                            throw;
                        if (isLimitError(error))
                            fail(RecoveryError::Code::LimitExceeded, "definition lowering exceeds its recovery limit", record_key);
                        fail(
                            RecoveryError::Code::RecordMismatch,
                            "canonical ATTACH TYPE cannot be lowered for its durable record",
                            record_key);
                    }
                }
                if (!slot.input)
                    fail(RecoveryError::Code::RecordMismatch, "definition lowering produced no durable input", record_key);
                retained_budget.charge(*slot.input, record_key);
                lowered.push_back(std::move(*slot.input));
            }
            batch_begin = batch_end;
        }
    }

    std::vector<Definition::Ptr> checked_definitions;
    try
    {
        checked_definitions = TemplateChecker::checkAll(std::move(lowered), limits.checker);
    }
    catch (const Exception & error)
    {
        if (mustRethrowRecoveryResourceOrControlException(error))
            throw;
        if (isLimitError(error))
            fail(RecoveryError::Code::LimitExceeded, "definition checking exceeds its recovery limit");
        fail(RecoveryError::Code::DefinitionMismatch, "lowered definitions do not form one valid Atomic authority");
    }
    if (checked_definitions.size() != definition_records.size())
        fail(RecoveryError::Code::DefinitionMismatch, "definition checker returned an incomplete authority batch");
    for (std::size_t index = 0; index < definition_records.size(); ++index)
    {
        if (definition_records[index].identity != checked_definitions[index]->getIdentity()
            || !recordMatchesCheckedDefinition(definition_records[index], *checked_definitions[index]))
            fail(
                RecoveryError::Code::DefinitionMismatch,
                "durable definition record differs from freshly checked semantics",
                definitionRecordKey(definition_records[index]));
    }

    try
    {
        if (limits.effective_database_limits)
        {
            return AuthorityRootBuilder::build(
                std::move(authority_state),
                recovered_type_index_generation,
                checked_definitions,
                definition_records,
                expectation_records,
                std::move(schema_graph),
                *limits.effective_database_limits,
                limits.root,
                dependent_objects);
        }
        return AuthorityRootBuilder::build(
            std::move(authority_state),
            recovered_type_index_generation,
            checked_definitions,
            definition_records,
            expectation_records,
            std::move(schema_graph),
            limits.root,
            dependent_objects);
    }
    catch (const AuthorityRootError & error)
    {
        switch (error.code)
        {
            case AuthorityRootError::Code::InvalidConfiguration:
                fail(RecoveryError::Code::InvalidConfiguration, "authority root recovery limits are invalid");
            case AuthorityRootError::Code::InvalidAuthorityState:
                fail(RecoveryError::Code::AuthorityStateMismatch, "authority root rejected the anchored authority state");
            case AuthorityRootError::Code::DatabaseMismatch:
                fail(RecoveryError::Code::RecordMismatch, "authority component belongs to another database");
            case AuthorityRootError::Code::InvalidDefinition:
            case AuthorityRootError::Code::RecordDefinitionMismatch:
                fail(RecoveryError::Code::DefinitionMismatch, "durable definition record differs from freshly checked semantics");
            case AuthorityRootError::Code::InvalidRecord:
            case AuthorityRootError::Code::DuplicateRecordIdentity:
                fail(RecoveryError::Code::RecordMismatch, "authority contains an invalid or duplicate durable record");
            case AuthorityRootError::Code::InventoryMismatch:
                fail(RecoveryError::Code::InventoryMismatch, "reconstructed authority inventory differs from its anchor");
            case AuthorityRootError::Code::GraphMismatch:
                fail(RecoveryError::Code::SchemaGraphMismatch, "schema graph differs from reconstructed authority records");
            case AuthorityRootError::Code::LimitExceeded:
                fail(RecoveryError::Code::LimitExceeded, "reconstructed authority root exceeds its recovery limit");
        }
    }
}
}
