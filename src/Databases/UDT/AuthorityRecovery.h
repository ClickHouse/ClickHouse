#pragma once

#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/AuthorityInventorySnapshot.h>
#include <DataTypes/UDT/DefinitionLowering.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/TemplateChecker.h>

#include <Core/Types.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt64 authority_recovery_default_maximum_definitions = 10'000;
inline constexpr UInt64 authority_recovery_maximum_definitions = 100'000;
inline constexpr UInt64 authority_recovery_maximum_expectations = 100'000;
inline constexpr UInt64 authority_recovery_default_maximum_record_images
    = authority_recovery_default_maximum_definitions + authority_recovery_maximum_expectations;
inline constexpr UInt64 authority_recovery_maximum_record_images
    = authority_recovery_maximum_definitions + authority_recovery_maximum_expectations;
inline constexpr UInt64 authority_recovery_maximum_total_record_bytes = resource_implementation_maximum_deterministic_catalog_bytes;
inline constexpr UInt64 authority_recovery_default_deterministic_catalog_bytes = 256ULL << 20;
inline constexpr UInt64 authority_recovery_maximum_parser_depth = 256;
inline constexpr UInt64 authority_recovery_maximum_parser_backtracks = 100'000;
inline constexpr UInt64 authority_recovery_maximum_parallel_workers = 64;
inline constexpr UInt64 authority_recovery_maximum_parallel_batch_records = 1'024;

/// One canonical authority record read from the database-owned metadata
/// layout. The key is supplied by the anchored inventory snapshot, never
/// reconstructed from a file name or from the record payload.
struct AuthorityRecordImage
{
    AuthorityInventoryKey key;
    std::string_view canonical_bytes;
};

struct AuthorityRecoveryLimits
{
    /// Exact implementation/server/persisted-database/adapter tuple selected
    /// by the owning database. Absence is retained only for compatibility with
    /// focused synthetic callers and selects the normative database defaults.
    std::optional<EffectiveResourceLimits> effective_database_limits;
    AuthorityRootBuildLimits root = []
    {
        AuthorityRootBuildLimits result;
        result.type_catalog.maximum_definitions = authority_recovery_default_maximum_definitions;
        result.inventory.maximum_leaves = authority_recovery_default_maximum_record_images;
        result.authority_state.maximum_leaves = authority_recovery_default_maximum_record_images;
        result.maximum_definition_records = authority_recovery_default_maximum_definitions;
        result.maximum_expectation_records = authority_recovery_maximum_expectations;
        return result;
    }();
    AuthorityInventorySnapshotLimits inventory_snapshot = []
    {
        AuthorityInventorySnapshotLimits result;
        result.inventory.maximum_leaves = authority_recovery_default_maximum_record_images;
        return result;
    }();
    SchemaObjectDependencyGraphLimits schema_graph;
    DefinitionLoweringLimits lowering = []
    {
        DefinitionLoweringLimits result;
        result.maximum_definitions = authority_recovery_default_maximum_definitions;
        result.maximum_catalog_string_bytes = authority_recovery_default_deterministic_catalog_bytes;
        return result;
    }();
    TemplateCheckerLimits checker = []
    {
        TemplateCheckerLimits result;
        result.maximum_definitions = authority_recovery_default_maximum_definitions;
        result.maximum_catalog_input_bytes = authority_recovery_default_deterministic_catalog_bytes;
        result.maximum_catalog_nodes = 40'960'000;
        result.maximum_catalog_edges = 2'560'000;
        result.maximum_catalog_checker_work = 3'287'070'000;
        result.maximum_canonical_catalog_bytes = authority_recovery_default_deterministic_catalog_bytes;
        result.maximum_scratch_bytes = 1ULL << 30;
        return result;
    }();
    UInt64 maximum_record_images = authority_recovery_default_maximum_record_images;
    UInt64 maximum_total_record_bytes = authority_recovery_maximum_total_record_bytes;
    UInt64 maximum_parser_depth = authority_recovery_maximum_parser_depth;
    UInt64 maximum_parser_backtracks = authority_recovery_maximum_parser_backtracks;
    /// Actual local recovery concurrency. Defaults are deliberately modest;
    /// callers may lower them or raise them only within the reviewed hard
    /// implementation domain above. The batch bound limits simultaneously
    /// materialized decoded/lowered records independently of catalog size.
    UInt64 parallel_workers = 8;
    UInt64 maximum_parallel_batch_records = 64;
};

class AuthorityRecoveryError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        InventoryMismatch,
        RecordMismatch,
        CanonicalSQLMismatch,
        AuthorityStateMismatch,
        SchemaGraphMismatch,
        DefinitionMismatch,
    };

    AuthorityRecoveryError(Code code_, std::string_view message, std::optional<AuthorityInventoryKey> record_key_ = std::nullopt);

    const Code code;
    const std::optional<AuthorityInventoryKey> record_key;
};

/// Restart has no surviving in-memory generation. Recovery therefore derives
/// the rebuilt type-index generation from the already anchored database epoch;
/// callers may pass it as an expectation but cannot select another value.
[[nodiscard]] constexpr UInt64 recoveredTypeIndexGeneration(const AuthorityState & authority_state) noexcept
{
    return authority_state.database_catalog_epoch;
}

/// Reconstructs one complete immutable authority value from durable bytes.
/// The inventory and graph snapshots are verified against `authority_state`;
/// record images must be a one-to-one match for the inventory leaves. Every
/// definition is parsed, lowered and checked as one same-database batch before
/// the record/checker/root comparison and the single returned publication.
[[nodiscard]] AuthorityRoot::Ptr recoverAuthorityRoot(
    AuthorityState authority_state,
    UInt64 expected_type_index_generation,
    std::string_view inventory_snapshot_bytes,
    std::string_view schema_graph_snapshot_bytes,
    std::span<const AuthorityRecordImage> record_images,
    const AuthorityRecoveryLimits & limits = {},
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects = {});
}
