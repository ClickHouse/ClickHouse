#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/Record.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::UDT
{

enum class DefinitionMutationKind : UInt8
{
    Create = 1,
    ReplaceSemantic = 2,
    Rename = 3,
    Comment = 4,
    Drop = 5,
};

/// One fully checked definition mutation request. Create has no before
/// identity; Drop has no after image; every other operation has both. An
/// expected epoch of zero names the never-enabled authority state.
struct DefinitionMutationRequest
{
    DefinitionMutationKind kind{};
    UUID database_uuid = UUIDHelpers::Nil;
    /// Zero is accepted only when CREATE IF NOT EXISTS proves a no-op. Every
    /// mutation that produces durable work requires a nonzero transaction ID.
    UInt64 transaction_id = 0;
    std::optional<UInt64> expected_database_catalog_epoch;
    std::optional<DefinitionIdentity> expected_before_identity;
    Definition::Ptr after_definition;
    std::optional<Record> after_record;
    /// A name collision is a no-op only when the checked executable body is
    /// exactly equivalent. Administrative and presentation bytes are ignored.
    bool if_not_exists = false;
    /// Authenticated internal replay may additionally bind the no-op to the
    /// requested stable type UUID. Valid only together with `if_not_exists`.
    bool require_exact_type_uuid_on_noop = false;

    /// RENAME may need to update the canonical SQL presentation of every
    /// definition that directly calls the renamed identity, including the
    /// target's decreasing self-call surface. These auxiliary records keep
    /// their exact checked definitions and every durable semantic/admin byte;
    /// only the two synchronized canonical SQL fields may differ. The planner
    /// proves this vector is the complete exact direct-dependent set and emits
    /// all rewrites in the same replacement root and WAL transition.
    std::vector<Record> rename_dependent_record_rewrites;
};

struct DefinitionMutationPlannerLimits
{
    DefinitionMutationPlannerLimits();

    AuthorityRootBuildLimits authority_root;
    DatabaseSchemaWALLimits schema_wal;
    UInt64 maximum_definition_retained_bytes = 256ULL << 10;
    /// Mandatory only for the never-enabled -> epoch-1 transition. It is the
    /// exact implementation/server/persisted-database/adapter minimum already
    /// reconciled by DatabaseAtomic. Ordinary successors inherit the tuple
    /// from `current_root` and must not supply a replacement here.
    std::optional<EffectiveResourceLimits> initial_effective_database_limits;
};

class DefinitionMutationPlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidRequest,
        DatabaseMismatch,
        ExpectedEpochMismatch,
        DefinitionNotFound,
        DuplicateTypeUUID,
        DuplicateName,
        InvalidRevision,
        DefinitionRecordMismatch,
        MutationKindMismatch,
        DefinitionConflict,
        ReferencedDefinition,
        LimitExceeded,
        InvalidBase,
        InvalidTransition,
    };

    DefinitionMutationPlannerError(Code code_, std::string_view message);

    const Code code;
};

/// Pure pre-I/O result. A no-op owns neither a replacement root nor a WAL
/// transition. Otherwise the replacement root may be released into
/// AtomicAuthority::preparePublication while this value keeps
/// the validated transition alive for durable execution.
class PreparedDefinitionMutation final
{
public:
    PreparedDefinitionMutation(const PreparedDefinitionMutation &) = delete;
    PreparedDefinitionMutation & operator=(const PreparedDefinitionMutation &) = delete;
    PreparedDefinitionMutation(PreparedDefinitionMutation &&) noexcept = default;
    PreparedDefinitionMutation & operator=(PreparedDefinitionMutation &&) noexcept = default;

    const AuthorityRoot & getReplacementRoot() const;
    const DatabaseSchemaWALValidatedTransition & getValidatedTransition() const;
    bool isNoOp() const noexcept { return !replacement_root && !transition; }
    bool hasReplacementRoot() const noexcept { return replacement_root != nullptr; }
    bool hasValidatedTransition() const noexcept { return transition.has_value(); }
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

private:
    PreparedDefinitionMutation() = default;
    PreparedDefinitionMutation(AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_);

    friend class DefinitionMutationPlanner;

    AuthorityRoot::Ptr replacement_root;
    std::optional<DatabaseSchemaWALValidatedTransition> transition;
};

class DefinitionMutationPlanner final
{
public:
    /// `current_root` is a caller-pinned immutable composite root, or null for
    /// first activation. The planner performs no I/O and never enumerates
    /// storage; all authority inputs come from that exact root.
    [[nodiscard]] static PreparedDefinitionMutation
    plan(const AuthorityRoot * current_root, DefinitionMutationRequest request, const DefinitionMutationPlannerLimits & limits = {});

private:
    DefinitionMutationPlanner() = delete;
};

}
