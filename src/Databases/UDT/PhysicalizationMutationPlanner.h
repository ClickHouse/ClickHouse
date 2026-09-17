#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/PhysicalizationPlan.h>

#include <Core/Types.h>

#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace DB::UDT
{

/// Exact before/after images produced by the registered schema-object adapter
/// while the caller still owns the database schema-mutation guard. The
/// planner treats the bytes as opaque canonical metadata, but binds both
/// adapter-reported fingerprints and the complete before-image content
/// address to the freshly recomputed loss manifest.
struct PhysicalizationRewriteImage
{
    SchemaObjectID object;
    UInt64 before_object_schema_revision = 0;
    UInt64 after_object_schema_revision = 0;
    String before_canonical_metadata_bytes;
    String after_canonical_metadata_bytes;
    Digest before_physical_schema_fingerprint{};
    Digest after_physical_schema_fingerprint{};
};

struct PhysicalizationMutationPlannerLimits
{
    PhysicalizationMutationPlannerLimits();

    AuthorityRootBuildLimits authority_root;
    DatabaseSchemaWALLimits schema_wal;
    UInt64 maximum_definition_retained_bytes = 256ULL << 10;
};

class PhysicalizationMutationPlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidRequest,
        DatabaseMismatch,
        ExpectedEpochMismatch,
        StalePlan,
        InvalidPlan,
        InvalidRewriteImages,
        IntegrityMismatch,
        RemainingDependent,
        LimitExceeded,
        InvalidTransition,
    };

    PhysicalizationMutationPlannerError(Code code_, std::string_view message);

    const Code code;
};

/// Pure pre-I/O result. It owns both the replacement composite root and the
/// already cross-linked WAL transition. A caller may release the root into
/// AtomicAuthority::preparePublication while retaining this
/// value until durable execution has completed.
class PreparedPhysicalizationMutation final
{
public:
    PreparedPhysicalizationMutation(const PreparedPhysicalizationMutation &) = delete;
    PreparedPhysicalizationMutation & operator=(const PreparedPhysicalizationMutation &) = delete;
    PreparedPhysicalizationMutation(PreparedPhysicalizationMutation &&) noexcept = default;
    PreparedPhysicalizationMutation & operator=(PreparedPhysicalizationMutation &&) noexcept = default;

    const AuthorityRoot & getReplacementRoot() const noexcept { return *replacement_root; }
    const DatabaseSchemaWALValidatedTransition & getValidatedTransition() const noexcept { return transition; }
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

private:
    PreparedPhysicalizationMutation(AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_);

    friend class PhysicalizationMutationPlanner;

    AuthorityRoot::Ptr replacement_root;
    DatabaseSchemaWALValidatedTransition transition;
};

class PhysicalizationMutationPlanner final
{
public:
    /// `current_root` and `plan` must name the same exact dependent-object-capable epoch. The
    /// images must be in the plan's canonical object order and contain every
    /// selected object exactly once.
    [[nodiscard]] static PreparedPhysicalizationMutation plan(
        const AuthorityRoot & current_root,
        const PhysicalizationPlan & freshly_recomputed_plan,
        UInt64 transaction_id,
        UInt64 expected_database_catalog_epoch,
        std::span<const PhysicalizationRewriteImage> rewrite_images,
        const PhysicalizationMutationPlannerLimits & limits = {});

private:
    PhysicalizationMutationPlanner() = delete;
};

}
