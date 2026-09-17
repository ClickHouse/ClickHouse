#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <Core/Types.h>

#include <optional>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

struct DependentObjectActivationPlannerLimits
{
    DatabaseSchemaWALLimits schema_wal;
};

class DependentObjectActivationPlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidRequest,
        ExpectedEpochMismatch,
        CapabilityMismatch,
        LimitExceeded,
        InvalidTransition,
    };

    DependentObjectActivationPlannerError(Code code_, std::string_view message);

    const Code code;
};

/// Pure, pre-I/O result for the one permanent definition-only -> dependent-object-capable capability
/// transition. The replacement root shares the exact immutable content payload
/// with the pinned definition-only root; the WAL transition carries no content delta.
class PreparedDependentObjectActivation final
{
public:
    PreparedDependentObjectActivation(const PreparedDependentObjectActivation &) = delete;
    PreparedDependentObjectActivation & operator=(const PreparedDependentObjectActivation &) = delete;
    PreparedDependentObjectActivation(PreparedDependentObjectActivation &&) noexcept = default;
    PreparedDependentObjectActivation & operator=(PreparedDependentObjectActivation &&) noexcept = default;

    const AuthorityRoot & getReplacementRoot() const noexcept { return *replacement_root; }
    const DatabaseSchemaWALValidatedTransition & getValidatedTransition() const noexcept { return transition; }
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

private:
    PreparedDependentObjectActivation(AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_);

    friend class DependentObjectActivationPlanner;

    AuthorityRoot::Ptr replacement_root;
    DatabaseSchemaWALValidatedTransition transition;
};

class DependentObjectActivationPlanner final
{
public:
    [[nodiscard]] static PreparedDependentObjectActivation plan(
        const AuthorityRoot & current_root,
        UInt64 transaction_id,
        std::optional<UInt64> expected_database_catalog_epoch = std::nullopt,
        const DependentObjectActivationPlannerLimits & limits = {});

private:
    DependentObjectActivationPlanner() = delete;
};

}
