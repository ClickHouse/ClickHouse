#pragma once

#include <variant>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using NoCommitOptions = std::monostate;

template <typename ZooKeeperConnection>
struct MetaInKeeperCommitOptions
{
    bool need_rollback_blobs = true;
    bool may_retry = false;
    ZooKeeperConnection * zookeeper;
    Coordination::Requests additional_requests;
};

using TransactionCommitOptionsVariant = std::variant<
    NoCommitOptions,
    MetaInKeeperCommitOptions<zkutil::ZooKeeper>,
    MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>>;

inline String getFirstFailedPath(Coordination::Error code, const Coordination::Responses & responses,
                                 size_t first_additional_request_idx, const Coordination::Requests & additional_ops)
{
    size_t failed_op_index = zkutil::getFailedOpIndex(code, responses);
    if (failed_op_index < first_additional_request_idx)
        return "<in disk transaction>";
    return additional_ops.at(failed_op_index - first_additional_request_idx)->getPath();
}

struct MetaInKeeperCommitOutcome
{
    Coordination::Error code;

    /// Responses for the keeper multi-op performed by this transaction.
    /// The order is: disk's internal ops, then `additional_requests`.
    /// Populated only if isUserError(code).
    Coordination::Responses responses;
    size_t first_additional_request_idx = 0;

    size_t additionalOpIdx(size_t idx) const { return first_additional_request_idx + idx; }
};

using TransactionCommitOutcomeVariant = std::variant<
    bool,
    MetaInKeeperCommitOutcome>;

inline bool isSuccessfulOutcome(const TransactionCommitOutcomeVariant & outcome)
{
    if (const auto * success = std::get_if<bool>(&outcome); success)
        return *success;
    else if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        return result->code == Coordination::Error::ZOK;

    UNREACHABLE();
}

inline bool needRollbackBlobs(const TransactionCommitOptionsVariant & options)
{
    if (const auto * with_faults = std::get_if<MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>>(&options); with_faults)
        return with_faults->need_rollback_blobs;
    else if (const auto * just_keeper = std::get_if<MetaInKeeperCommitOptions<zkutil::ZooKeeper>>(&options); just_keeper)
        return just_keeper->need_rollback_blobs;

    return true;
}

inline bool canRollbackBlobs(const TransactionCommitOptionsVariant & options, const TransactionCommitOutcomeVariant & outcome)
{
    if (needRollbackBlobs(options))
    {
        if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        {
            return !Coordination::isHardwareError(result->code);
        }
        return true;
    }
    return false;
}

inline bool mayRetryCommit(const TransactionCommitOptionsVariant & options, const TransactionCommitOutcomeVariant & outcome)
{
    if (isSuccessfulOutcome(outcome))
        return false;

    if (const auto * with_faults = std::get_if<MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>>(&options); with_faults)
    {
        return with_faults->may_retry;
    }
    else if (const auto * just_keeper = std::get_if<MetaInKeeperCommitOptions<zkutil::ZooKeeper>>(&options); just_keeper)
    {
        return just_keeper->may_retry;
    }

    return false;
}

inline MetaInKeeperCommitOutcome getKeeperOutcome(const TransactionCommitOutcomeVariant & outcome)
{
    if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        return *result;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not keeper outcome");
}

}
