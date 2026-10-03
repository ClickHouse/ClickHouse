#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <expected>
#include <memory>

namespace DB
{

class IMergePredicate
{
public:
    virtual ~IMergePredicate() = default;

    virtual std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const = 0;

    /// Returns maximal version of patch part required to be applied to the part during merge.
    /// Returns 0 if there are no patch parts to apply.
    virtual PartsRange getPatchesToApplyOnMerge(const PartsRange & range) const = 0;

    /// Checks that `range`, whose parts belong to one partition, includes every part of the partition that exists or that
    /// an operation already knows will produce, e.g. a part that this replica has not fetched yet. The caller has checked
    /// that `range` holds all parts of the partition that the parts collector has returned. Parts inserted after the check
    /// are not considered.
    virtual std::expected<void, PreformattedMessage> checkRangeCoversPartition(const PartsRange & range) const = 0;
};

using MergePredicatePtr = std::shared_ptr<const IMergePredicate>;

}
