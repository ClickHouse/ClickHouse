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
};

using MergePredicatePtr = std::shared_ptr<const IMergePredicate>;

}
