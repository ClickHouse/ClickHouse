#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>

#include <expected>
#include <memory>

namespace DB
{

class IMergePredicate
{
public:
    virtual ~IMergePredicate() = default;

    virtual std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const = 0;
};

using MergePredicatePtr = std::shared_ptr<const IMergePredicate>;

}
