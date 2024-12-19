#pragma once

#include <Storages/MergeTree/FutureMergedMutatedPart.h>

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>

namespace DB
{

class IPartsFinder
{
public:
    virtual ~IPartsFinder() = default;

    virtual FutureMergedMutatedPartPtr constructFuturePart(const MergeSelectorChoice & choice) const = 0;
};

}
