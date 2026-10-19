#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

FutureMergedMutatedPartPtr constructFuturePart(const MergeTreeData & data, const MergeSelectorChoice & choice, MergeTreeData::DataPartStates lookup_statuses);

}
