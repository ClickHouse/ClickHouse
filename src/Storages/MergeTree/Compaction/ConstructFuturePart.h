#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Common/LoggingFormatStringHelpers.h>

#include <expected>

namespace DB
{

std::expected<FutureMergedMutatedPartPtr, PreformattedMessage> constructFuturePart(const MergeTreeData & data, const MergeSelectorChoice & choice, MergeTreeData::DataPartStates lookup_statuses);

}
