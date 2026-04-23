#pragma once

#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

class Pipe;
class AlterConversions;
using AlterConversionsPtr = std::shared_ptr<const AlterConversions>;

class MergeTreeData;
class IMergeTreeDataPart;
struct ProjectionDescription;
struct StorageInMemoryMetadata;
struct RangesInDataPart;
struct MergeTreePartInfo;

/// How a part should be read to produce rows in commit order,
/// i.e. ascending by `(_block_number, _block_offset)`.
struct CommitOrderReadStrategy
{
    enum Kind : uint8_t
    {
        /// Reading the part in its native storage order already yields commit order.
        Native,
        /// The part must be sorted by `(_block_number, _block_offset)` after reading.
        Sort,
    };

    Kind kind = Kind::Sort;
    RangesInDataPart ranges_to_read;
};

/// Throws `ILLEGAL_STREAM` when the part cannot be served in commit order.
CommitOrderReadStrategy chooseCommitOrderReadStrategy(
    const RangesInDataPart & ranges,
    const StorageMetadataPtr & metadata);

/// `columns_to_read` must include `_block_number` / `_block_offset` for Sort.
Pipe createCommitOrderReadStream(
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    AlterConversionsPtr alter_conversions,
    Names columns_to_read,
    const CommitOrderReadStrategy & strategy,
    ContextPtr context);

}
