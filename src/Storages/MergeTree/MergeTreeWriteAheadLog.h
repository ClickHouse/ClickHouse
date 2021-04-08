#pragma once

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Disks/IDisk.h>

namespace DB
{

class MergeTreeData;

/** WAL stores addditions and removals of data parts in in-memory format.
  * Format of data in WAL:
  * - version
  * - type of action (ADD or DROP)
  * - part name
  * - part's block in Native format. (for ADD action)
  */
class MergeTreeWriteAheadLog
{
public:
    /// Append-only enum. It is serialized to WAL
    enum class ActionType : UInt8
    {
        ADD_PART = 0,
        DROP_PART = 1,
    };

    constexpr static auto WAL_FILE_NAME = "wal";
    constexpr static auto WAL_FILE_EXTENSION = ".bin";
    constexpr static auto DEFAULT_WAL_FILE_NAME = "wal.bin";

    MergeTreeWriteAheadLog(const MergeTreeData & storage_, const DiskPtr & disk_,
        const String & name = DEFAULT_WAL_FILE_NAME);

    void addPart(const Block & block, const String & part_name);
    void dropPart(const String & part_name);
    std::vector<MergeTreeMutableDataPartPtr> restore(const StorageMetadataPtr & metadata_snapshot);

    using MinMaxBlockNumber = std::pair<Int64, Int64>;
    static std::optional<MinMaxBlockNumber> tryParseMinMaxBlockNumber(const String & filename);

private:
    void init();
    void rotate(const std::lock_guard<std::mutex> & lock);

    const MergeTreeData & storage;
    DiskPtr disk;
    String name;
    String path;

    std::unique_ptr<WriteBuffer> out;
    std::unique_ptr<NativeBlockOutputStream> block_out;

    Int64 min_block_number = std::numeric_limits<Int64>::max();
    Int64 max_block_number = -1;

    mutable std::mutex write_mutex;
};

}
