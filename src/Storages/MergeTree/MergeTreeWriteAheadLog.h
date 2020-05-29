#pragma once

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Disks/IDisk.h>

namespace DB
{

class MergeTreeData;

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
    constexpr static auto DEFAULT_WAL_FILE = "wal.bin";

    MergeTreeWriteAheadLog(const MergeTreeData & storage_, const DiskPtr & disk_,
        const String & name = DEFAULT_WAL_FILE);

    void addPart(const Block & block, const String & part_name);
    void dropPart(const String & part_name);
    std::vector<MergeTreeMutableDataPartPtr> restore();

    using MinMaxBlockNumber = std::pair<Int64, Int64>;
    static std::optional<MinMaxBlockNumber> tryParseMinMaxBlockNumber(const String & filename);

private:
    void init();
    void rotate();

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
