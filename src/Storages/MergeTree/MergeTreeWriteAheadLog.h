#pragma once

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>

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

    struct ActionMetadata
    {
        /// The minimum version of WAL reader that can understand metadata written by current ClickHouse version.
        /// This field must be increased when making backwards incompatible changes.
        ///
        /// The same approach can be used recursively inside metadata.
        UInt8 min_compatible_version = 0;

        /// Actual metadata.
        UUID part_uuid = UUIDHelpers::Nil;

        void write(WriteBuffer & meta_out) const;
        void read(ReadBuffer & meta_in);

    private:
        static constexpr auto JSON_KEY_PART_UUID = "part_uuid";

        String toJSON() const;
        void fromJSON(const String & buf);
    };

    constexpr static UInt8 WAL_VERSION = 1;
    constexpr static auto WAL_FILE_NAME = "wal";
    constexpr static auto WAL_FILE_EXTENSION = ".bin";
    constexpr static auto DEFAULT_WAL_FILE_NAME = "wal.bin";

    MergeTreeWriteAheadLog(MergeTreeData & storage_, const DiskPtr & disk_,
        const String & name = DEFAULT_WAL_FILE_NAME);

    ~MergeTreeWriteAheadLog();

    void addPart(DataPartInMemoryPtr & part);
    void dropPart(const String & part_name);
    std::vector<MergeTreeMutableDataPartPtr> restore(const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    using MinMaxBlockNumber = std::pair<Int64, Int64>;
    static std::optional<MinMaxBlockNumber> tryParseMinMaxBlockNumber(const String & filename);

private:
    void init();
    void rotate(const std::unique_lock<std::mutex> & lock);
    void sync(std::unique_lock<std::mutex> & lock);

    const MergeTreeData & storage;
    DiskPtr disk;
    String name;
    String path;

    std::unique_ptr<WriteBuffer> out;
    std::unique_ptr<NativeWriter> block_out;

    Int64 min_block_number = std::numeric_limits<Int64>::max();
    Int64 max_block_number = -1;

    BackgroundSchedulePool & pool;
    BackgroundSchedulePoolTaskHolder sync_task;
    std::condition_variable sync_cv;

    size_t bytes_at_last_sync = 0;
    bool sync_scheduled = false;

    mutable std::mutex write_mutex;

    Poco::Logger * log;
};

}
