#pragma once
#include <Core/Types.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

enum class MergeTreeDeduplicationOp : uint8_t
{
    ADD = 1,
    DROP = 2,
};

struct MergeTreeDeduplicationLogRecord
{
    MergeTreeDeduplicationOp operation;
    std::string part_name;
    std::string block_id;
};

struct MergeTreeDeduplicationLogNameDescription
{
    std::string path;
    size_t entries_count;
};

class MergeTreeDeduplicationLog
{
public:
    MergeTreeDeduplicationLog(
        const std::string & logs_dir_,
        size_t deduplication_window_,
        size_t rotate_interval_);

    bool addPart(const MergeTreeData::MutableDataPartPtr & part);
    void dropPart(const MergeTreeData::MutableDataPartPtr & part);
    void dropPartition(const std::string & partition_id);

    void load();
private:
    const std::string logs_dir;
    const size_t deduplication_window;
    const size_t rotate_interval;
    size_t log_counter = 1;
    std::map<size_t, MergeTreeDeduplicationLogNameDescription> existing_logs;

    std::unordered_set<std::string> deduplication_set;

    std::unique_ptr<WriteBufferFromFile> current_writer;
    size_t entries_written_in_current_file;

    void rotate();
    std::unordered_set<std::string> loadSingleLog(const std::string & path) const;
};

}
