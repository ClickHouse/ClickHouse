#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Common/logger_useful.h>

namespace DB
{


/// Used to read data from single part with select query in order of primary key.
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeInOrderSelectAlgorithm final : public MergeTreeSelectAlgorithm
{
public:
    template <typename... Args>
    explicit MergeTreeInOrderSelectAlgorithm(Args &&... args)
        : MergeTreeSelectAlgorithm{std::forward<Args>(args)...}
    {
        LOG_TRACE(log, "Reading {} ranges in order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    String getName() const override { return "MergeTreeInOrder"; }

private:
    bool getNewTaskImpl() override;
    void finalizeNewTask() override {}

    Poco::Logger * log = &Poco::Logger::get("MergeTreeInOrderSelectProcessor");
};

}
