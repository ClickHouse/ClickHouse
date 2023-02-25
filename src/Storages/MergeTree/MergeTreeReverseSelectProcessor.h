#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>


namespace DB
{

/// Used to read data from single part with select query
/// in reverse order of primary key.
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeReverseSelectAlgorithm final : public MergeTreeSelectAlgorithm
{
public:
    template <typename... Args>
    explicit MergeTreeReverseSelectAlgorithm(Args &&... args)
        : MergeTreeSelectAlgorithm{std::forward<Args>(args)...}
    {
        LOG_TRACE(log, "Reading {} ranges in reverse order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    String getName() const override { return "MergeTreeReverse"; }

private:
    bool getNewTaskImpl() override;
    void finalizeNewTask() override {}

    bool getNewTaskParallelReplicas();
    bool getNewTaskOrdinaryReading();

    BlockAndProgress readFromPart() override;

    std::vector<BlockAndProgress> chunks;

    /// Used for parallel replicas
    bool no_more_tasks{false};

    Poco::Logger * log = &Poco::Logger::get("MergeTreeReverseSelectProcessor");
};

}
