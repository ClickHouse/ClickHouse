#pragma once
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>


namespace DB
{

/// Used to read data from single part with select query
/// in reverse order of primary key.
/// Cares about PREWHERE, virtual columns, indexes etc.
/// To read data from multiple parts, Storage (MergeTree) creates multiple such objects.
class MergeTreeReverseSelectProcessor final : public MergeTreeSelectProcessor
{
public:
    template <typename... Args>
    explicit MergeTreeReverseSelectProcessor(Args &&... args)
        : MergeTreeSelectProcessor{std::forward<Args>(args)...}
    {
        LOG_TRACE(log, "Reading {} ranges in reverse order from part {}, approx. {} rows starting from {}",
            all_mark_ranges.size(), data_part->name, total_rows,
            data_part->index_granularity.getMarkStartingRow(all_mark_ranges.front().begin));
    }

    String getName() const override { return "MergeTreeReverse"; }

private:
    bool getNewTaskImpl() override;
    void finalizeNewTask() override {}

    BlockAndRowCount readFromPart() override;

    std::vector<BlockAndRowCount> chunks;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeReverseSelectProcessor");
};

}
