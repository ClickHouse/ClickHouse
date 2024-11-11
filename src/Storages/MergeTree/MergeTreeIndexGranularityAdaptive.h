#pragma once
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

namespace DB
{

class MergeTreeIndexGranularityAdaptive : public MergeTreeIndexGranularity
{
public:
    MergeTreeIndexGranularityAdaptive() = default;
    explicit MergeTreeIndexGranularityAdaptive(const std::vector<size_t> & marks_rows_partial_sums_);

    size_t getRowsCountInRange(size_t begin, size_t end) const override;
    size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const override;
    size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const override;

    size_t getMarksCount() const override;
    size_t getTotalRows() const override;

    size_t getMarkRows(size_t mark_index) const override;
    size_t getMarkStartingRow(size_t mark_index) const override;
    bool hasFinalMark() const override;

    void appendMark(size_t rows_count) override;
    void adjustLastMark(size_t rows_count) override;
    void shrinkToFitInMemory() override;
    std::shared_ptr<MergeTreeIndexGranularity> optimize() const override;

    std::string describe() const override;

private:
    std::vector<size_t> marks_rows_partial_sums;
};

}

