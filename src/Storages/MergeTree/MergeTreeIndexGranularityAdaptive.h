#pragma once
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

#include <vector>

namespace DB
{

/// Class that stores adaptive index granularity.
/// Inside it contains vector of partial sums of rows after mark:
/// |-----|---|----|----|
/// |  5  | 8 | 12 | 16 |
class MergeTreeIndexGranularityAdaptive final : public MergeTreeIndexGranularity
{
public:
    MergeTreeIndexGranularityAdaptive() = default;
    explicit MergeTreeIndexGranularityAdaptive(const std::vector<size_t> & marks_rows_partial_sums_);

    [[nodiscard]] std::optional<size_t> getConstantGranularity() const override { return {}; }
    [[nodiscard]] size_t getRowsCountInRange(size_t begin, size_t end) const override;
    [[nodiscard]] size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const override;
    [[nodiscard]] size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const override;

    [[nodiscard]] size_t getMarksCount() const override;
    [[nodiscard]] size_t getTotalRows() const override;

    [[nodiscard]] size_t getMarkRows(size_t mark_index) const override;
    [[nodiscard]] MarkRange getMarkRangeForRowOffset(size_t row_offset) const override;
    [[nodiscard]] bool hasFinalMark() const override;

    void appendMark(size_t rows_count) override;
    void adjustLastMark(size_t rows_count) override;

    [[nodiscard]] uint64_t getBytesSize() const override;
    [[nodiscard]] uint64_t getBytesAllocated() const override;

    std::shared_ptr<MergeTreeIndexGranularity> optimize() override;
    [[nodiscard]] std::string describe() const override;
    [[nodiscard]] std::shared_ptr<MergeTreeIndexGranularity> clone() const override { return std::make_shared<MergeTreeIndexGranularityAdaptive>(*this); }

private:
    std::vector<size_t> marks_rows_partial_sums;
};

}

