#pragma once
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

namespace DB
{

/// Class that stores constant index granularity for whole part, except
/// last non-zero granule and final granule which always has zero rows.
class MergeTreeIndexGranularityConstant final : public MergeTreeIndexGranularity
{
private:
    size_t constant_granularity;
    size_t last_mark_granularity;

    size_t num_marks_without_final = 0;
    bool has_final_mark = false;

    size_t getMarkUpperBoundForRow(size_t row_index) const;

public:
    MergeTreeIndexGranularityConstant() = default;
    explicit MergeTreeIndexGranularityConstant(size_t constant_granularity_);
    MergeTreeIndexGranularityConstant(size_t constant_granularity_, size_t last_mark_granularity_, size_t num_marks_without_final_, bool has_final_mark_);

    std::optional<size_t> getConstantGranularity() const override { return constant_granularity; }
    size_t getRowsCountInRange(size_t begin, size_t end) const override;
    size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const override;
    size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const override;

    size_t getMarksCount() const override;
    size_t getTotalRows() const override;

    size_t getMarkRows(size_t mark_index) const override;
    MarkRange getMarkRangeForRowOffset(size_t row_offset) const override;
    bool hasFinalMark() const override { return has_final_mark; }

    void appendMark(size_t rows_count) override;
    void adjustLastMark(size_t rows_count) override;

    uint64_t getBytesSize() const override { return sizeof(size_t) * 3 + sizeof(bool); }
    uint64_t getBytesAllocated() const override { return getBytesSize(); }

    std::shared_ptr<MergeTreeIndexGranularity> optimize() override { return nullptr; }
    std::string describe() const override;
};

}

