#pragma once
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

namespace DB
{

/// Class that stores constant index granularity for whole part, except
/// last non-zero granule and final granule which always has zero rows.
class MergeTreeIndexGranularityConstant final : public MergeTreeIndexGranularity
{
private:
    size_t constant_granularity{};
    size_t last_mark_granularity{};

    size_t num_marks_without_final = 0;
    bool has_final_mark = false;

    [[nodiscard]] size_t getMarkUpperBoundForRow(size_t row_index) const;

public:
    MergeTreeIndexGranularityConstant() = default;
    explicit MergeTreeIndexGranularityConstant(size_t constant_granularity_);
    MergeTreeIndexGranularityConstant(size_t constant_granularity_, size_t last_mark_granularity_, size_t num_marks_without_final_, bool has_final_mark_);

    [[nodiscard]] std::optional<size_t> getConstantGranularity() const override { return constant_granularity; }
    [[nodiscard]] size_t getRowsCountInRange(size_t begin, size_t end) const override;
    [[nodiscard]] size_t countMarksForRows(size_t from_mark, size_t number_of_rows) const override;
    [[nodiscard]] size_t countRowsForRows(size_t from_mark, size_t number_of_rows, size_t offset_in_rows) const override;

    [[nodiscard]] size_t getMarksCount() const override;
    [[nodiscard]] size_t getTotalRows() const override;

    [[nodiscard]] size_t getMarkRows(size_t mark_index) const override;
    [[nodiscard]] MarkRange getMarkRangeForRowOffset(size_t row_offset) const override;
    [[nodiscard]] bool hasFinalMark() const override { return has_final_mark; }

    void appendMark(size_t rows_count) override;
    void adjustLastMark(size_t rows_count) override;

    /// Fix the last mark granularity and final mark from the actual row count.
    /// When loaded from non-adaptive mark files, all marks are treated as data marks
    /// with constant granularity, and has_final_mark is set to false.
    /// This method detects the final mark and adjusts the last data mark's granularity.
    void fixFromRowsCount(size_t rows_count);

    /// Same as fixFromRowsCount but returns a new object, leaving this one unchanged.
    /// Use this when the current object may be shared with concurrent readers.
    [[nodiscard]] std::shared_ptr<MergeTreeIndexGranularityConstant> fixedFromRowsCount(size_t rows_count) const;

    [[nodiscard]] uint64_t getBytesSize() const override { return sizeof(size_t) * 3 + sizeof(bool); }
    [[nodiscard]] uint64_t getBytesAllocated() const override { return getBytesSize(); }

    std::shared_ptr<MergeTreeIndexGranularity> optimize() override { return nullptr; }
    [[nodiscard]] std::string describe() const override;
    [[nodiscard]] std::shared_ptr<MergeTreeIndexGranularity> clone() const override { return std::make_shared<MergeTreeIndexGranularityConstant>(*this); }
};

}

