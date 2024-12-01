#pragma once
#include <Columns/FilterDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/** Implements [LIMIT INRANGE FROM from_expr TO to_expr, window] operation.
  * Filtering condition columns are added to the block during the Expression transform processor.
  * The bounds are computed considering the existence of the 'window', and the resulting chunks include only rows within these bounds.
  * If remove_filter_column is true, remove filter column from block.
  */
class LimitInRangeTransform : public ISimpleTransform
{
public:
    LimitInRangeTransform(
        const Block & header_, String from_filter_column_name_, String to_filter_column_name_,
        UInt64 limit_inrange_window_, bool remove_filter_column_, bool on_totals_);

    static Block transformHeader(
        Block header, const String & from_filter_column_name, const String & to_filter_column_name,
        UInt64 limit_inrange_window, bool remove_filter_column);

    String getName() const override { return "LimitInRangeTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    String from_filter_column_name;
    String to_filter_column_name;
    UInt64 limit_inrange_window;
    bool remove_filter_column;
    bool on_totals;

    Chunk bufferized_chunk;
    size_t remaining_window = 0;

    ConstantFilterDescription constant_from_filter_description;
    ConstantFilterDescription constant_to_filter_description;
    size_t from_filter_column_position = 0;
    size_t to_filter_column_position = 0;

    bool from_index_found = false;
    bool to_index_found = false;

    /// Header after expression, but before removing filter column.
    Block transformed_header;

    void doFromTransform(Chunk & chunk);
    void doToTransform(Chunk & chunk);
    void doFromAndToTransform(Chunk & chunk);

    void handleFromCase(Chunk & chunk, std::optional<size_t> from_index);
    void handleToCase(Chunk & chunk, std::optional<size_t> from_index, std::optional<size_t> to_index);

    void processChunkFromCaseWithWindow(Chunk & chunk, std::optional<size_t> from_index, std::optional<size_t> to_index);
    void processChunkToCaseWithWindow(Chunk & chunk, size_t start, size_t length, bool use_bufferized = false);

    bool processRemainingWindow(Chunk & chunk);
    void removeFilterIfNeed(Chunk & chunk) const;

    std::optional<size_t> findIndex(Chunk & chunk, size_t column_position, bool & index_found, size_t starting_pos);
};

}
