#pragma once
#include <Columns/FilterDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/** Implements [LIMIT INRANGE FROM from_expr TO to_expr] operation.
  * Takes from_expr and to_expr, which add to the block two ColumnUInt8 columns containing the filtering conditions.
  * The expression is evaluated and result chunks contain only the filtered rows.
  * If remove_filter_column is true, remove filter column from block.
  */
class LimitInRangeTransform : public ISimpleTransform
{
public:
    LimitInRangeTransform(
        const Block & header_,
        String from_filter_column_name_,
        String to_filter_column_name_,
        bool remove_filter_column_,
        bool on_totals_);

    static Block
    transformHeader(Block header, const String & from_filter_column_name, const String & to_filter_column_name, bool remove_filter_column);

    String getName() const override { return "LimitInRangeTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    String from_filter_column_name;
    String to_filter_column_name;
    bool remove_filter_column;
    bool on_totals;

    ConstantFilterDescription constant_from_filter_description;
    ConstantFilterDescription constant_to_filter_description;
    size_t from_filter_column_position = 0;
    size_t to_filter_column_position = 0;

    bool from_index_found = false;
    bool to_index_found = false;
    /// Header after expression, but before removing filter column.
    Block transformed_header;

    // bool are_prepared_sets_initialized = false;

    void doFromTransform(Chunk & chunk);
    void doToTransform(Chunk & chunk);
    void doFromAndToTransform(Chunk & chunk);
    void removeFilterIfNeed(Chunk & chunk) const;
};

}
