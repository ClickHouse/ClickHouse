#pragma once
#include <Processors/ISimpleTransform.h>
#include <Columns/FilterDescription.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

/** Implements WHERE, HAVING operations.
  * Takes an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions.
  * The expression is evaluated and result chunks contain only the filtered rows.
  * If remove_filter_column is true, remove filter column from block.
  */
class FilterTransform : public ISimpleTransform
{
public:
    FilterTransform(
        const Block & header_, ExpressionActionsPtr expression_, String filter_column_name_,
        bool remove_filter_column_, bool on_totals_ = false, std::shared_ptr<std::atomic<size_t>> rows_filtered_ = nullptr);

    static Block
    transformHeader(const Block & header, const ActionsDAG * expression, const String & filter_column_name, bool remove_filter_column);

    String getName() const override { return "FilterTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    String filter_column_name;
    bool remove_filter_column;
    bool on_totals;

    ConstantFilterDescription constant_filter_description;
    size_t filter_column_position = 0;

    std::shared_ptr<std::atomic<size_t>> rows_filtered;

    /// Header after expression, but before removing filter column.
    Block transformed_header;

    bool are_prepared_sets_initialized = false;

    void doTransform(Chunk & chunk);
    void removeFilterIfNeed(Columns & columns) const;
};

}
