#pragma once
#include <Processors/ISimpleTransform.h>
#include <Columns/FilterDescription.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Implements WHERE, HAVING operations.
  * Takes an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions.
  * The expression is evaluated and result chunks contain only the filtered rows.
  * If remove_filter_column is true, remove filter column from block.
  */
class FilterTransform : public ISimpleTransform
{
public:
    FilterTransform(
        const Block & header_, String filter_column_name_, bool remove_filter_column_, bool on_totals_ = false);

    static Block transformHeader(Block header, const String & filter_column_name, bool remove_filter_column);

    String getName() const override { return "FilterTransform"; }

    Status prepare() override;

protected:
    void transform(Chunk & chunk) override;

private:
    String filter_column_name;
    bool remove_filter_column;
    bool on_totals;

    ConstantFilterDescription constant_filter_description;
    size_t filter_column_position = 0;

    void removeFilterIfNeed(Chunk & chunk) const;
};

}
