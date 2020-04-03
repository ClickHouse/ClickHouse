#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/SetVariants.h>

namespace DB
{

/** This class is intended for implementation of SELECT DISTINCT clause and
  * leaves only unique rows in the stream.
  *
  * To optimize the SELECT DISTINCT ... LIMIT clause we can
  * set limit_hint to non zero value. So we stop emitting new rows after
  * count of already emitted rows will reach the limit_hint.
  */
class DistinctBlockInputStream : public IBlockInputStream
{
public:
    /// Empty columns_ means all collumns.
    DistinctBlockInputStream(const BlockInputStreamPtr & input, const SizeLimits & set_size_limits_, UInt64 limit_hint_, const Names & columns_);

    String getName() const override { return "Distinct"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    ColumnRawPtrs getKeyColumns(const Block & block) const;

    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants) const;


    Names columns_names;
    SetVariants data;
    Sizes key_sizes;
    UInt64 limit_hint;

    bool no_more_rows = false;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;
};

}
