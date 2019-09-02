#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/FillingRow.h>

namespace DB
{

/** Implements the WITH FILL part of ORDER BY operation.
*/
class FillingBlockInputStream : public IBlockInputStream
{
public:
    FillingBlockInputStream(const BlockInputStreamPtr & input, const SortDescription & fill_description_);

    String getName() const override { return "Filling"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    Block createResultBlock(MutableColumns & fill_columns, MutableColumns & other_columns) const;

    const SortDescription sort_description; /// Contains only rows with WITH FILL.
    FillingRow filling_row; /// Current row, which is used to fill gaps.
    FillingRow next_row; /// Row to which we need to generate filling rows.
    Block header;

    using Positions = std::vector<size_t>;
    Positions fill_column_positions;
    Positions other_column_positions;
    bool first = true;
};

}
