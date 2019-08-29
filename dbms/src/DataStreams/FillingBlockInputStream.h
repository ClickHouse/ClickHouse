#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace DB
{

class FillingRow
{
public:
    FillingRow(const SortDescription & sort_description);

    /// Generates next row according to fill 'from', 'to' and 'step' values.
    bool next(const FillingRow & to_row);

    void initFromDefaults(size_t from_pos = 0);
    void initFromColumns(const Columns & columns, size_t row_ind, size_t from_pos);

    Field & operator[](size_t ind) { return row[ind]; }
    const Field & operator[](size_t ind) const { return row[ind]; }
    size_t size() const { return row.size(); }
    bool operator<(const FillingRow & other) const;
    bool operator==(const FillingRow & other) const;

    int getDirection(size_t ind) const { return description[ind].direction; }
    FillColumnDescription & getFillDescription(size_t ind) { return description[ind].fill_description; }

private:
    std::vector<Field> row;
    SortDescription description;
};

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
