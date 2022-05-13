#pragma once
#include <Core/SortDescription.h>
#include <Core/InterpolateDescription.h>
#include <Columns/IColumn.h>


namespace DB
{

/// Compares fields in terms of sorting order, considering direction.
bool less(const Field & lhs, const Field & rhs, int direction);
bool equals(const Field & lhs, const Field & rhs);

/** Helps to implement modifier WITH FILL for ORDER BY clause.
 *  Stores row as array of fields and provides functions to generate next row for filling gaps and for comparing rows.
 *  Used in FillingTransform.
 */
class FillingRow
{
public:
    explicit FillingRow(const SortDescription & sort_description);

    /// Generates next row according to fill 'from', 'to' and 'step' values.
    bool next(const FillingRow & to_row);

    void initFromDefaults(size_t from_pos = 0);

    Field & operator[](size_t index) { return row[index]; }
    const Field & operator[](size_t index) const { return row[index]; }
    size_t size() const { return row.size(); }
    bool operator<(const FillingRow & other) const;
    bool operator==(const FillingRow & other) const;

    int getDirection(size_t index) const { return sort_description[index].direction; }
    FillColumnDescription & getFillDescription(size_t index) { return sort_description[index].fill_description; }

private:
    Row row;
    SortDescription sort_description;
};

void insertFromFillingRow(MutableColumns & filling_columns, MutableColumns & interpolate_columns, MutableColumns & other_columns,
    const FillingRow & filling_row, const Block & interpolate_block);
void copyRowFromColumns(MutableColumns & dest, const Columns & source, size_t row_num);

}
