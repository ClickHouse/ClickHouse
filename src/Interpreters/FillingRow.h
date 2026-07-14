#pragma once

#include <Core/SortDescription.h>

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
    /// finds last value <= to
    std::optional<Field> doLongJump(const FillColumnDescription & descr, size_t column_ind, const Field & to);

    bool hasSomeConstraints(size_t pos) const;
    bool isConstraintsSatisfied(size_t pos) const;

public:
    explicit FillingRow(const SortDescription & sort_description);

    /// Generates next row according to fill 'from', 'to' and 'step' values.
    /// Returns true if filling values should be inserted into result set
    bool next(const FillingRow & next_original_row, bool& value_changed);

    /// Returns true if need to generate some prefix for to_row
    bool shift(const FillingRow & next_original_row, bool& value_changed);

    bool hasSomeConstraints() const;
    bool isConstraintsSatisfied() const;

    void initUsingFrom(size_t from_pos = 0);
    void initUsingTo(size_t from_pos = 0);
    void updateConstraintsWithStalenessRow(const Columns& base_row, size_t row_ind);

    Field & operator[](size_t index) { return row[index]; }
    const Field & operator[](size_t index) const { return row[index]; }
    size_t size() const { return row.size(); }
    bool operator<(const FillingRow & other) const;
    bool operator==(const FillingRow & other) const;
    bool operator>=(const FillingRow & other) const;
    bool isNull() const;

    int getDirection(size_t index) const { return sort_description[index].direction; }
    FillColumnDescription & getFillDescription(size_t index) { return sort_description[index].fill_description; }
    const FillColumnDescription & getFillDescription(size_t index) const { return sort_description[index].fill_description; }

    String dump() const;

private:
    Row row;
    Row constraints;
    SortDescription sort_description;
};

WriteBuffer & operator<<(WriteBuffer & out, const FillingRow & row);

}
