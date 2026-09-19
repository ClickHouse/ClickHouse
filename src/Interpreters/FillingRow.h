#pragma once

#include <Core/SortDescription.h>

namespace DB
{

/// Compares fields in terms of sorting order, considering direction.
bool less(const Field & lhs, const Field & rhs, int direction);
bool equals(const Field & lhs, const Field & rhs);

/// Whether `value` lies within the window of the calendar representable by the type - the `[0000-01-01, 9999-12-31]`
/// window of `DateLUTImpl`, taken in the local civil calendar of the column's time zone. Only `Date32` and
/// `DateTime64` have a calendar window narrower than their storage type; for every other type the answer is `true`.
bool fillValueWithinCalendarRange(const Field & value, const IDataType & type);

/// Whether `value` is a valid value of a fill column of type `type`: it fits the range of the storage type and,
/// for the calendar-backed `Date32` and `DateTime64`, the representable calendar window as well. Types that
/// saturate instead of wrapping around (`Float`, `Decimal`) are not checked and always pass.
bool fillValueFitsColumnType(const Field & value, const IDataType & type);

/// The inclusive `[min, max]` range that `fillValueFitsColumnType` checks against, as `Field`s comparable with
/// the values the filling generates, so that the per-generated-value check does not have to redo the type
/// dispatch. Null bounds mean the type is not checked and every value passes.
std::pair<Field, Field> fillRepresentableRangeOfColumnType(const IDataType & type);

/** Helps to implement modifier WITH FILL for ORDER BY clause.
 *  Stores row as array of fields and provides functions to generate next row for filling gaps and for comparing rows.
 *  Used in FillingTransform.
 */
class FillingRow
{
    /// finds last value <= to
    Field doLongJump(const FillColumnDescription & descr, size_t column_ind, const Field & to);

    void checkGeneratedValueFitsColumnType(const Field & value, size_t column_ind) const;

    /// Throws when stepping from `current_value` produced a `next_value` that is not strictly further in the
    /// sorting direction - the sequence wrapped around the column type or stagnated at a fixed point of the step
    /// function, so continuing would generate garbage or hang.
    void checkStepAdvancesInSortingDirection(const Field & current_value, const Field & next_value, size_t column_ind) const;

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
