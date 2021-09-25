#include <Interpreters/FillingRow.h>
#include <Common/FieldVisitorSum.h>
#include <Common/FieldVisitorsAccurateComparison.h>


namespace DB
{

bool less(const Field & lhs, const Field & rhs, int direction)
{
    if (direction == -1)
        return applyVisitor(FieldVisitorAccurateLess(), rhs, lhs);

    return applyVisitor(FieldVisitorAccurateLess(), lhs, rhs);
}

bool equals(const Field & lhs, const Field & rhs)
{
    return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs);
}


FillingRow::FillingRow(const SortDescription & sort_description) : description(sort_description)
{
    row.resize(description.size());
}

bool FillingRow::operator<(const FillingRow & other) const
{
    for (size_t i = 0; i < size(); ++i)
    {
        if (row[i].isNull() || other[i].isNull() || equals(row[i], other[i]))
            continue;
        return less(row[i], other[i], getDirection(i));
    }
    return false;
}

bool FillingRow::operator==(const FillingRow & other) const
{
    for (size_t i = 0; i < size(); ++i)
        if (!equals(row[i], other[i]))
            return false;
    return true;
}

bool FillingRow::next(const FillingRow & to_row)
{
    size_t pos = 0;

    /// Find position we need to increment for generating next row.
    for (; pos < row.size(); ++pos)
        if (!row[pos].isNull() && !to_row[pos].isNull() && !equals(row[pos], to_row[pos]))
            break;

    if (pos == row.size() || less(to_row[pos], row[pos], getDirection(pos)))
        return false;

    /// If we have any 'fill_to' value at position greater than 'pos',
    ///  we need to generate rows up to 'fill_to' value.
    for (size_t i = row.size() - 1; i > pos; --i)
    {
        if (getFillDescription(i).fill_to.isNull() || row[i].isNull())
            continue;

        auto next_value = row[i];
        applyVisitor(FieldVisitorSum(getFillDescription(i).fill_step), next_value);
        if (less(next_value, getFillDescription(i).fill_to, getDirection(i)))
        {
            row[i] = next_value;
            initFromDefaults(i + 1);
            return true;
        }
    }

    auto next_value = row[pos];
    applyVisitor(FieldVisitorSum(getFillDescription(pos).fill_step), next_value);

    if (less(to_row[pos], next_value, getDirection(pos)))
        return false;

    row[pos] = next_value;
    if (equals(row[pos], to_row[pos]))
    {
        bool is_less = false;
        for (size_t i = pos + 1; i < size(); ++i)
        {
            const auto & fill_from = getFillDescription(i).fill_from;
            if (!fill_from.isNull())
                row[i] = fill_from;
            else
                row[i] = to_row[i];
            is_less |= less(row[i], to_row[i], getDirection(i));
        }

        return is_less;
    }

    initFromDefaults(pos + 1);
    return true;
}

void FillingRow::initFromDefaults(size_t from_pos)
{
    for (size_t i = from_pos; i < row.size(); ++i)
        row[i] = getFillDescription(i).fill_from;
}


void insertFromFillingRow(MutableColumns & filling_columns, MutableColumns & other_columns, const FillingRow & filling_row)
{
    for (size_t i = 0; i < filling_columns.size(); ++i)
    {
        if (filling_row[i].isNull())
            filling_columns[i]->insertDefault();
        else
            filling_columns[i]->insert(filling_row[i]);
    }

    for (const auto & other_column : other_columns)
        other_column->insertDefault();
}

void copyRowFromColumns(MutableColumns & dest, const Columns & source, size_t row_num)
{
    for (size_t i = 0; i < source.size(); ++i)
        dest[i]->insertFrom(*source[i], row_num);
}

}
