#include <Interpreters/FillingRow.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <IO/Operators.h>


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
    /// This will treat NaNs as equal
    if (lhs.getType() == rhs.getType())
        return lhs == rhs;

    return applyVisitor(FieldVisitorAccurateEquals(), lhs, rhs);
}


FillingRow::FillingRow(const SortDescription & sort_description_)
    : sort_description(sort_description_)
{
    row.resize(sort_description.size());
}

bool FillingRow::operator<(const FillingRow & other) const
{
    for (size_t i = 0; i < sort_description.size(); ++i)
    {
        if ((*this)[i].isNull() || other.row[i].isNull() || equals(row[i], other.row[i]))
            continue;
        return less(row[i], other.row[i], getDirection(i));
    }
    return false;
}

bool FillingRow::operator==(const FillingRow & other) const
{
    for (size_t i = 0; i < sort_description.size(); ++i)
        if (!equals(row[i], other.row[i]))
            return false;
    return true;
}

bool FillingRow::operator>=(const FillingRow & other) const
{
    return !(*this < other);
}

bool FillingRow::isNull() const
{
    for (const auto & field : row)
        if (!field.isNull())
            return false;

    return true;
}

std::pair<bool, bool> FillingRow::next(const FillingRow & to_row)
{
    const size_t row_size = size();
    size_t pos = 0;

    /// Find position we need to increment for generating next row.
    for (; pos < row_size; ++pos)
        if (!row[pos].isNull() && !to_row.row[pos].isNull() && !equals(row[pos], to_row.row[pos]))
            break;

    if (pos == row_size || less(to_row.row[pos], row[pos], getDirection(pos)))
        return {false, false};

    /// If we have any 'fill_to' value at position greater than 'pos',
    ///  we need to generate rows up to 'fill_to' value.
    for (size_t i = row_size - 1; i > pos; --i)
    {
        auto & fill_column_desc = getFillDescription(i);

        if (fill_column_desc.fill_to.isNull() || row[i].isNull())
            continue;

        Field next_value = row[i];
        fill_column_desc.step_func(next_value);
        if (less(next_value, fill_column_desc.fill_to, getDirection(i)))
        {
            row[i] = next_value;
            initFromDefaults(i + 1);
            return {true, true};
        }
    }

    auto next_value = row[pos];
    getFillDescription(pos).step_func(next_value);

    if (less(to_row.row[pos], next_value, getDirection(pos)) || equals(next_value, getFillDescription(pos).fill_to))
        return {false, false};

    row[pos] = next_value;
    if (equals(row[pos], to_row.row[pos]))
    {
        bool is_less = false;
        for (size_t i = pos + 1; i < row_size; ++i)
        {
            const auto & fill_from = getFillDescription(i).fill_from;
            if (!fill_from.isNull())
                row[i] = fill_from;
            else
                row[i] = to_row.row[i];
            is_less |= less(row[i], to_row.row[i], getDirection(i));
        }

        return {is_less, true};
    }

    initFromDefaults(pos + 1);
    return {true, true};
}

void FillingRow::initFromDefaults(size_t from_pos)
{
    for (size_t i = from_pos; i < sort_description.size(); ++i)
        row[i] = getFillDescription(i).fill_from;
}

String FillingRow::dump() const
{
    WriteBufferFromOwnString out;
    for (size_t i = 0; i < row.size(); ++i)
    {
        if (i != 0)
            out << ", ";
        out << row[i].dump();
    }
    return out.str();
}

WriteBuffer & operator<<(WriteBuffer & out, const FillingRow & row)
{
    out << row.dump();
    return out;
}

}
