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
    staleness_base_row.resize(sort_description.size());
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

std::optional<Field> FillingRow::doJump(const FillColumnDescription& descr, size_t column_ind)
{
    Field next_value = row[column_ind];
    descr.step_func(next_value, 1);

    if (!descr.fill_to.isNull() && less(descr.fill_to, next_value, getDirection(column_ind)))
        return std::nullopt;

    if (!descr.fill_staleness.isNull())
    {
        Field staleness_border = staleness_base_row[column_ind];
        descr.staleness_step_func(staleness_border, 1);

        if (less(next_value, staleness_border, getDirection(column_ind)))
            return next_value;
        else
            return std::nullopt;
    }

    return next_value;
}

std::optional<Field> FillingRow::doLongJump(const FillColumnDescription & descr, size_t column_ind, const Field & to)
{
    Field shifted_value = row[column_ind];

    if (less(to, shifted_value, getDirection(column_ind)))
        return std::nullopt;

    for (int32_t step_len = 1, step_no = 0; step_no < 100; ++step_no)
    {
        Field next_value = shifted_value;
        descr.step_func(next_value, step_len);

        if (less(next_value, to, getDirection(0)))
        {
            shifted_value = std::move(next_value);
            step_len *= 2;
        }
        else
        {
            step_len /= 2;
        }
    }

    return shifted_value;
}

std::pair<bool, bool> FillingRow::next(const FillingRow & to_row, bool long_jump)
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

        auto next_value = doJump(fill_column_desc, i);
        if (next_value.has_value() && !equals(next_value.value(), fill_column_desc.fill_to))
        {
            row[i] = std::move(next_value.value());
            initFromDefaults(i + 1);
            return {true, true};
        }
    }

    auto & fill_column_desc = getFillDescription(pos);
    std::optional<Field> next_value;

    if (long_jump)
    {
        next_value = doLongJump(fill_column_desc, pos, to_row[pos]);

        if (!next_value.has_value())
            return {false, false};

        Field calibration_jump_value = next_value.value();
        fill_column_desc.step_func(calibration_jump_value, 1);

        if (equals(calibration_jump_value, to_row[pos]))
            next_value = calibration_jump_value;

        if (!next_value.has_value() || less(to_row.row[pos], next_value.value(), getDirection(pos)) || equals(next_value.value(), getFillDescription(pos).fill_to))
            return {false, false};
    }
    else
    {
        next_value = doJump(fill_column_desc, pos);

        if (!next_value.has_value() || less(to_row.row[pos], next_value.value(), getDirection(pos)) || equals(next_value.value(), getFillDescription(pos).fill_to))
            return {false, false};
    }

    row[pos] = std::move(next_value.value());
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

void FillingRow::initStalenessRow(const Columns& base_row, size_t row_ind)
{
    for (size_t i = 0; i < size(); ++i)
        staleness_base_row[i] = (*base_row[i])[row_ind];
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
