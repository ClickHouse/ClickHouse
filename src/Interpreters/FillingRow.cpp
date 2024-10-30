#include <cstddef>

#include <IO/Operators.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Interpreters/FillingRow.h>


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
    staleness_border.resize(sort_description.size());
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

std::optional<Field> FillingRow::doLongJump(const FillColumnDescription & descr, size_t column_ind, const Field & to)
{
    Field shifted_value = row[column_ind];

    if (less(to, shifted_value, getDirection(column_ind)))
        return std::nullopt;

    for (int32_t step_len = 1, step_no = 0; step_no < 100 && step_len > 0; ++step_no)
    {
        Field next_value = shifted_value;
        descr.step_func(next_value, step_len);

        if (less(to, next_value, getDirection(0)))
        {
            step_len /= 2;
        }
        else
        {
            shifted_value = std::move(next_value);
            step_len *= 2;
        }
    }

    return shifted_value;
}

bool FillingRow::hasSomeConstraints(size_t pos) const
{
    const auto & descr = getFillDescription(pos);

    if (!descr.fill_to.isNull())
        return true;

    if (!descr.fill_staleness.isNull())
        return true;

    return false;
}

bool FillingRow::isConstraintsComplete(size_t pos) const
{
    auto logger = getLogger("FillingRow::isConstraintComplete");
    chassert(!row[pos].isNull());
    chassert(hasSomeConstraints(pos));

    const auto & descr = getFillDescription(pos);
    int direction = getDirection(pos);

    if (!descr.fill_to.isNull() && !less(row[pos], descr.fill_to, direction))
    {
        LOG_DEBUG(logger, "fill to: {}, row: {}, direction: {}", descr.fill_to.dump(), row[pos].dump(), direction);
        return false;
    }

    if (!descr.fill_staleness.isNull() && !less(row[pos], staleness_border[pos], direction))
    {
        LOG_DEBUG(logger, "staleness border: {}, row: {}, direction: {}", staleness_border[pos].dump(), row[pos].dump(), direction);
        return false;
    }

    return true;
}

Field findMin(Field a, Field b, Field c, int dir)
{
    auto logger = getLogger("FillingRow");
    LOG_DEBUG(logger, "a: {} b: {} c: {}", a.dump(), b.dump(), c.dump());

    if (a.isNull() || (!b.isNull() && less(b, a, dir)))
        a = b;

    if (a.isNull() || (!c.isNull() && less(c, a, dir)))
        a = c;

    return a;
}

bool FillingRow::next(const FillingRow & next_original_row, bool& value_changed)
{
    auto logger = getLogger("FillingRow");

    const size_t row_size = size();
    size_t pos = 0;

    /// Find position we need to increment for generating next row.
    for (; pos < row_size; ++pos)
    {
        if (row[pos].isNull())
            continue;

        const auto & descr = getFillDescription(pos);
        auto min_constr = findMin(next_original_row[pos], staleness_border[pos], descr.fill_to, getDirection(pos));
        LOG_DEBUG(logger, "min_constr: {}", min_constr);

        if (!min_constr.isNull() && !equals(row[pos], min_constr))
            break;
    }

    LOG_DEBUG(logger, "pos: {}", pos);

    if (pos == row_size)
        return false;

    const auto & pos_descr = getFillDescription(pos);

    if (!next_original_row[pos].isNull() && less(next_original_row[pos], row[pos], getDirection(pos)))
        return false;

    if (!staleness_border[pos].isNull() && !less(row[pos], staleness_border[pos], getDirection(pos)))
        return false;

    if (!pos_descr.fill_to.isNull() && !less(row[pos], pos_descr.fill_to, getDirection(pos)))
        return false;

    /// If we have any 'fill_to' value at position greater than 'pos' or configured staleness,
    /// we need to generate rows up to one of this borders.
    for (size_t i = row_size - 1; i > pos; --i)
    {
        auto & fill_column_desc = getFillDescription(i);

        if (row[i].isNull())
            continue;

        if (fill_column_desc.fill_to.isNull() && staleness_border[i].isNull())
            continue;

        Field next_value = row[i];
        fill_column_desc.step_func(next_value, 1);

        if (!staleness_border[i].isNull() && !less(next_value, staleness_border[i], getDirection(i)))
            continue;

        if (!fill_column_desc.fill_to.isNull() && !less(next_value, fill_column_desc.fill_to, getDirection(i)))
            continue;

        row[i] = next_value;
        initUsingFrom(i + 1);

        value_changed = true;
        return true;
    }

    auto next_value = row[pos];
    getFillDescription(pos).step_func(next_value, 1);

    if (!next_original_row[pos].isNull() && less(next_original_row[pos], next_value, getDirection(pos)))
        return false;

    if (!staleness_border[pos].isNull() && !less(next_value, staleness_border[pos], getDirection(pos)))
        return false;

    if (!pos_descr.fill_to.isNull() && !less(next_value, pos_descr.fill_to, getDirection(pos)))
        return false;

    row[pos] = next_value;
    if (equals(row[pos], next_original_row[pos]))
    {
        bool is_less = false;
        for (size_t i = pos + 1; i < row_size; ++i)
        {
            const auto & descr = getFillDescription(i);
            if (!descr.fill_from.isNull())
                row[i] = descr.fill_from;
            else
                row[i] = next_original_row[i];

            is_less |= (
                (next_original_row[i].isNull() || less(row[i], next_original_row[i], getDirection(i))) &&
                (staleness_border[i].isNull() || less(row[i], staleness_border[i], getDirection(i))) &&
                (descr.fill_to.isNull() || less(row[i], descr.fill_to, getDirection(i)))
            );
        }

        value_changed = true;
        return is_less;
    }

    initUsingFrom(pos + 1);

    value_changed = true;
    return true;
}

bool FillingRow::shift(const FillingRow & next_original_row, bool& value_changed)
{
    auto logger = getLogger("FillingRow::shift");
    LOG_DEBUG(logger, "next_original_row: {}, current: {}", next_original_row.dump(), dump());

    for (size_t pos = 0; pos < size(); ++pos)
    {
        if (row[pos].isNull() || next_original_row[pos].isNull() || equals(row[pos], next_original_row[pos]))
            continue;

        if (less(next_original_row[pos], row[pos], getDirection(pos)))
            return false;

        std::optional<Field> next_value = doLongJump(getFillDescription(pos), pos, next_original_row[pos]);

        if (!next_value.has_value())
        {
            LOG_DEBUG(logger, "next value: {}", "None");
            continue;
        }
        else
        {
            LOG_DEBUG(logger, "next value: {}", next_value->dump());
        }

        row[pos] = std::move(next_value.value());

        if (equals(row[pos], next_original_row[pos]))
        {
            bool is_less = false;
            for (size_t i = pos + 1; i < size(); ++i)
            {
                const auto & descr = getFillDescription(i);
                if (!descr.fill_from.isNull())
                    row[i] = descr.fill_from;
                else
                    row[i] = next_original_row[i];

                is_less |= (
                    (next_original_row[i].isNull() || less(row[i], next_original_row[i], getDirection(i))) &&
                    (staleness_border[i].isNull() || less(row[i], staleness_border[i], getDirection(i))) &&
                    (descr.fill_to.isNull() || less(row[i], descr.fill_to, getDirection(i)))
                );
            }

            LOG_DEBUG(logger, "is less: {}", is_less);

            value_changed = true;
            return is_less;
        }
        else
        {
            initUsingTo(/*from_pos=*/pos + 1);

            value_changed = false;
            return false;
        }
    }

    return false;
}

bool FillingRow::hasSomeConstraints() const
{
    for (size_t pos = 0; pos < size(); ++pos)
        if (hasSomeConstraints(pos))
            return true;

    return false;
}

bool FillingRow::isConstraintsComplete() const
{
    for (size_t pos = 0; pos < size(); ++pos)
    {
        if (row[pos].isNull() || !hasSomeConstraints(pos))
            continue;

        return isConstraintsComplete(pos);
    }

    return true;
}

void FillingRow::initUsingFrom(size_t from_pos)
{
    for (size_t i = from_pos; i < sort_description.size(); ++i)
        row[i] = getFillDescription(i).fill_from;
}

void FillingRow::initUsingTo(size_t from_pos)
{
    for (size_t i = from_pos; i < sort_description.size(); ++i)
        row[i] = getFillDescription(i).fill_to;
}

void FillingRow::initStalenessRow(const Columns& base_row, size_t row_ind)
{
    for (size_t i = 0; i < size(); ++i)
    {
        const auto& descr = getFillDescription(i);
        if (!descr.fill_staleness.isNull())
        {
            staleness_border[i] = (*base_row[i])[row_ind];
            descr.staleness_step_func(staleness_border[i], 1);
        }
    }
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
