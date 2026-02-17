#include <cstddef>

#include <Columns/IColumn.h>
#include <IO/Operators.h>
#include <Interpreters/FillingRow.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

constexpr static bool debug_logging_enabled = false;

template <class... Args>
inline static void logDebug(const char * fmt_str, Args&&... args)
{
    if constexpr (debug_logging_enabled)
        LOG_DEBUG(getLogger("FillingRow"), "{}", fmt::format(fmt::runtime(fmt_str), std::forward<Args>(args)...));
}

bool less(const Field & lhs, const Field & rhs, int direction)
{
    if (direction == -1)
        return accurateLess(rhs, lhs);

    return accurateLess(lhs, rhs);
}

bool equals(const Field & lhs, const Field & rhs)
{
    /// This will treat NaNs as equal
    if (lhs.getType() == rhs.getType())
        return lhs == rhs;

    return accurateEquals(lhs, rhs);
}


FillingRow::FillingRow(const SortDescription & sort_description_)
    : sort_description(sort_description_)
{
    row.resize(sort_description.size());

    constraints.reserve(sort_description.size());
    for (size_t i = 0; i < size(); ++i)
        constraints.push_back(getFillDescription(i).fill_to);
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
    return !constraints[pos].isNull();
}

bool FillingRow::isConstraintsSatisfied(size_t pos) const
{
    chassert(!row[pos].isNull());
    chassert(hasSomeConstraints(pos));

    int direction = getDirection(pos);
    logDebug("constraint: {}, row: {}, direction: {}", constraints[pos], row[pos], direction);

    return less(row[pos], constraints[pos], direction);
}

static const Field & findBorder(const Field & constraint, const Field & next_original, int direction)
{
    if (constraint.isNull())
        return next_original; /// NOLINT(bugprone-return-const-ref-from-parameter)

    if (next_original.isNull())
        return constraint; /// NOLINT(bugprone-return-const-ref-from-parameter)

    if (less(constraint, next_original, direction))
        return constraint; /// NOLINT(bugprone-return-const-ref-from-parameter)

    return next_original; /// NOLINT(bugprone-return-const-ref-from-parameter)
}

bool FillingRow::next(const FillingRow & next_original_row, bool& value_changed)
{

    const size_t row_size = size();
    size_t pos = 0;

    /// Find position we need to increment for generating next row.
    for (; pos < row_size; ++pos)
    {
        if (row[pos].isNull())
            continue;

        const Field & border = findBorder(constraints[pos], next_original_row[pos], getDirection(pos));
        logDebug("border: {}", border);

        if (!border.isNull() && !equals(row[pos], border))
            break;
    }

    logDebug("pos: {}", pos);

    if (pos == row_size)
        return false;

    if (!next_original_row[pos].isNull() && less(next_original_row[pos], row[pos], getDirection(pos)))
        return false;

    if (!constraints[pos].isNull() && !less(row[pos], constraints[pos], getDirection(pos)))
        return false;

    /// If we have any 'fill_to' value at position greater than 'pos' or configured staleness,
    /// we need to generate rows up to one of this borders.
    for (size_t i = row_size - 1; i > pos; --i)
    {
        auto & fill_column_desc = getFillDescription(i);

        if (row[i].isNull())
            continue;

        if (constraints[i].isNull())
            continue;

        Field next_value = row[i];
        fill_column_desc.step_func(next_value, 1);

        if (!less(next_value, constraints[i], getDirection(i)))
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

    if (!constraints[pos].isNull() && !less(next_value, constraints[pos], getDirection(pos)))
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
                (constraints[i].isNull() || less(row[i], constraints[i], getDirection(i)))
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
    logDebug("next_original_row: {}, current: {}", next_original_row, *this);

    for (size_t pos = 0; pos < size(); ++pos)
    {
        if (row[pos].isNull() || next_original_row[pos].isNull() || equals(row[pos], next_original_row[pos]))
            continue;

        if (less(next_original_row[pos], row[pos], getDirection(pos)))
            return false;

        std::optional<Field> next_value = doLongJump(getFillDescription(pos), pos, next_original_row[pos]);
        logDebug("jumped to next value: {}", next_value.value_or("Did not complete"));

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
                    (constraints[i].isNull() || less(row[i], constraints[i], getDirection(i)))
                );
            }

            logDebug("is less: {}", is_less);

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

bool FillingRow::isConstraintsSatisfied() const
{
    for (size_t pos = 0; pos < size(); ++pos)
    {
        if (row[pos].isNull() || !hasSomeConstraints(pos))
            continue;

        return isConstraintsSatisfied(pos);
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

void FillingRow::updateConstraintsWithStalenessRow(const Columns& base_row, size_t row_ind)
{
    for (size_t i = 0; i < size(); ++i)
    {
        const auto& descr = getFillDescription(i);

        if (!descr.fill_staleness.isNull())
        {
            Field staleness_border = (*base_row[i])[row_ind];
            descr.staleness_step_func(staleness_border, 1);
            constraints[i] = findBorder(descr.fill_to, staleness_border, getDirection(i));
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

template <>
struct fmt::formatter<DB::FillingRow> : fmt::formatter<string_view>
{
    constexpr auto format(const DB::FillingRow & row, format_context & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", row.dump());
    }
};
