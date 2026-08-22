#include <cstddef>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <Interpreters/FillingRow.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/FieldVisitorToString.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

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

bool fillValueWithinCalendarRange(const Field & value, const IDataType & type)
{
    /// For Date32 and DateTime64 the boundaries of the representable calendar lie strictly inside the range of
    /// the storage type, and the values between the two are invalid: nothing else produces them (conversions
    /// clamp at the calendar boundary), the serializers render them as the clamped boundary date, and the
    /// calendar arithmetic of an INTERVAL step clamps back into the calendar from them. (For Date and DateTime
    /// the whole storage range maps into the calendar, so there is nothing to check there.)
    WhichDataType which(type);

    if (which.isDate32() && value.getType() == Field::Types::Int64)
    {
        const Int64 day_num = value.safeGet<Int64>();
        return day_num >= DATE_LUT_MIN_EXTEND_DAY_NUM && day_num <= DATE_LUT_MAX_EXTEND_DAY_NUM;
    }

    if (which.isDateTime64() && value.getType() == Field::Types::Decimal64)
    {
        /// The calendar clamps in the local civil calendar of the column's time zone (the local year is clamped
        /// to [0000, 9999], see e.g. DateLUTImpl::addYearsOutOfRange), so the boundary expressed in raw ticks is
        /// shifted by the UTC offset of the time zone: the last representable second is the raw value of local
        /// 9999-12-31 23:59:59, not of the same instant in UTC.
        const auto & time_zone = static_cast<const DataTypeDateTime64 &>(type).getTimeZone();
        const Int64 max_seconds = time_zone.makeDateTime(DATE_LUT_MAX_REPRESENTABLE_YEAR, 12, 31, 23, 59, 59);
        const Int64 min_seconds = time_zone.makeDateTime(DATE_LUT_MIN_REPRESENTABLE_YEAR, 1, 1, 0, 0, 0);
        const auto & decimal = value.safeGet<DecimalField<Decimal64>>();
        const Int128 scale_multiplier = DecimalUtils::scaleMultiplier<Int128>(decimal.getScale());
        /// The last representable time point of the calendar has all its sub-second digits set.
        const Int128 max_ticks = static_cast<Int128>(max_seconds) * scale_multiplier + (scale_multiplier - 1);
        const Int128 min_ticks = static_cast<Int128>(min_seconds) * scale_multiplier;
        const Int128 ticks = decimal.getValue().value;
        return ticks >= min_ticks && ticks <= max_ticks;
    }

    return true;
}

bool fillValueFitsColumnType(const Field & value, const IDataType & type)
{
    /// Only integers wrap around. Float values saturate, and they are inexact for a narrower column type anyway,
    /// so an exact-representability check would reject ordinary queries. Decimal conversion throws on overflow.
    WhichDataType which(type);
    if (!isInteger(type) && !which.isDate() && !which.isDate32() && !which.isDateTime() && !which.isDateTime64())
        return true;

    /// `convertFieldToType` returns Null for a value that is out of range of the target storage type.
    return !convertFieldToType(value, type).isNull() && fillValueWithinCalendarRange(value, type);
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

Field FillingRow::doLongJump(const FillColumnDescription & descr, size_t column_ind, const Field & to)
{
    Field shifted_value = row[column_ind];

    int64_t step_len = 1;
    int64_t step_no = 0;
    for (; step_no < 500 && step_len > 0; ++step_no)
    {
        Field next_value = shifted_value;
        descr.step_func(next_value, step_len);

        int direction = getDirection(column_ind);
        bool overflowed = less(next_value, shifted_value, direction);
        logDebug("doLongJump: shifted_value: {}, next_value: {}, to: {}, step_no: {}, step_len: {}", shifted_value, next_value, to, step_no, step_len);
        if (overflowed || less(to, next_value, direction))
        {
            step_len /= 2;
        }
        else
        {
            shifted_value = std::move(next_value);
            step_len = step_len <= INT64_MAX/2 ? step_len * 2 : step_len;
        }
    }

    logDebug("doLongJump: {} (step_no: {}, step_len: {})", shifted_value, step_no, step_len);
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

/** The arithmetic of filling is performed in a carrier type wide enough for it - Int64 for every integer column
  * type - while the generated values are written into a column of the column's own type, which silently truncates
  * whatever does not fit. Bounds known up front are checked when the transform is constructed, but a fill anchored
  * at a data value only becomes known here: `WITH FILL TO 257 STEP 3` over a UInt8 column stops at 254 when the
  * data ends at 11 and reaches 256 - which the column would store as 0 - when it ends at 13. Check every value
  * that is about to be generated, so that no value the column cannot hold ever reaches it.
  */
void FillingRow::checkGeneratedValueFitsColumnType(const Field & value, size_t column_ind) const
{
    const auto & descr = getFillDescription(column_ind);

    if (descr.fill_column_type && !fillValueFitsColumnType(value, *descr.fill_column_type))
        throw Exception(
            ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
            "WITH FILL generates the value {} which is out of range of the ORDER BY column type {}",
            applyVisitor(FieldVisitorToString(), value),
            descr.fill_column_type->getName());
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

    if (row[pos].isNaN() || row[pos].isInf())
        return false;

    if (next_original_row[pos].isNaN() || next_original_row[pos].isInf())
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

        if (!less(row[i], next_value, getDirection(i)))
            throw Exception(
                ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL step does not advance in the sorting direction");

        if (!less(next_value, constraints[i], getDirection(i)))
            continue;

        checkGeneratedValueFitsColumnType(next_value, i);

        row[i] = next_value;
        initUsingFrom(i + 1);

        value_changed = true;
        return true;
    }

    auto next_value = row[pos];
    getFillDescription(pos).step_func(next_value, 1);

    if (!less(row[pos], next_value, getDirection(pos)))
        throw Exception(
            ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
            "WITH FILL step does not advance in the sorting direction");

    if (!next_original_row[pos].isNull() && less(next_original_row[pos], next_value, getDirection(pos)))
        return false;

    if (!constraints[pos].isNull() && !less(next_value, constraints[pos], getDirection(pos)))
        return false;

    checkGeneratedValueFitsColumnType(next_value, pos);

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

        Field next_value = doLongJump(getFillDescription(pos), pos, next_original_row[pos]);

        row[pos] = std::move(next_value);

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

            if (!fillValueFitsColumnType(staleness_border, *descr.fill_column_type))
                throw Exception(
                    ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL STALENESS bound does not fit the column type");

            if (!less((*base_row[i])[row_ind], staleness_border, getDirection(i)))
                throw Exception(
                    ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL STALENESS does not advance in the sorting direction");

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
