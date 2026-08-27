#include <algorithm>
#include <cstddef>
#include <limits>

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

std::pair<Field, Field> fillRepresentableRangeOfColumnType(const IDataType & type)
{
    WhichDataType which(type);

    if (which.isDate())
        return {Field(static_cast<UInt64>(0)), Field(static_cast<UInt64>(std::numeric_limits<UInt16>::max()))};

    if (which.isDateTime())
        return {Field(static_cast<UInt64>(0)), Field(static_cast<UInt64>(std::numeric_limits<UInt32>::max()))};

    if (which.isDate32())
        return {Field(static_cast<Int64>(DATE_LUT_MIN_EXTEND_DAY_NUM)), Field(static_cast<Int64>(DATE_LUT_MAX_EXTEND_DAY_NUM))};

    if (which.isDateTime64())
    {
        /// See the comment in `fillValueWithinCalendarRange`: the calendar boundary in raw ticks is shifted by
        /// the UTC offset of the column's time zone, and the last representable time point has all its sub-second
        /// digits set. For a high enough scale the calendar window in ticks is wider than the Int64 storage of
        /// the column, so the window is clamped to the storage range.
        const auto & date_time_type = static_cast<const DataTypeDateTime64 &>(type);
        const auto & time_zone = date_time_type.getTimeZone();
        const UInt32 scale = date_time_type.getScale();
        const Int64 max_seconds = time_zone.makeDateTime(DATE_LUT_MAX_REPRESENTABLE_YEAR, 12, 31, 23, 59, 59);
        const Int64 min_seconds = time_zone.makeDateTime(DATE_LUT_MIN_REPRESENTABLE_YEAR, 1, 1, 0, 0, 0);
        const Int128 scale_multiplier = DecimalUtils::scaleMultiplier<Int128>(scale);
        const Int128 max_ticks = std::min(
            static_cast<Int128>(max_seconds) * scale_multiplier + (scale_multiplier - 1),
            static_cast<Int128>(std::numeric_limits<Int64>::max()));
        const Int128 min_ticks = std::max(
            static_cast<Int128>(min_seconds) * scale_multiplier,
            static_cast<Int128>(std::numeric_limits<Int64>::min()));
        return {
            Field(DecimalField<Decimal64>(static_cast<Int64>(min_ticks), scale)),
            Field(DecimalField<Decimal64>(static_cast<Int64>(max_ticks), scale))};
    }

    if (isInteger(type))
    {
        switch (which.idx)
        {
            case TypeIndex::UInt8:
                return {Field(static_cast<UInt64>(0)), Field(static_cast<UInt64>(std::numeric_limits<UInt8>::max()))};
            case TypeIndex::UInt16:
                return {Field(static_cast<UInt64>(0)), Field(static_cast<UInt64>(std::numeric_limits<UInt16>::max()))};
            case TypeIndex::UInt32:
                return {Field(static_cast<UInt64>(0)), Field(static_cast<UInt64>(std::numeric_limits<UInt32>::max()))};
            case TypeIndex::UInt64:
                return {Field(static_cast<UInt64>(0)), Field(std::numeric_limits<UInt64>::max())};
            case TypeIndex::UInt128:
                return {Field(static_cast<UInt128>(0)), Field(std::numeric_limits<UInt128>::max())};
            case TypeIndex::UInt256:
                return {Field(static_cast<UInt256>(0)), Field(std::numeric_limits<UInt256>::max())};
            case TypeIndex::Int8:
                return {Field(static_cast<Int64>(std::numeric_limits<Int8>::min())), Field(static_cast<Int64>(std::numeric_limits<Int8>::max()))};
            case TypeIndex::Int16:
                return {Field(static_cast<Int64>(std::numeric_limits<Int16>::min())), Field(static_cast<Int64>(std::numeric_limits<Int16>::max()))};
            case TypeIndex::Int32:
                return {Field(static_cast<Int64>(std::numeric_limits<Int32>::min())), Field(static_cast<Int64>(std::numeric_limits<Int32>::max()))};
            case TypeIndex::Int64:
                return {Field(std::numeric_limits<Int64>::min()), Field(std::numeric_limits<Int64>::max())};
            case TypeIndex::Int128:
                return {Field(std::numeric_limits<Int128>::min()), Field(std::numeric_limits<Int128>::max())};
            case TypeIndex::Int256:
                return {Field(std::numeric_limits<Int256>::min()), Field(std::numeric_limits<Int256>::max())};
            default:
                break;
        }
    }

    /// Types that saturate instead of wrapping around (Float, Decimal) and everything else are not checked.
    return {Field(), Field()};
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

    /// Null bounds - a type whose every value is representable, nothing to check.
    if (descr.fill_representable_min.isNull())
        return;

    if (accurateLess(value, descr.fill_representable_min) || accurateLess(descr.fill_representable_max, value))
        throw Exception(
            ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
            "WITH FILL generates the value {} which is out of range of the ORDER BY column {} of type {}",
            applyVisitor(FieldVisitorToString(), value),
            sort_description[column_ind].column_name,
            descr.fill_column_type->getName());
}

void FillingRow::checkStepAdvancesInSortingDirection(const Field & current_value, const Field & next_value, size_t column_ind) const
{
    if (less(current_value, next_value, getDirection(column_ind)))
        return;

    const auto & descr = getFillDescription(column_ind);
    throw Exception(
        ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
        "WITH FILL step does not advance the value {} of the ORDER BY column {} of type {} in the sorting direction: "
        "the next value is {}. This means the sequence wrapped around the range of the column type or stagnated "
        "at a fixed point of the step function, so continuing would generate values the column cannot hold",
        applyVisitor(FieldVisitorToString(), current_value),
        sort_description[column_ind].column_name,
        descr.fill_column_type ? descr.fill_column_type->getName() : "unknown",
        applyVisitor(FieldVisitorToString(), next_value));
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

    /// A NaN border (a NaN `fill_to` or a staleness border derived from a NaN row) generates nothing: `accurateLess`
    /// orders NaN greatest, so the comparison above cannot stop an ascending fill towards it, and the sequence would
    /// run until the step stagnates in the float precision.
    if (constraints[pos].isNaN())
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

        /// A NaN border generates nothing, see the comment at the same check above.
        if (constraints[i].isNaN())
            continue;

        Field next_value = row[i];
        fill_column_desc.step_func(next_value, 1);

        if (!less(next_value, constraints[i], getDirection(i)))
            continue;

        /// Checked only after the constraint cut-off above: a step that fails to advance (wrapped around the
        /// column type, stagnated at a fixed point of the calendar arithmetic or of the float addition, or was
        /// applied to NaN) stops the filling at the constraint the same way it always did, and only a sequence
        /// that would otherwise loop forever is rejected.
        checkStepAdvancesInSortingDirection(row[i], next_value, i);

        checkGeneratedValueFitsColumnType(next_value, i);

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

    /// Checked only after the cut-offs above: a step that fails to advance (wrapped around the column type,
    /// stagnated at a fixed point of the calendar arithmetic or of the float addition, or was applied to NaN)
    /// stops the filling at the border the same way it always did, and only a sequence that would otherwise
    /// loop forever is rejected.
    checkStepAdvancesInSortingDirection(row[pos], next_value, pos);

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

            /// A staleness step cannot advance NaN - keep the border as is (a NaN border makes
            /// `isConstraintsSatisfied` false, so filling just stops) instead of rejecting the query.
            /// An infinite border is not exempted: it makes the fill towards it never terminate,
            /// so failing the advance check below is the right outcome for it.
            if (staleness_border.isNaN())
            {
                constraints[i] = findBorder(descr.fill_to, staleness_border, getDirection(i));
                continue;
            }

            if (!descr.fill_representable_min.isNull()
                && (accurateLess(staleness_border, descr.fill_representable_min)
                    || accurateLess(descr.fill_representable_max, staleness_border)))
                throw Exception(
                    ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL STALENESS border {} of the ORDER BY column {} does not fit the column type {}",
                    applyVisitor(FieldVisitorToString(), staleness_border),
                    sort_description[i].column_name,
                    descr.fill_column_type->getName());

            if (!less((*base_row[i])[row_ind], staleness_border, getDirection(i)))
                throw Exception(
                    ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "WITH FILL STALENESS border {} of the ORDER BY column {} does not advance the value {} "
                    "in the sorting direction",
                    applyVisitor(FieldVisitorToString(), staleness_border),
                    sort_description[i].column_name,
                    applyVisitor(FieldVisitorToString(), (*base_row[i])[row_ind]));

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
