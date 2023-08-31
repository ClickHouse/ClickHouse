#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <array>
#include <cassert>
#include <chrono>
#include <concepts>
#include <span>
#include <string_view>
#include <tuple>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

using simple_year = std::chrono::duration<int64_t, std::ratio<365L * 24L * 60L * 60L, 1L>>; // 365 days
using simple_month = std::chrono::duration<int64_t, std::ratio<(30L * 24L + 12L) * 60L * 60L, 1L>>; // 30.5 days


// This is a helper type for various chrono lengths (and it can't be inside the FunctionFormatReadableTimeDelta class)
struct Unit
{
    int num;
    int denom;

    std::string_view name;

    friend bool operator==(const Unit & lhs, const Unit & rhs)
    {
        return std::tie(lhs.num, lhs.denom) == std::tie(rhs.num, rhs.denom);
    }

    friend bool operator==(const Unit & lhs, std::string_view needle)
    {
        return lhs.name == needle;
    }

    UInt64 convertToUnit(std::chrono::duration<Float64> & input) const noexcept
    {
        return static_cast<UInt64>(input.count() / num * denom);
    }

    std::chrono::duration<Float64> convertFromUnit(UInt64 count) const noexcept
    {
        return std::chrono::duration<Float64>{static_cast<Float64>(count) * num / denom};
    }

    template <typename T> static consteval std::string_view getUnitName()
    {
        if constexpr (std::same_as<simple_year, T>)
        {
            return "year";
        }
        else if constexpr (std::same_as<simple_month, T>)
        {
            return "month";
        }
        else if constexpr (std::same_as<std::chrono::days, T>)
        {
            return "day";
        }
        else if constexpr (std::same_as<std::chrono::hours, T>)
        {
            return "hour";
        }
        else if constexpr (std::same_as<std::chrono::minutes, T>)
        {
            return "minute";
        }
        else if constexpr (std::same_as<std::chrono::seconds, T>)
        {
            return "second";
        }
        else if constexpr (std::same_as<std::chrono::milliseconds, T>)
        {
            return "millisecond";
        }
        else if constexpr (std::same_as<std::chrono::microseconds, T>)
        {
            return "microsecond";
        }
        else // if constexpr (std::same_as<std::chrono::nanoseconds, T>)
        {
            static_assert(std::same_as<std::chrono::nanoseconds, T>);
            return "nanosecond";
        }
    }

    template <typename T, long Num, long Den> consteval Unit(std::chrono::duration<T, std::ratio<Num, Den>>):
        num{static_cast<int>(Num)},
        denom{static_cast<int>(Den)},
        name{getUnitName<std::chrono::duration<T, std::ratio<Num, Den>>>()} { }
};

/** Prints amount of seconds in form of:
  * "1 year, 2 months, 12 days, 3 hours, 1 minute and 33 seconds".
  * Maximum unit can be specified as a second argument: for example, you can specify "days",
  * and it will avoid using years and months.
  *
  * The length of years and months (and even days in presence of time adjustments) are rough:
  * year is just 365 days, month is 30.5 days, day is 86400 seconds.
  *
  * You may think that the choice of constants and the whole purpose of this function is very ignorant...
  * And you're right. But actually it's made similar to a random Python library from the internet:
  * https://github.com/jmoiron/humanize/blob/b37dc30ba61c2446eecb1a9d3e9ac8c9adf00f03/src/humanize/time.py#L462
  */
class FunctionFormatReadableTimeDelta : public IFunction
{
public:
    static constexpr auto name = "formatReadableTimeDelta";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatReadableTimeDelta>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 1.",
                getName(), arguments.size());

        if (arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be at most 3.",
                getName(), arguments.size());

        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} as time delta", type.getName());

        if (arguments.size() >= 2)
        {
            const auto * largest_unit_arg = arguments[1].get();
            if (!isStringOrFixedString(largest_unit_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument largest_unit of function {}",
                                largest_unit_arg->getName(), getName());
        }

        if (arguments.size() == 3)
        {
            const auto * smallest_unit_arg = arguments[2].get();
            if (!isStringOrFixedString(smallest_unit_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument smallest_unit of function {}",
                                smallest_unit_arg->getName(), getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForConstants() const override { return true; }

    static constexpr auto all_units = std::array{
        Unit{simple_year()}, /// different definitions of year and month than std::chrono ones
        Unit{simple_month()},
        Unit{std::chrono::days()},
        Unit{std::chrono::hours()},
        Unit{std::chrono::minutes()},
        Unit{std::chrono::seconds()},
        Unit{std::chrono::milliseconds()},
        Unit{std::chrono::microseconds()},
        Unit{std::chrono::nanoseconds()},
    };

    Unit convertFromString(std::string_view unit_str, std::string_view default_unit, std::string_view argument_name) const
    {
        using namespace std::string_view_literals;

        if (unit_str.empty())
            unit_str = default_unit;

        if (unit_str.back() == 's')
        {
            unit_str.remove_suffix(1);
        }

        const auto unit_it = std::find(all_units.begin(), all_units.end(), unit_str);

        if (unit_it == all_units.end())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected value of {} argument ({}) for function {}, the only allowed values are:"
                " 'nanoseconds', 'microseconds', 'milliseconds', 'seconds', 'minutes', 'hours', 'days', 'months', 'years'.",
                argument_name, unit_str, getName());
        }

        return *unit_it;
    }

    [[maybe_unused]] std::span<const Unit> selectUnitRange(const ColumnsWithTypeAndName & arguments) const
    {
        using namespace std::string_view_literals;

        const auto get_unit_str = [&arguments](size_t i) -> std::string_view
        {
            if (arguments.size() <= i)
                return {};

            const ColumnPtr & unit_column = arguments[i].column;
            const ColumnConst * unit_const_col = checkAndGetColumnConstStringOrFixedString(unit_column.get());

            if (!unit_const_col)
                return {};

            return unit_const_col->getDataColumn().getDataAt(0).toView();
        };

        const std::string_view largest_unit_str = get_unit_str(1);
        const std::string_view smallest_unit_str = get_unit_str(2);

        const Unit largest_unit = convertFromString(largest_unit_str, "years"sv, "largest_unit"sv);
        const Unit smallest_unit = convertFromString(smallest_unit_str, "seconds"sv, "smallest_unit"sv);

        auto largest_unit_iter = std::find(all_units.begin(), all_units.end(), largest_unit);
        auto smallest_unit_iter = std::find(all_units.begin(), all_units.end(), smallest_unit);

        // Make sure the range of units is always from Largest to Smallest (year > seconds)
        if (smallest_unit_iter < largest_unit_iter)
        {
            std::swap(smallest_unit_iter, largest_unit_iter);
        }

        /// We are already sure we we have something as the table was used to search for Unit value
        assert(smallest_unit_iter != all_units.end());

        /// Resulting range of units
        return std::span(largest_unit_iter, std::next(smallest_unit_iter));
    }

    ColumnPtr executeImpl([[maybe_unused]] const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        [[maybe_unused]] const auto selected_units = selectUnitRange(arguments);
        assert(!selected_units.empty());

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// Virtual call is Ok (negligible comparing to the rest of calculations).
            [[maybe_unused]] const auto duration = std::chrono::duration<Float64>{arguments[0].column->getFloat64(i)};

            formatDurationInto(duration, selected_units, buf_to);

            /// And finish the string... (always)
            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

    [[maybe_unused]] static void formatDurationInto(std::chrono::duration<Float64> duration, std::span<const Unit> selected_units, WriteBuffer & buf_to)
    {
        if (!isFinite(duration.count()))
        {
            /// Cannot decide what unit it is (years, month), just simply write inf or nan.
            writeFloatText(duration.count(), buf_to);
            return;
        }

        const bool is_negative = duration.count() < 0;
        if (is_negative)
        {
            writeChar('-', buf_to);
            duration = abs(duration);
        }

        /// To output separators between parts: ", " and " and ".
        bool has_previous_output = false;

        for (const auto & unit: selected_units)
        {
            if (unlikely(duration.count() + 1.0 == duration.count()))
            {
                /// The case when value is too large so exact representation for subsequent smaller units is not possible.
                const UInt64 count = unit.convertToUnit(duration);
                writeText(count, buf_to);
                writeChar(' ', buf_to);
                buf_to.write(unit.name.data(), unit.name.size());
                writeChar('s', buf_to);
                return;
            }

            if (processUnit(unit, selected_units.back(), duration, buf_to, has_previous_output)) {
                return;
            }
        }
    }

    static bool processUnit(const Unit & unit, const Unit & smallest_unit, std::chrono::duration<Float64> & duration, WriteBuffer & buf_to, bool & has_previous_output)
    {
        const std::string_view unit_name = unit.name;
        const UInt64 duration_in_unit = unit.convertToUnit(duration);

        /// Remaining value to print on next iteration.
        duration -= unit.convertFromUnit(duration_in_unit); // and now subtract it

        if (duration_in_unit == 0)
        {
            /// Nothing to print (skip)
            return false;
        }
        
        /// Remainder is zero, which means all smaller units will be skipped
        const bool unit_is_last_one = (unit == smallest_unit) || (smallest_unit.convertToUnit(duration) == 0);

        if (has_previous_output)
        {
            /// Need delimiter between parts. The last delimiter is " and ", all previous are comma.
            if (unit_is_last_one)
                writeCString(" and ", buf_to);
            else 
                writeCString(", ", buf_to);
        }

        justPrintCountAndUnit(duration_in_unit, unit_name, buf_to);
        has_previous_output |= true;
        
        return unit_is_last_one;
    }

    static void justPrintCountAndUnit(UInt64 count, std::string_view unit_name, WriteBuffer & buf_to)
    {
        writeText(count, buf_to);
        writeChar(' ', buf_to);
        buf_to.write(unit_name.data(), unit_name.size());

        /// How to pronounce: unit vs. units.
        if (count != 1u)
            writeChar('s', buf_to);
    }
};

}

REGISTER_FUNCTION(FormatReadableTimeDelta)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}
