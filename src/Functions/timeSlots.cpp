#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** timeSlots(StartTime, Duration[, Size=1800])
  * - for the time interval beginning at `StartTime` and continuing `Duration` seconds,
  *   returns an array of time points, consisting of rounding down to Size (1800 seconds by default) of points from this interval.
  *  For example, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  This is necessary to search for hits that are part of the corresponding visit.
  *
  * This is obsolete function. It was developed for Metrica web analytics system, but the art of its usage has been forgotten.
  * But this function was adopted by wider audience.
  */

struct TimeSlotsImpl
{
    /// The following three methods process DateTime type
    static void vectorVector(
        const PaddedPODArray<UInt32> & starts, const PaddedPODArray<UInt32> & durations, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets,
        size_t input_rows_count)
    {
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (UInt32 value = starts[i] / time_slot_size, end = (starts[i] + durations[i]) / time_slot_size; value <= end; ++value)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vectorConstant(
        const PaddedPODArray<UInt32> & starts, UInt32 duration, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets,
        size_t input_rows_count)
    {
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (UInt32 value = starts[i] / time_slot_size, end = (starts[i] + duration) / time_slot_size; value <= end; ++value)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constantVector(
        UInt32 start, const PaddedPODArray<UInt32> & durations, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets,
        size_t input_rows_count)
    {
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (UInt32 value = start / time_slot_size, end = (start + durations[i]) / time_slot_size; value <= end; ++value)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }
    /*
    The following three methods process DateTime64 type
    NO_SANITIZE_UNDEFINED is put here because user shall be careful when working with Decimal
    Adjusting different scales can cause overflow -- it is OK for us. Don't use scales that differ a lot :)
    */
    static NO_SANITIZE_UNDEFINED void vectorVector(
        const PaddedPODArray<DateTime64> & starts, const PaddedPODArray<Decimal64> & durations, Decimal64 time_slot_size,
        PaddedPODArray<DateTime64> & result_values, ColumnArray::Offsets & result_offsets, UInt16 dt_scale, UInt16 duration_scale, UInt16 time_slot_scale,
        size_t input_rows_count)
    {
        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        /// Modify all units to have same scale
        UInt16 max_scale = std::max({dt_scale, duration_scale, time_slot_scale});

        Int64 dt_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - dt_scale);
        Int64 dur_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - duration_scale);
        Int64 ts_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - time_slot_scale);

        ColumnArray::Offset current_offset = 0;
        time_slot_size = time_slot_size.value * ts_multiplier;
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (DateTime64 value = (starts[i] * dt_multiplier) / time_slot_size, end = (starts[i] * dt_multiplier + durations[i] * dur_multiplier) / time_slot_size; value <= end; value += 1)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }
            result_offsets[i] = current_offset;
        }
    }

    static NO_SANITIZE_UNDEFINED void vectorConstant(
        const PaddedPODArray<DateTime64> & starts, Decimal64 duration, Decimal64 time_slot_size,
        PaddedPODArray<DateTime64> & result_values, ColumnArray::Offsets & result_offsets, UInt16 dt_scale, UInt16 duration_scale, UInt16 time_slot_scale,
        size_t input_rows_count)
    {
        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        /// Modify all units to have same scale
        UInt16 max_scale = std::max({dt_scale, duration_scale, time_slot_scale});

        Int64 dt_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - dt_scale);
        Int64 dur_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - duration_scale);
        Int64 ts_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - time_slot_scale);

        ColumnArray::Offset current_offset = 0;
        duration = duration * dur_multiplier;
        time_slot_size = time_slot_size.value * ts_multiplier;
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (DateTime64 value = (starts[i] * dt_multiplier) / time_slot_size, end = (starts[i] * dt_multiplier + duration) / time_slot_size; value <= end; value += 1)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }
            result_offsets[i] = current_offset;
        }
    }

    static NO_SANITIZE_UNDEFINED void constantVector(
        DateTime64 start, const PaddedPODArray<Decimal64> & durations, Decimal64 time_slot_size,
        PaddedPODArray<DateTime64> & result_values, ColumnArray::Offsets & result_offsets, UInt16 dt_scale, UInt16 duration_scale, UInt16 time_slot_scale,
        size_t input_rows_count)
    {
        result_offsets.resize(input_rows_count);
        result_values.reserve(input_rows_count);

        /// Modify all units to have same scale
        UInt16 max_scale = std::max({dt_scale, duration_scale, time_slot_scale});

        Int64 dt_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - dt_scale);
        Int64 dur_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - duration_scale);
        Int64 ts_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(max_scale - time_slot_scale);

        ColumnArray::Offset current_offset = 0;
        start = dt_multiplier * start;
        time_slot_size = time_slot_size.value * ts_multiplier;
        if (time_slot_size == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time slot size cannot be zero");

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            for (DateTime64 value = start / time_slot_size, end = (start + durations[i] * dur_multiplier) / time_slot_size; value <= end; value += 1)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }
            result_offsets[i] = current_offset;
        }
    }
};


class FunctionTimeSlots : public IFunction
{
public:
    static constexpr auto name = "timeSlots";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSlots>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                            getName(), arguments.size());

        if (WhichDataType(arguments[0].type).isDateTime())
        {
            if (!WhichDataType(arguments[1].type).isUInt32())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}. "
                    "Must be UInt32 when first argument is DateTime.", arguments[1].type->getName(), getName());

            if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isNativeUInt())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}. "
                    "Must be UInt32 when first argument is DateTime.", arguments[2].type->getName(), getName());
        }
        else if (WhichDataType(arguments[0].type).isDateTime64())
        {
            if (!WhichDataType(arguments[1].type).isDecimal64())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of second argument of function {}. "
                    "Must be Decimal64 when first argument is DateTime64.", arguments[1].type->getName(), getName());

            if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isDecimal64())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of function {}. "
                    "Must be Decimal64 when first argument is DateTime64.", arguments[2].type->getName(), getName());
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of first argument of function {}. "
                                "Must be DateTime or DateTime64.", arguments[0].type->getName(), getName());

        /// If time zone is specified for source data type, attach it to the resulting type.
        /// Note that there is no explicit time zone argument for this function (we specify 2 as an argument number with explicit time zone).
        if (WhichDataType(arguments[0].type).isDateTime())
        {
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0, false)));
        }

        auto start_time_scale = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type).getScale();
        auto duration_scale = assert_cast<const DataTypeDecimal64 &>(*arguments[1].type).getScale();
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime64>(
            std::max(start_time_scale, duration_scale), extractTimeZoneNameFromFunctionArguments(arguments, 3, 0, false)));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (WhichDataType(arguments[0].type).isDateTime())
        {
            UInt32 time_slot_size = 1800;
            if (arguments.size() == 3)
            {
                const auto * time_slot_column = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
                if (!time_slot_column)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument for function {} must be constant UInt32", getName());

                if (time_slot_size = time_slot_column->getValue<UInt32>(); time_slot_size <= 0)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument for function {} must be greater than zero", getName());
            }

            const auto * dt_starts = checkAndGetColumn<ColumnDateTime>(arguments[0].column.get());
            const auto * dt_const_starts = checkAndGetColumnConst<ColumnDateTime>(arguments[0].column.get());

            const auto * durations = checkAndGetColumn<ColumnDateTime>(arguments[1].column.get());
            const auto * const_durations = checkAndGetColumnConst<ColumnDateTime>(arguments[1].column.get());

            auto res = ColumnArray::create(ColumnUInt32::create());
            ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

            if (dt_starts && durations)
            {
                TimeSlotsImpl::vectorVector(dt_starts->getData(), durations->getData(), time_slot_size, res_values, res->getOffsets(), input_rows_count);
                return res;
            }
            if (dt_starts && const_durations)
            {
                TimeSlotsImpl::vectorConstant(
                    dt_starts->getData(),
                    const_durations->getValue<UInt32>(),
                    time_slot_size,
                    res_values,
                    res->getOffsets(),
                    input_rows_count);
                return res;
            }
            if (dt_const_starts && durations)
            {
                TimeSlotsImpl::constantVector(
                    dt_const_starts->getValue<UInt32>(),
                    durations->getData(),
                    time_slot_size,
                    res_values,
                    res->getOffsets(),
                    input_rows_count);
                return res;
            }
        }
        else
        {
            assert(WhichDataType(arguments[0].type).isDateTime64());
            Decimal64 time_slot_size = Decimal64(1800);
            UInt16 time_slot_scale = 0;
            if (arguments.size() == 3)
            {
                const auto * time_slot_column = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
                if (!time_slot_column)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Third argument for function {} must be constant Decimal64", getName());

                if (time_slot_size = time_slot_column->getValue<Decimal64>(); time_slot_size <= 0)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Third argument for function {} must be greater than zero", getName());
                time_slot_scale = assert_cast<const DataTypeDecimal64 *>(arguments[2].type.get())->getScale();
            }

            const auto * starts = checkAndGetColumn<ColumnDateTime64>(arguments[0].column.get());
            const auto * const_starts = checkAndGetColumnConst<ColumnDateTime64>(arguments[0].column.get());

            const auto * durations = checkAndGetColumn<ColumnDecimal<Decimal64>>(arguments[1].column.get());
            const auto * const_durations = checkAndGetColumnConst<ColumnDecimal<Decimal64>>(arguments[1].column.get());

            const auto start_time_scale = assert_cast<const DataTypeDateTime64 *>(arguments[0].type.get())->getScale();
            const auto duration_scale = assert_cast<const DataTypeDecimal64 *>(arguments[1].type.get())->getScale();

            auto res = ColumnArray::create(DataTypeDateTime64(start_time_scale).createColumn());
            DataTypeDateTime64::ColumnType::Container & res_values = typeid_cast<DataTypeDateTime64::ColumnType &>(res->getData()).getData();

            if (starts && durations)
            {
                TimeSlotsImpl::vectorVector(starts->getData(), durations->getData(), time_slot_size, res_values, res->getOffsets(),
                    start_time_scale, duration_scale, time_slot_scale, input_rows_count);
                return res;
            }
            if (starts && const_durations)
            {
                TimeSlotsImpl::vectorConstant(
                    starts->getData(),
                    const_durations->getValue<Decimal64>(),
                    time_slot_size,
                    res_values,
                    res->getOffsets(),
                    start_time_scale,
                    duration_scale,
                    time_slot_scale,
                    input_rows_count);
                return res;
            }
            if (const_starts && durations)
            {
                TimeSlotsImpl::constantVector(
                    const_starts->getValue<DateTime64>(),
                    durations->getData(),
                    time_slot_size,
                    res_values,
                    res->getOffsets(),
                    start_time_scale,
                    duration_scale,
                    time_slot_scale,
                    input_rows_count);
                return res;
            }
        }

        if (arguments.size() == 3)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal columns {}, {}, {} of arguments of function {}",
                arguments[0].column->getName(), arguments[1].column->getName(), arguments[2].column->getName(), getName());
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal columns {}, {} of arguments of function {}",
            arguments[0].column->getName(),
            arguments[1].column->getName(),
            getName());
    }
};

}

REGISTER_FUNCTION(TimeSlots)
{
    factory.registerFunction<FunctionTimeSlots>();
}

}
