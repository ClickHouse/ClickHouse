#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnArray.h>
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

template <typename DurationType>
struct TimeSlotsImpl
{
    static void vectorVector(
        const PaddedPODArray<Int64> & starts, const PaddedPODArray<DurationType> & durations, UInt64 time_slot_size,
        PaddedPODArray<Int64> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (Int64 value = starts[i] / time_slot_size, end = (starts[i] + durations[i]) / time_slot_size; value <= end; ++value)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vectorConstant(
        const PaddedPODArray<Int64> & starts, DurationType duration, UInt64 time_slot_size,
        PaddedPODArray<Int64> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (Int64 value = starts[i] / time_slot_size, end = (starts[i] + duration) / time_slot_size; value <= end; ++value)
            {
                result_values.push_back(value * time_slot_size);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constantVector(
        Int64 start, const PaddedPODArray<DurationType> & durations, UInt64 time_slot_size,
        PaddedPODArray<Int64> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = durations.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (Int64 value = start / time_slot_size, end = (start + durations[i]) / time_slot_size; value <= end; ++value)
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
    static constexpr UInt64 TIME_SLOT_SIZE = 1800;
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
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(arguments.size()) + ", should be 2 or 3",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!WhichDataType(arguments[0].type).isDateTime() && !WhichDataType(arguments[0].type).isDateTime64())
            throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName()
                            + ". Must be DateTime or DateTime64.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1].type).isNativeUInt())
            throw Exception("Illegal type " + arguments[1].type->getName() + " of second argument of function " + getName() + ". Must be UInt64.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isNativeUInt())
            throw Exception("Illegal type " + arguments[2].type->getName() + " of third argument of function " + getName() + ". Must be UInt64.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// If time zone is specified for source data type, attach it to the resulting type.
        /// Note that there is no explicit time zone argument for this function (we specify 2 as an argument number with explicit time zone).
        if (WhichDataType(arguments[0].type).isDateTime())
        {
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0)));

        }
        else
        {
            auto dt64_scale = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type).getScale();
            return std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeDateTime64>(dt64_scale, extractTimeZoneNameFromFunctionArguments(arguments, 3, 0)));
        }

    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * starts = checkAndGetColumn<ColumnInt64>(arguments[0].column.get());
        const auto * const_starts = checkAndGetColumnConst<ColumnInt64>(arguments[0].column.get());

        const auto * durations = checkAndGetColumn<ColumnInt64>(arguments[1].column.get());
        const auto * const_durations = checkAndGetColumnConst<ColumnInt64>(arguments[1].column.get());

        auto res = ColumnArray::create(ColumnInt64::create());
        ColumnInt64::Container & res_values = typeid_cast<ColumnInt64 &>(res->getData()).getData();

        auto time_slot_size = TIME_SLOT_SIZE;

        if (arguments.size() == 3)
        {
            const auto * time_slot_column = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
            if (!time_slot_column)
                throw Exception("Third argument for function " + getName() + " must be constant UInt64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (time_slot_size = time_slot_column->getValue<UInt64>(); time_slot_size == 0)
                throw Exception("Third argument for function " + getName() + " must be greater than zero", ErrorCodes::ILLEGAL_COLUMN);
        }

        if (starts && durations)
        {
            TimeSlotsImpl<Int64>::vectorVector(starts->getData(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<Int64>::vectorConstant(starts->getData(), const_durations->getValue<Int64>(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<Int64>::constantVector(const_starts->getValue<Int64>(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else
            if (arguments.size() == 3)
        {
            throw Exception("Illegal columns " + arguments[0].column->getName()
                    + ", " + arguments[1].column->getName()
                    + ", " + arguments[2].column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
        else
        {
            throw Exception("Illegal columns " + arguments[0].column->getName()
                                + ", " + arguments[1].column->getName()
                                + " of arguments of function " + getName(),
                            ErrorCodes::ILLEGAL_COLUMN);
        }

    }
};

}

void registerFunctionTimeSlots(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeSlots>();
}

}
