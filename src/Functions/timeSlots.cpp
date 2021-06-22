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

/** timeSlots(StartTime, Duration)
  * - for the time interval beginning at `StartTime` and continuing `Duration` seconds,
  *   returns an array of time points, consisting of rounding down to half an hour (default; or another value) of points from this interval.
  *  For example, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  This is necessary to search for hits that are part of the corresponding visit.
  *
  * This is obsolete function. It was developed for Yandex.Metrica, but no longer used in Yandex.
  * But this function was adopted by wider audience.
  */

template <typename DurationType>
struct TimeSlotsImpl
{
    static void vectorVector(
        const PaddedPODArray<UInt32> & starts, const PaddedPODArray<DurationType> & durations, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
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
        const PaddedPODArray<UInt32> & starts, DurationType duration, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
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
        UInt32 start, const PaddedPODArray<DurationType> & durations, UInt32 time_slot_size,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = durations.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = start / time_slot_size, end = (start + durations[i]) / time_slot_size; value <= end; ++value)
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
    static constexpr UInt32 TIME_SLOT_SIZE = 1800;
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

        if (!WhichDataType(arguments[0].type).isDateTime())
            throw Exception("Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1].type).isUInt32())
            throw Exception("Illegal type " + arguments[1].type->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3 && !WhichDataType(arguments[2].type).isNativeUInt())
            throw Exception("Illegal type " + arguments[2].type->getName() + " of third argument of function " + getName() + ". Must be UInt32.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// If time zone is specified for source data type, attach it to the resulting type.
        /// Note that there is no explicit time zone argument for this function (we specify 2 as an argument number with explicit time zone).
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 3, 0)));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * starts = checkAndGetColumn<ColumnUInt32>(arguments[0].column.get());
        const auto * const_starts = checkAndGetColumnConst<ColumnUInt32>(arguments[0].column.get());

        const auto * durations = checkAndGetColumn<ColumnUInt32>(arguments[1].column.get());
        const auto * const_durations = checkAndGetColumnConst<ColumnUInt32>(arguments[1].column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        auto time_slot_size = TIME_SLOT_SIZE;

        if (arguments.size() == 3)
        {
            const auto * time_slot_column = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
            if (!time_slot_column)
                throw Exception("Third argument for function " + getName() + " must be constant UInt32", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (time_slot_size = time_slot_column->getValue<UInt32>(); time_slot_size == 0)
                throw Exception("Third argument for function " + getName() + " must be greater than zero", ErrorCodes::ILLEGAL_COLUMN);
        }

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vectorVector(starts->getData(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vectorConstant(starts->getData(), const_durations->getValue<UInt32>(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constantVector(const_starts->getValue<UInt32>(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            return res;
        }
        else
            throw Exception("Illegal columns " + arguments[0].column->getName()
                    + ", " + arguments[1].column->getName()
                    + ", " + arguments[2].column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionTimeSlots(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeSlots>();
}

}
