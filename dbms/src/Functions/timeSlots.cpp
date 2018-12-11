#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
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
}

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
    static void vector_vector(
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

    static void vector_constant(
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

    static void constant_vector(
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
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlots>(); }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    String getSignature() const override
    {
        return "f(T : DateTime, const duration UInt32, [const slot_size NativeUInt]) -> Array(T)";
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        auto starts = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());
        auto const_starts = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());

        auto durations = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());
        auto const_durations = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        auto time_slot_size = TIME_SLOT_SIZE;

        if (arguments.size() == 3)
        {
            auto time_slot_column = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[2]).column.get());
            if (!time_slot_column)
                throw Exception("Third argument for function " + getName() + " must be constant UInt32", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (time_slot_size = time_slot_column->getValue<UInt32>(); time_slot_size == 0)
                throw Exception("Third argument for function " + getName() + " must be greater than zero", ErrorCodes::ILLEGAL_COLUMN);
        }

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getValue<UInt32>(), time_slot_size, res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constant_vector(const_starts->getValue<UInt32>(), durations->getData(), time_slot_size, res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
                    + ", " + block.getByPosition(arguments[1]).column->getName()
                    + ", " + block.getByPosition(arguments[2]).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionTimeSlots(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeSlots>();
}

}
