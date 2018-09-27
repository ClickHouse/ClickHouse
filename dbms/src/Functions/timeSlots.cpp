#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** timeSlots(StartTime, Duration)
  * - for the time interval beginning at `StartTime` and continuing `Duration` seconds,
  *   returns an array of time points, consisting of rounding down to half an hour of points from this interval.
  *  For example, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  This is necessary to search for hits that are part of the corresponding visit.
  *
  * This is obsolete function. It was developed for Yandex.Metrica, but no longer used.
  */

template <typename DurationType>
struct TimeSlotsImpl
{
    static constexpr UInt32 TIME_SLOT_SIZE = 1800;

    static void vector_vector(
        const PaddedPODArray<UInt32> & starts, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vector_constant(
        const PaddedPODArray<UInt32> & starts, DurationType duration,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + duration) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constant_vector(
        UInt32 start, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = durations.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constant_constant(
        UInt32 start, DurationType duration,
        Array & result)
    {
        for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + duration) / TIME_SLOT_SIZE; ++value)
            result.push_back(static_cast<UInt64>(value * TIME_SLOT_SIZE));
    }
};


class FunctionTimeSlots : public IFunction
{
public:
    static constexpr auto name = "timeSlots";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlots>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isDateTime())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1]).isUInt32())
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto starts = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());
        auto const_starts = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());

        auto durations = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());
        auto const_durations = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getValue<UInt32>(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constant_vector(const_starts->getValue<UInt32>(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && const_durations)
        {
            Array const_res;
            TimeSlotsImpl<UInt32>::constant_constant(const_starts->getValue<UInt32>(), const_durations->getValue<UInt32>(), const_res);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, const_res);
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
                    + ", " + block.getByPosition(arguments[1]).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionTimeSlots(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTimeSlots>();
}

}
