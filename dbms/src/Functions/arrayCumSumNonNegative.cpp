#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

/** arrayCumSumNonNegative() - returns an array with cumulative sums of the original. (If value < 0 -> 0).
  */
struct ArrayCumSumNonNegativeImpl
{
    static bool useDefaultImplementationForConstants() { return true; }
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        WhichDataType which(expression_return);

        if (which.isNativeUInt())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());

        if (which.isNativeInt())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

        if (which.isFloat())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

        throw Exception("arrayCumSumNonNegativeImpl cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        const ColumnVector<Element> * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);

        if (!column)
            return false;

        const IColumn::Offsets & offsets = array.getOffsets();
        const typename ColumnVector<Element>::Container & data = column->getData();

        auto res_nested = ColumnVector<Result>::create();
        typename ColumnVector<Result>::Container & res_values = res_nested->getData();
        res_values.resize(data.size());

        size_t pos = 0;
        Result accum_sum = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            // skip empty arrays
            if (pos < offsets[i])
            {
                accum_sum = data[pos];
                res_values[pos] = accum_sum > 0 ? accum_sum : 0;
                for (++pos; pos < offsets[i]; ++pos)
                {
                    accum_sum = accum_sum + data[pos];

                    res_values[pos] = accum_sum > 0 ? accum_sum : 0;
                }
            }
        }
        res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
        return true;

    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        ColumnPtr res;

        if (executeType< UInt8 , UInt64>(mapped, array, res) ||
            executeType< UInt16, UInt64>(mapped, array, res) ||
            executeType< UInt32, UInt64>(mapped, array, res) ||
            executeType< UInt64, UInt64>(mapped, array, res) ||
            executeType<  Int8 ,  Int64>(mapped, array, res) ||
            executeType<  Int16,  Int64>(mapped, array, res) ||
            executeType<  Int32,  Int64>(mapped, array, res) ||
            executeType<  Int64,  Int64>(mapped, array, res) ||
            executeType<Float32,Float64>(mapped, array, res) ||
            executeType<Float64,Float64>(mapped, array, res))
            return res;
        else
            throw Exception("Unexpected column for arrayCumSumNonNegativeImpl: " + mapped->getName());
    }

};

struct NameArrayCumSumNonNegative { static constexpr auto name = "arrayCumSumNonNegative"; };
using FunctionArrayCumSumNonNegative = FunctionArrayMapped<ArrayCumSumNonNegativeImpl, NameArrayCumSumNonNegative>;

void registerFunctionArrayCumSumNonNegative(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCumSumNonNegative>();
}

}

