#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct ArrayCumSumImpl
{
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

        throw Exception("arrayCumSum cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray & array, ColumnPtr & res_ptr)
    {
        const ColumnVector<Element> * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);

        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColumnVector<Element>>(&*mapped);

            if (!column_const)
                return false;

            const Element x = column_const->template getValue<Element>();
            const IColumn::Offsets & offsets = array.getOffsets();

            auto res_nested = ColumnVector<Result>::create();
            typename ColumnVector<Result>::Container & res_values = res_nested->getData();
            res_values.resize(column_const->size());

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                // skip empty arrays
                if (pos < offsets[i])
                {
                    res_values[pos++] = x;
                    for (; pos < offsets[i]; ++pos)
                    {
                        res_values[pos] = res_values[pos - 1] + x;
                    }
                }
            }

            res_ptr = ColumnArray::create(std::move(res_nested), array.getOffsetsPtr());
            return true;
        }

        const IColumn::Offsets & offsets = array.getOffsets();
        const typename ColumnVector<Element>::Container & data = column->getData();

        auto res_nested = ColumnVector<Result>::create();
        typename ColumnVector<Result>::Container & res_values = res_nested->getData();
        res_values.resize(data.size());

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            // skip empty arrays
            if (pos < offsets[i])
            {
                res_values[pos] = data[pos];
                for (++pos; pos < offsets[i]; ++pos)
                {
                    res_values[pos] = res_values[pos - 1] + data[pos];
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
            throw Exception("Unexpected column for arrayCumSum: " + mapped->getName());
    }

};

struct NameArrayCumSum { static constexpr auto name = "arrayCumSum"; };
using FunctionArrayCumSum = FunctionArrayMapped<ArrayCumSumImpl, NameArrayCumSum>;

void registerFunctionArrayCumSum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayCumSum>();
}

}

