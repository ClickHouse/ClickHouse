#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionArrayMapped.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct ArraySumImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        WhichDataType which(expression_return);

        if (which.isNativeUInt())
            return std::make_shared<DataTypeUInt64>();

        if (which.isNativeInt())
            return std::make_shared<DataTypeInt64>();

        if (which.isFloat())
            return std::make_shared<DataTypeFloat64>();

        throw Exception("arraySum cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        const ColumnVector<Element> * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);

        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColumnVector<Element>>(&*mapped);

            if (!column_const)
                return false;

            const Element x = column_const->template getValue<Element>();

            auto res_column = ColumnVector<Result>::create(offsets.size());
            typename ColumnVector<Result>::Container & res = res_column->getData();

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                res[i] = x * (offsets[i] - pos);
                pos = offsets[i];
            }

            res_ptr = std::move(res_column);
            return true;
        }

        const typename ColumnVector<Element>::Container & data = column->getData();
        auto res_column = ColumnVector<Result>::create(offsets.size());
        typename ColumnVector<Result>::Container & res = res_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            Result s = 0;
            for (; pos < offsets[i]; ++pos)
            {
                s += data[pos];
            }
            res[i] = s;
        }

        res_ptr = std::move(res_column);
        return true;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const IColumn::Offsets & offsets = array.getOffsets();
        ColumnPtr res;

        if (executeType< UInt8 , UInt64>(mapped, offsets, res) ||
            executeType< UInt16, UInt64>(mapped, offsets, res) ||
            executeType< UInt32, UInt64>(mapped, offsets, res) ||
            executeType< UInt64, UInt64>(mapped, offsets, res) ||
            executeType<  Int8 ,  Int64>(mapped, offsets, res) ||
            executeType<  Int16,  Int64>(mapped, offsets, res) ||
            executeType<  Int32,  Int64>(mapped, offsets, res) ||
            executeType<  Int64,  Int64>(mapped, offsets, res) ||
            executeType<Float32,Float64>(mapped, offsets, res) ||
            executeType<Float64,Float64>(mapped, offsets, res))
            return res;
        else
            throw Exception("Unexpected column for arraySum: " + mapped->getName());
    }
};

struct NameArraySum { static constexpr auto name = "arraySum"; };
using FunctionArraySum = FunctionArrayMapped<ArraySumImpl, NameArraySum>;

void registerFunctionArraySum(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArraySum>();
}

}

