#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


/** Functions for an unusual conversion to a string or array:
  *
  * bitmaskToList - takes an integer - a bitmask, returns a string of degrees of 2 separated by a comma.
  *                 for example, bitmaskToList(50) = '2,16,32'
  *
  * bitmaskToArray(x) - Returns an array of powers of two in the binary form of x. For example, bitmaskToArray(50) = [2, 16, 32].
  *
  */

namespace
{

class FunctionBitmaskToList : public IFunction
{
public:
    static constexpr auto name = "bitmaskToList";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmaskToList>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypePtr & type = arguments[0];

        if (!isInteger(type))
            throw Exception("Cannot format " + type->getName() + " as bitmask string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        ColumnPtr res;
        if (!((res = executeType<UInt8>(arguments))
            || (res = executeType<UInt16>(arguments))
            || (res = executeType<UInt32>(arguments))
            || (res = executeType<UInt64>(arguments))
            || (res = executeType<Int8>(arguments))
            || (res = executeType<Int16>(arguments))
            || (res = executeType<Int32>(arguments))
            || (res = executeType<Int64>(arguments))))
            throw Exception("Illegal column " + arguments[0].column->getName()
                            + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        return res;
    }

private:
    template <typename T>
    inline static void writeBitmask(T x, WriteBuffer & out)
    {
        using UnsignedT = make_unsigned_t<T>;
        UnsignedT u_x = x;

        bool first = true;
        while (u_x)
        {
            UnsignedT y = u_x & (u_x - 1);
            UnsignedT bit = u_x ^ y;
            u_x = y;
            if (!first)
                writeChar(',', out);
            first = false;
            writeIntText(static_cast<T>(bit), out);
        }
    }

    template <typename T>
    ColumnPtr executeType(const ColumnsWithTypeAndName & columns) const
    {
        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(columns[0].column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                writeBitmask<T>(vec_from[i], buf_to);
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }

            buf_to.finalize();
            return col_to;
        }

        return nullptr;
    }
};


class FunctionBitmaskToArray : public IFunction
{
public:
    static constexpr auto name = "bitmaskToArray";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitmaskToArray>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename T>
    bool tryExecute(const IColumn * column, ColumnPtr & out_column) const
    {
        using UnsignedT = make_unsigned_t<T>;

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(column))
        {
            auto col_values = ColumnVector<T>::create();
            auto col_offsets = ColumnArray::ColumnOffsets::create();

            typename ColumnVector<T>::Container & res_values = col_values->getData();
            ColumnArray::Offsets & res_offsets = col_offsets->getData();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            res_offsets.resize(size);
            res_values.reserve(size * 2);

            for (size_t row = 0; row < size; ++row)
            {
                UnsignedT x = vec_from[row];
                while (x)
                {
                    UnsignedT y = x & (x - 1);
                    UnsignedT bit = x ^ y;
                    x = y;
                    res_values.push_back(bit);
                }
                res_offsets[row] = res_values.size();
            }

            out_column = ColumnArray::create(std::move(col_values), std::move(col_offsets));
            return true;
        }
        else
        {
            return false;
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * in_column = arguments[0].column.get();
        ColumnPtr out_column;

        if (tryExecute<UInt8>(in_column, out_column) ||
            tryExecute<UInt16>(in_column, out_column) ||
            tryExecute<UInt32>(in_column, out_column) ||
            tryExecute<UInt64>(in_column, out_column) ||
            tryExecute<Int8>(in_column, out_column) ||
            tryExecute<Int16>(in_column, out_column) ||
            tryExecute<Int32>(in_column, out_column) ||
            tryExecute<Int64>(in_column, out_column))
            return out_column;

        throw Exception("Illegal column " + arguments[0].column->getName()
                        + " of first argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

class FunctionBitPositionsToArray : public IFunction
{
public:
    static constexpr auto name = "bitPositionsToArray";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBitPositionsToArray>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument of function {}",
                            getName(),
                            arguments[0]->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    template <typename T>
    ColumnPtr executeType(const IColumn * column) const
    {
        const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(column);
        if (!col_from)
            return nullptr;

        auto result_array_values = ColumnVector<UInt64>::create();
        auto result_array_offsets = ColumnArray::ColumnOffsets::create();

        auto & result_array_values_data = result_array_values->getData();
        auto & result_array_offsets_data = result_array_offsets->getData();

        auto & vec_from = col_from->getData();
        size_t size = vec_from.size();
        result_array_offsets_data.resize(size);
        result_array_values_data.reserve(size * 2);

        using UnsignedType = make_unsigned_t<T>;

        for (size_t row = 0; row < size; ++row)
        {
            UnsignedType x = static_cast<UnsignedType>(vec_from[row]);

            if constexpr (is_big_int_v<UnsignedType>)
            {
                size_t position = 0;

                while (x)
                {
                    if (x & 1)
                        result_array_values_data.push_back(position);

                    x >>= 1;
                    ++position;
                }
            }
            else
            {
                while (x)
                {
                    result_array_values_data.push_back(std::countr_zero(x));
                    x &= (x - 1);
                }
            }

            result_array_offsets_data[row] = result_array_values_data.size();
        }

        auto result_column = ColumnArray::create(std::move(result_array_values), std::move(result_array_offsets));

        return result_column;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IColumn * in_column = arguments[0].column.get();
        ColumnPtr result_column;

        if (!((result_column = executeType<UInt8>(in_column))
              || (result_column = executeType<UInt16>(in_column))
              || (result_column = executeType<UInt32>(in_column))
              || (result_column = executeType<UInt32>(in_column))
              || (result_column = executeType<UInt64>(in_column))
              || (result_column = executeType<UInt128>(in_column))
              || (result_column = executeType<UInt256>(in_column))
              || (result_column = executeType<Int8>(in_column))
              || (result_column = executeType<Int16>(in_column))
              || (result_column = executeType<Int32>(in_column))
              || (result_column = executeType<Int64>(in_column))
              || (result_column = executeType<Int128>(in_column))
              || (result_column = executeType<Int256>(in_column))))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of first argument of function {}",
                            arguments[0].column->getName(),
                            getName());
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(BitToArray)
{
    factory.registerFunction<FunctionBitPositionsToArray>();
    factory.registerFunction<FunctionBitmaskToArray>();
    factory.registerFunction<FunctionBitmaskToList>();
}

}

