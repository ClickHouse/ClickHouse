#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


/** Function for an unusual conversion to a string:
  *
  * bitmaskToList - takes an integer - a bitmask, returns a string of degrees of 2 separated by a comma.
  *                 for example, bitmaskToList(50) = '2,16,32'
  */

namespace
{

class FunctionBitmaskToList : public IFunction
{
public:
    static constexpr auto name = "bitmaskToList";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitmaskToList>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

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
            writeIntText(T(bit), out);
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

}

void registerFunctionBitmaskToList(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitmaskToList>();
}

}

