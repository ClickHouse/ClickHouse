#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
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

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers &, size_t result, size_t /*input_rows_count*/) const override
    {
        if (!(executeType<UInt8>(columns, result)
            || executeType<UInt16>(columns, result)
            || executeType<UInt32>(columns, result)
            || executeType<UInt64>(columns, result)
            || executeType<Int8>(columns, result)
            || executeType<Int16>(columns, result)
            || executeType<Int32>(columns, result)
            || executeType<Int64>(columns, result)))
            throw Exception("Illegal column " + columns[0].column->getName()
                            + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
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
    bool executeType(ColumnsWithTypeAndName & columns, size_t result) const
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
            columns[result].column = std::move(col_to);
        }
        else
        {
            return false;
        }

        return true;
    }
};

}

void registerFunctionBitmaskToList(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitmaskToList>();
}

}

