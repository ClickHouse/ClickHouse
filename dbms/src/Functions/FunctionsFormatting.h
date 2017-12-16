#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** Function for an unusual conversion to a string:
    *
    * bitmaskToList - takes an integer - a bitmask, returns a string of degrees of 2 separated by a comma.
    *                     for example, bitmaskToList(50) = '2,16,32'
    *
    * formatReadableSize - prints the transferred size in bytes in form `123.45 GiB`.
    */

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
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType * type = arguments[0].get();

        if (!type->isInteger())
            throw Exception("Cannot format " + type->getName() + " as bitmask string", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeType<UInt8>(block, arguments, result)
            ||    executeType<UInt16>(block, arguments, result)
            ||    executeType<UInt32>(block, arguments, result)
            ||    executeType<UInt64>(block, arguments, result)
            ||    executeType<Int8>(block, arguments, result)
            ||    executeType<Int16>(block, arguments, result)
            ||    executeType<Int32>(block, arguments, result)
            ||    executeType<Int64>(block, arguments, result)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    inline static void writeBitmask(T x, WriteBuffer & out)
    {
        bool first = true;
        while (x)
        {
            T y = (x & (x - 1));
            T bit = x ^ y;
            x = y;
            if (!first)
                out.write(",", 1);
            first = false;
            writeIntText(bit, out);
        }
    }

    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> buf_to(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                writeBitmask<T>(vec_from[i], buf_to);
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }
            data_to.resize(buf_to.count());

            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            return false;
        }

        return true;
    }
};


class FunctionFormatReadableSize : public IFunction
{
public:
    static constexpr auto name = "formatReadableSize";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFormatReadableSize>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const IDataType & type = *arguments[0];

        if (!type.isNumber())
            throw Exception("Cannot format " + type.getName() + " as size in bytes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeType<UInt8>(block, arguments, result)
            ||    executeType<UInt16>(block, arguments, result)
            ||    executeType<UInt32>(block, arguments, result)
            ||    executeType<UInt64>(block, arguments, result)
            ||    executeType<Int8>(block, arguments, result)
            ||    executeType<Int16>(block, arguments, result)
            ||    executeType<Int32>(block, arguments, result)
            ||    executeType<Int64>(block, arguments, result)
            ||    executeType<Float32>(block, arguments, result)
            ||    executeType<Float64>(block, arguments, result)))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

private:
    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            ColumnString::Chars_t & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars_t> buf_to(data_to);

            for (size_t i = 0; i < size; ++i)
            {
                formatReadableSizeWithBinarySuffix(vec_from[i], buf_to);
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }
            data_to.resize(buf_to.count());

            block.getByPosition(result).column = std::move(col_to);
            return true;
        }

        return false;
    }
};

}
