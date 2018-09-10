#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnNumbers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


/** If the string is encoded in UTF-8, then it selects a substring of code points in it.
  * Otherwise, the behavior is undefined.
  */
struct SubstringUTF8Impl
{
    static void vector(const ColumnString::Chars_t & data,
        const ColumnString::Offsets & offsets,
        size_t start,
        size_t length,
        ColumnString::Chars_t & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.reserve(data.size());
        size_t size = offsets.size();
        res_offsets.resize(size);

        ColumnString::Offset prev_offset = 0;
        ColumnString::Offset res_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            ColumnString::Offset pos = 1;
            ColumnString::Offset bytes_start = 0;
            ColumnString::Offset bytes_length = 0;
            while (j < offsets[i] - 1)
            {
                if (pos == start)
                    bytes_start = j - prev_offset + 1;

                if (data[j] < 0xBF)
                    j += 1;
                else if (data[j] < 0xE0)
                    j += 2;
                else if (data[j] < 0xF0)
                    j += 3;
                else
                    j += 1;

                if (pos >= start && pos < start + length)
                    bytes_length = j - prev_offset + 1 - bytes_start;
                else if (pos >= start + length)
                    break;

                ++pos;
            }

            if (bytes_start == 0)
            {
                res_data.resize(res_data.size() + 1);
                res_data[res_offset] = 0;
                ++res_offset;
            }
            else
            {
                size_t bytes_to_copy = std::min(offsets[i] - prev_offset - bytes_start, bytes_length);
                res_data.resize(res_data.size() + bytes_to_copy + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], &data[prev_offset + bytes_start - 1], bytes_to_copy);
                res_offset += bytes_to_copy + 1;
                res_data[res_offset - 1] = 0;
            }
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};


class FunctionSubstringUTF8 : public IFunction
{
public:
    static constexpr auto name = "substringUTF8";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionSubstringUTF8>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isNumber(arguments[1]) || !isNumber(arguments[2]))
            throw Exception("Illegal type " + (isNumber(arguments[1]) ? arguments[2]->getName() : arguments[1]->getName())
                    + " of argument of function "
                    + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_start = block.getByPosition(arguments[1]).column;
        const ColumnPtr column_length = block.getByPosition(arguments[2]).column;

        if (!column_start->isColumnConst() || !column_length->isColumnConst())
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.");

        Field start_field = (*block.getByPosition(arguments[1]).column)[0];
        Field length_field = (*block.getByPosition(arguments[2]).column)[0];

        if (start_field.getType() != Field::Types::UInt64 || length_field.getType() != Field::Types::UInt64)
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be non-negative and must have UInt type.");

        UInt64 start = start_field.get<UInt64>();
        UInt64 length = length_field.get<UInt64>();

        if (start == 0)
            throw Exception("Second argument of function substring must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        /// Otherwise may lead to overflow and pass bounds check inside inner loop.
        if (start >= 0x8000000000000000ULL || length >= 0x8000000000000000ULL)
            throw Exception("Too large values of 2nd or 3rd argument provided for function substring.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_string.get()))
        {
            auto col_res = ColumnString::create();
            SubstringUTF8Impl::vector(col->getChars(), col->getOffsets(), start, length, col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionSubstringUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSubstringUTF8>();
}

}
