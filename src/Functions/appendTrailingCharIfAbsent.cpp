#include <Columns/ColumnString.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <ext/range.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
    static constexpr auto name = "appendTrailingCharIfAbsent";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionAppendTrailingCharIfAbsent>();
    }

    String getName() const override
    {
        return name;
    }


private:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception{"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!isString(arguments[1]))
            throw Exception{"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const auto & column = block.getByPosition(arguments[0]).column;
        const auto & column_char = block.getByPosition(arguments[1]).column;

        if (!checkColumnConst<ColumnString>(column_char.get()))
            throw Exception{"Second argument of function " + getName() + " must be a constant string", ErrorCodes::ILLEGAL_COLUMN};

        String trailing_char_str = assert_cast<const ColumnConst &>(*column_char).getValue<String>();

        if (trailing_char_str.size() != 1)
            throw Exception{"Second argument of function " + getName() + " must be a one-character string", ErrorCodes::BAD_ARGUMENTS};

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            const auto & src_data = col->getChars();
            const auto & src_offsets = col->getOffsets();

            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();

            const auto size = src_offsets.size();
            dst_data.resize(src_data.size() + size);
            dst_offsets.resize(size);

            ColumnString::Offset src_offset{};
            ColumnString::Offset dst_offset{};

            for (const auto i : ext::range(0, size))
            {
                const auto src_length = src_offsets[i] - src_offset;
                memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
                src_offset = src_offsets[i];
                dst_offset += src_length;

                if (src_length > 1 && dst_data[dst_offset - 2] != trailing_char_str.front())
                {
                    dst_data[dst_offset - 1] = trailing_char_str.front();
                    dst_data[dst_offset] = 0;
                    ++dst_offset;
                }

                dst_offsets[i] = dst_offset;
            }

            dst_data.resize_assume_reserved(dst_offset);
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception{"Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }
};

void registerFunctionAppendTrailingCharIfAbsent(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
}

}
