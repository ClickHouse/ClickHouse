#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/range.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionAppendTrailingCharIfAbsent : public IFunction
{
public:
    static constexpr auto name = "appendTrailingCharIfAbsent";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionAppendTrailingCharIfAbsent>();
    }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }


private:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of the first argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of the second argument of function {}", arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & column = arguments[0].column;
        const auto & column_char = arguments[1].column;

        if (!checkColumnConst<ColumnString>(column_char.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be a constant string", getName());

        String trailing_char_str = assert_cast<const ColumnConst &>(*column_char).getValue<String>();

        if (trailing_char_str.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument of function {} must be a one-character string", getName());

        if (const auto * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            const auto & src_data = col->getChars();
            const auto & src_offsets = col->getOffsets();

            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();

            dst_data.resize(src_data.size() + input_rows_count);
            dst_offsets.resize(input_rows_count);

            ColumnString::Offset src_offset{};
            ColumnString::Offset dst_offset{};

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto src_length = src_offsets[i] - src_offset;
                memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
                src_offset = src_offsets[i];
                dst_offset += src_length;

                if (src_length > 1 && dst_data[dst_offset - 2] != static_cast<UInt8>(trailing_char_str.front()))
                {
                    dst_data[dst_offset - 1] = trailing_char_str.front();
                    dst_data[dst_offset] = 0;
                    ++dst_offset;
                }

                dst_offsets[i] = dst_offset;
            }

            dst_data.resize_assume_reserved(dst_offset);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

}

REGISTER_FUNCTION(AppendTrailingCharIfAbsent)
{
    factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
}

}
