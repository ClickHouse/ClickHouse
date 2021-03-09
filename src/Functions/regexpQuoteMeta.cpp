#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <common/find_symbols.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionRegexpQuoteMeta : public IFunction
{
public:
    static constexpr auto name = "regexpQuoteMeta";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionRegexpQuoteMeta>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!WhichDataType(arguments[0].type).isString())
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName() + ". Must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const ColumnPtr & column_string = block.getByPosition(arguments[0]).column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

        if (!input)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto dst_column = ColumnString::create();
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input->getOffsets();

        const auto * src_begin = reinterpret_cast<const char *>(input->getChars().data());
        const auto * src_pos = src_begin;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            /// NOTE This implementation slightly differs from re2::RE2::QuoteMeta.
            /// It escapes zero byte as \0 instead of \x00
            ///  and it escapes only required characters.
            /// This is Ok. Look at comments in re2.cc

            const char * src_end = src_begin + src_offsets[row_idx] - 1;

            while (true)
            {
                const char * next_src_pos = find_first_symbols<'\0', '\\', '|', '(', ')', '^', '$', '.', '[', ']', '?', '*', '+', '{', ':', '-'>(src_pos, src_end);

                size_t bytes_to_copy = next_src_pos - src_pos;
                size_t old_dst_size = dst_data.size();
                dst_data.resize(old_dst_size + bytes_to_copy);
                memcpySmallAllowReadWriteOverflow15(dst_data.data() + old_dst_size, src_pos, bytes_to_copy);
                src_pos = next_src_pos + 1;

                if (next_src_pos == src_end)
                {
                    dst_data.emplace_back('\0');
                    break;
                }

                dst_data.emplace_back('\\');
                dst_data.emplace_back(*next_src_pos);
            }

            dst_offsets[row_idx] = dst_data.size();
        }

        block.getByPosition(result).column = std::move(dst_column);
    }

};

void registerFunctionRegexpQuoteMeta(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRegexpQuoteMeta>();
}
}
