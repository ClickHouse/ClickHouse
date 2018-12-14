#include <Common/config.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <IO/WriteHelpers.h>

#include <re2/re2.h>
#include <re2/stringpiece.h>

#if USE_RE2_ST
    #include <re2_st/re2.h> // Y_IGNORE
#else
    #define re2_st re2
#endif

namespace DB
{
using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const ColumnPtr column_string = block.getByPosition(arguments[0]).column;
        const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

        if (!input)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto dst_column = ColumnString::create();
        auto & dst_data = dst_column->getChars();
        auto & dst_offsets = dst_column->getOffsets();

        dst_data.resize(input->getChars().size() * input->size());
        dst_offsets.resize(input_rows_count);

        const ColumnString::Offsets & src_offsets = input->getOffsets();

        auto source = reinterpret_cast<const char *>(input->getChars().data());
        auto dst = reinterpret_cast<char *>(dst_data.data());
        auto dst_pos = dst;

        size_t src_offset_prev = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t srclen = src_offsets[row] - src_offset_prev - 1;

            /// suboptimal, but uses original implementation from re2
            re2_st::StringPiece unquoted(source, srclen);
            const auto & quoted = re2_st::RE2::QuoteMeta(unquoted);
            std::memcpy(dst_pos, quoted.data(), quoted.size());

            source += srclen + 1;
            dst_pos += quoted.size() + 1;

            dst_offsets[row] = dst_pos - dst;
            src_offset_prev = src_offsets[row];
        }

        dst_data.resize(dst_pos - dst);

        block.getByPosition(result).column = std::move(dst_column);
    }

};

void registerFunctionRegexpQuoteMeta(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRegexpQuoteMeta>();
}
}
