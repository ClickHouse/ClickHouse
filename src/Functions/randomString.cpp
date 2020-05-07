#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Common/thread_local_rng.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}


/* Generate random string of specified length with fully random bytes(including zero). */
class FunctionRandomString : public IFunction
{
public:
    static constexpr auto name = "randomString";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionRandomString>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                "Function " + getName() + " requires at least one argument: the size of resulting string",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > 2)
            throw Exception(
                "Function " + getName() + " requires at most two arguments: the size of resulting string and optional disambiguation tag",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const IDataType & length_type = *arguments[0];
        if (!isNumber(length_type))
            throw Exception("First argument of function " + getName() + " must have numeric type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto col_to = ColumnString::create();
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        const IColumn & length_column = *block.getByPosition(arguments[0]).column;

        IColumn::Offset offset = 0;

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t length = length_column.getUInt(row_num);
            if (length > (1 << 30))
                throw Exception("Too large string size in function " + getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);


            IColumn::Offset next_offset = offset + length + 1;
            data_to.resize(next_offset);
            offsets_to[row_num] = next_offset;

            auto * data_to_ptr = data_to.data(); // avoid assert on array indexing after end
            for (size_t pos = offset, end = offset + length; pos < end;
                 pos += 8) // We have padding in column buffers that we can overwrite.
            {
                UInt64 rand = thread_local_rng();
                data_to_ptr[pos] = rand;
            }

            data_to[offset + length] = 0;

            offset = next_offset;
        }

        block.getByPosition(result).column = std::move(col_to);
    }
};

void registerFunctionRandomString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomString>();
}

}
