#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/PerformanceAdaptors.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <common/unaligned.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}


/* Generate random string of specified length with fully random bytes (including zero). */
template <typename RandImpl>
class FunctionRandomStringImpl : public IFunction
{
public:
    static constexpr auto name = "randomString";

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        auto col_to = ColumnString::create();
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        if (input_rows_count == 0)
        {
            block.getByPosition(result).column = std::move(col_to);
            return;
        }

        /// Fill offsets.
        offsets_to.resize(input_rows_count);
        const IColumn & length_column = *block.getByPosition(arguments[0]).column;

        IColumn::Offset offset = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t length = length_column.getUInt(row_num);
            if (length > (1 << 30))
                throw Exception("Too large string size in function " + getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);

            offset += length + 1;
            offsets_to[row_num] = offset;
        }

        /// Fill random bytes.
        data_to.resize(offsets_to.back());
        RandImpl::execute(reinterpret_cast<char *>(data_to.data()), data_to.size());

        /// Put zero bytes in between.
        auto * pos = data_to.data();
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            pos[offsets_to[row_num] - 1] = 0;

        block.getByPosition(result).column = std::move(col_to);
    }
};

class FunctionRandomString : public FunctionRandomStringImpl<TargetSpecific::Default::RandImpl>
{
public:
    explicit FunctionRandomString(const Context & context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            FunctionRandomStringImpl<TargetSpecific::Default::RandImpl>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomStringImpl<TargetSpecific::AVX2::RandImpl>>();
    #endif
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        selector.selectAndExecute(block, arguments, result, input_rows_count);
    }

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRandomString>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

void registerFunctionRandomString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomString>();
}

}
