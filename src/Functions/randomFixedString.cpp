#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/PerformanceAdaptors.h>
#include <Functions/FunctionsRandom.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <common/arithmeticOverflow.h>
#include <common/unaligned.h>

#include <common/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
}


/* Generate random fixed string with fully random bytes (including zero). */
template <typename RandImpl>
class FunctionRandomFixedStringImpl : public IFunction
{
public:
    static constexpr auto name = "randomFixedString";

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isUnsignedInteger(arguments[0].type))
            throw Exception("First argument for function " + getName() + " must be unsigned integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[0].column || !isColumnConst(*arguments[0].column))
            throw Exception("First argument for function " + getName() + " must be constant", ErrorCodes::ILLEGAL_COLUMN);

        const size_t n = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<UInt64>();
        return std::make_shared<DataTypeFixedString>(n);
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const size_t n = assert_cast<const ColumnConst &>(*block.getByPosition(arguments[0]).column).getValue<UInt64>();

        auto col_to = ColumnFixedString::create(n);
        ColumnFixedString::Chars & data_to = col_to->getChars();

        if (input_rows_count == 0)
        {
            block.getByPosition(result).column = std::move(col_to);
            return;
        }

        size_t total_size;
        if (common::mulOverflow(input_rows_count, n, total_size))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);

        /// Fill random bytes.
        data_to.resize(total_size);
        RandImpl::execute(reinterpret_cast<char *>(data_to.data()), total_size);

        block.getByPosition(result).column = std::move(col_to);
    }
};

class FunctionRandomFixedString : public FunctionRandomFixedStringImpl<TargetSpecific::Default::RandImpl>
{
public:
    explicit FunctionRandomFixedString(const Context & context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            FunctionRandomFixedStringImpl<TargetSpecific::Default::RandImpl>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            FunctionRandomFixedStringImpl<TargetSpecific::AVX2::RandImpl>>();
    #endif
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        selector.selectAndExecute(block, arguments, result, input_rows_count);
    }

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRandomFixedString>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

void registerFunctionRandomFixedString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomFixedString>();
}

}
