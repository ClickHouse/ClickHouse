#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Slices.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <ext/map.h>
#include <ext/range.h>

#include "formatString.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

using namespace GatherUtils;


template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    ConcatImpl(const Context & context_) : context(context_) {}
    static FunctionPtr create(const Context & context) { return std::make_shared<ConcatImpl>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > FormatImpl::argument_threshold)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most " + std::to_string(FormatImpl::argument_threshold),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto arg = arguments[arg_idx].get();
            if (!isStringOrFixedString(arg))
                throw Exception{"Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function "
                                    + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        /// Format function is not proven to be faster for two arguments.
        /// Actually there is overhead of 2 to 5 extra instructions for each string for checking empty strings in FormatImpl.
        /// Though, benchmarks are really close, for most examples we saw executeBinary is slightly faster (0-3%).
        /// For 3 and more arguments FormatImpl is much faster (up to 50-60%).
        if (arguments.size() == 2)
            executeBinary(block, arguments, result, input_rows_count);
        else
            executeFormatImpl(block, arguments, result, input_rows_count);
    }

private:
    const Context & context;

    void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result, size_t input_rows_count)
    {
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
        const IColumn * c1 = block.getByPosition(arguments[1]).column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto c_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*c_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*c_res, c0->size()));
        else
        {
            /// Fallback: use generic implementation for not very important cases.
            executeFormatImpl(block, arguments, result, input_rows_count);
            return;
        }

        block.getByPosition(result).column = std::move(c_res);
    }

    void executeFormatImpl(Block & block, const ColumnNumbers & arguments, const size_t result, size_t input_rows_count)
    {
        auto c_res = ColumnString::create();
        std::vector<const ColumnString::Chars *> data(arguments.size());
        std::vector<const ColumnString::Offsets *> offsets(arguments.size());
        std::vector<size_t> fixed_string_N(arguments.size());
        std::vector<String> constant_strings(arguments.size());
        bool has_column_string = false;
        bool has_column_fixed_string = false;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const ColumnPtr & column = block.getByPosition(arguments[i]).column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                has_column_string = true;
                data[i] = &col->getChars();
                offsets[i] = &col->getOffsets();
            }
            else if (const ColumnFixedString * fixed_col = checkAndGetColumn<ColumnFixedString>(column.get()))
            {
                has_column_fixed_string = true;
                data[i] = &fixed_col->getChars();
                fixed_string_N[i] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[i] = const_col->getValue<String>();
            }
            else
                throw Exception(
                    "Illegal column " + column->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
        }

        String pattern;
        pattern.reserve(2 * arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
            pattern += "{}";

        FormatImpl::formatExecute(
            has_column_string,
            has_column_fixed_string,
            std::move(pattern),
            data,
            offsets,
            fixed_string_N,
            constant_strings,
            c_res->getChars(),
            c_res->getOffsets(),
            input_rows_count);

        block.getByPosition(result).column = std::move(c_res);
    }
};


struct NameConcat
{
    static constexpr auto name = "concat";
};
struct NameConcatAssumeInjective
{
    static constexpr auto name = "concatAssumeInjective";
};

using FunctionConcat = ConcatImpl<NameConcat, false>;
using FunctionConcatAssumeInjective = ConcatImpl<NameConcatAssumeInjective, true>;


/// Also works with arrays.
class FunctionBuilderConcat : public FunctionBuilderImpl
{
public:
    static constexpr auto name = "concat";
    static FunctionBuilderPtr create(const Context & context) { return std::make_shared<FunctionBuilderConcat>(context); }

    FunctionBuilderConcat(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (isArray(arguments.at(0).type))
            return FunctionFactory::instance().get("arrayConcat", context)->build(arguments);
        else
            return std::make_shared<DefaultFunction>(
                FunctionConcat::create(context), ext::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }), return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// We always return Strings from concat, even if arguments were fixed strings.
        return std::make_shared<DataTypeString>();
    }

private:
    const Context & context;
};


void registerFunctionsConcat(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderConcat>(FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionConcatAssumeInjective>();
}

}
