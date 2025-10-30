#include <Columns/ColumnString.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Functions/formatString.h>
#include <IO/WriteHelpers.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

using namespace GatherUtils;

namespace
{

template <typename Name, bool is_injective>
class ConcatImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;
    explicit ConcatImpl(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<ConcatImpl>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const ColumnsWithTypeAndName &) const override { return is_injective; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() == 1)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Number of arguments for function {} should not be 1", getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.empty())
        {
            auto res_data = ColumnString::create();
            res_data->insertDefault();
            return ColumnConst::create(std::move(res_data), input_rows_count);
        }
        if (arguments.size() == 1)
            return arguments[0].column;
        /// Format function is not proven to be faster for two arguments.
        /// Actually there is overhead of 2 to 5 extra instructions for each string for checking empty strings in FormatImpl.
        /// Though, benchmarks are really close, for most examples we saw executeBinary is slightly faster (0-3%).
        /// For 3 and more arguments FormatStringImpl is much faster (up to 50-60%).
        if (arguments.size() == 2)
            return executeBinary(arguments, input_rows_count);
        return executeFormatImpl(arguments, input_rows_count);
    }

private:
    ContextWeakPtr context;

    ColumnPtr executeBinary(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const IColumn * c0 = arguments[0].column.get();
        const IColumn * c1 = arguments[1].column.get();

        const ColumnString * c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString * c1_string = checkAndGetColumn<ColumnString>(c1);
        const ColumnConst * c0_const_string = checkAndGetColumnConst<ColumnString>(c0);
        const ColumnConst * c1_const_string = checkAndGetColumnConst<ColumnString>(c1);

        auto col_res = ColumnString::create();

        if (c0_string && c1_string)
            concat(StringSource(*c0_string), StringSource(*c1_string), StringSink(*col_res, c0->size()));
        else if (c0_string && c1_const_string)
            concat(StringSource(*c0_string), ConstSource<StringSource>(*c1_const_string), StringSink(*col_res, c0->size()));
        else if (c0_const_string && c1_string)
            concat(ConstSource<StringSource>(*c0_const_string), StringSource(*c1_string), StringSink(*col_res, c0->size()));
        else
        {
            /// Fallback: use generic implementation for not very important cases.
            return executeFormatImpl(arguments, input_rows_count);
        }

        return col_res;
    }

    ColumnPtr executeFormatImpl(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const size_t num_arguments = arguments.size();
        assert(num_arguments >= 2);

        auto col_res = ColumnString::create();
        std::vector<const ColumnString::Chars *> data(num_arguments);
        std::vector<const ColumnString::Offsets *> offsets(num_arguments);
        std::vector<size_t> fixed_string_sizes(num_arguments);
        std::vector<std::optional<String>> constant_strings(num_arguments);
        std::vector<ColumnString::MutablePtr> converted_col_ptrs(num_arguments);
        bool has_column_string = false;
        bool has_column_fixed_string = false;
        for (size_t i = 0; i < num_arguments; ++i)
        {
            const ColumnPtr & column = arguments[i].column;
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
                fixed_string_sizes[i] = fixed_col->getN();
            }
            else if (const ColumnConst * const_col = checkAndGetColumnConstStringOrFixedString(column.get()))
            {
                constant_strings[i] = const_col->getValue<String>();
            }
            else
            {
                /// A non-String/non-FixedString-type argument: use the default serialization to convert it to String
                auto full_column = column->convertToFullIfNeeded();
                auto serialization = arguments[i].type->getDefaultSerialization();
                auto converted_col_str = ColumnString::create();
                ColumnStringHelpers::WriteHelper write_helper(*converted_col_str, column->size());
                auto & write_buffer = write_helper.getWriteBuffer();
                FormatSettings format_settings;
                for (size_t row = 0; row < column->size(); ++row)
                {
                    serialization->serializeText(*full_column, row, write_buffer, format_settings);
                    write_helper.rowWritten();
                }
                write_helper.finalize();

                /// Keep the pointer alive
                converted_col_ptrs[i] = std::move(converted_col_str);

                /// Same as the normal `ColumnString` branch
                has_column_string = true;
                data[i] = &converted_col_ptrs[i]->getChars();
                offsets[i] = &converted_col_ptrs[i]->getOffsets();
            }
        }

        String pattern;
        pattern.reserve(2 * num_arguments);

        for (size_t i = 0; i < num_arguments; ++i)
            pattern += "{}";

        FormatStringImpl::formatExecute(
            has_column_string,
            has_column_fixed_string,
            std::move(pattern),
            data,
            offsets,
            fixed_string_sizes,
            constant_strings,
            col_res->getChars(),
            col_res->getOffsets(),
            input_rows_count);

        return col_res;
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


/// Works with arrays via `arrayConcat`, maps via `mapConcat`, and tuples via `tupleConcat`.
/// Additionally, allows concatenation of arbitrary types that can be cast to string using the corresponding default serialization.
class ConcatOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "concat";
    static FunctionOverloadResolverPtr create(ContextPtr context) { return std::make_unique<ConcatOverloadResolver>(context); }

    explicit ConcatOverloadResolver(ContextPtr context_) : context(context_) { }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        if (arguments.size() == 1)
            return FunctionFactory::instance().getImpl("toString", context)->build(arguments);
        if (!arguments.empty() && std::ranges::all_of(arguments, [](const auto & elem) { return isArray(elem.type); }))
            return FunctionFactory::instance().getImpl("arrayConcat", context)->build(arguments);
        if (!arguments.empty() && std::ranges::all_of(arguments, [](const auto & elem) { return isMap(elem.type); }))
            return FunctionFactory::instance().getImpl("mapConcat", context)->build(arguments);
        if (!arguments.empty() && std::ranges::all_of(arguments, [](const auto & elem) { return isTuple(elem.type); }))
            return FunctionFactory::instance().getImpl("tupleConcat", context)->build(arguments);
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            FunctionConcat::create(context),
            DataTypes{std::from_range_t{}, arguments | std::views::transform([](auto & elem) { return elem.type; })},
            return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        /// We always return Strings from concat, even if arguments were fixed strings.
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(Concat)
{
    factory.registerFunction<ConcatOverloadResolver>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionConcatAssumeInjective>();
}

}
