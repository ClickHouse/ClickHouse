#pragma once
#include <base/map.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/IFunction.h>
#include <Functions/PerformanceAdaptors.h>
#include <Functions/TargetSpecific.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>

namespace DB
{

using namespace GatherUtils;

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct NameStartsWith
{
    static constexpr auto name = "startsWith";
};
struct NameEndsWith
{
    static constexpr auto name = "endsWith";
};

DECLARE_MULTITARGET_CODE(

template <typename Name>
class FunctionStartsEndsWith : public IFunction
{
public:
    static constexpr auto name = Name::name;

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (isStringOrFixedString(arguments[0]) && isStringOrFixedString(arguments[1]))
            return std::make_shared<DataTypeUInt8>();

        if (isArray(arguments[0]) && isArray(arguments[1]))
            return std::make_shared<DataTypeUInt8>();

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types {} {} of arguments of function {}. Both must be String or Array",
            arguments[0]->getName(), arguments[1]->getName(), getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto data_type = arguments[0].type;
        if (isStringOrFixedString(*data_type))
            return executeImplString(arguments, {}, input_rows_count);
        if (isArray(data_type))
            return executeImplArray(arguments, {}, input_rows_count);
        return {};
    }

private:
    ColumnPtr executeImplArray(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        DataTypePtr common_type = getLeastSupertype(collections::map(arguments, [](auto & arg) { return arg.type; }));

        Columns preprocessed_columns(2);
        for (size_t i = 0; i < 2; ++i)
            preprocessed_columns[i] = castColumn(arguments[i], common_type);

        std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;
        for (auto & argument_column : preprocessed_columns)
        {
            bool is_const = false;

            if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
            {
                is_const = true;
                argument_column = argument_column_const->getDataColumnPtr();
            }

            if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
                sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
            else
                throw Exception{"Arguments for function " + getName() + " must be arrays.", ErrorCodes::LOGICAL_ERROR};
        }

        auto result_column = ColumnUInt8::create(input_rows_count);
        auto * result_column_ptr = typeid_cast<ColumnUInt8 *>(result_column.get());

        if constexpr (std::is_same_v<Name, NameStartsWith>)
            GatherUtils::sliceHas(*sources[0], *sources[1], GatherUtils::ArraySearchType::StartsWith, *result_column_ptr);
        else
            GatherUtils::sliceHas(*sources[0], *sources[1], GatherUtils::ArraySearchType::EndsWith, *result_column_ptr);

        return result_column;
    }

    ColumnPtr executeImplString(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
    {
        const IColumn * haystack_column = arguments[0].column.get();
        const IColumn * needle_column = arguments[1].column.get();

        auto col_res = ColumnVector<UInt8>::create();
        typename ColumnVector<UInt8>::Container & vec_res = col_res->getData();

        vec_res.resize(input_rows_count);

        if (const ColumnString * haystack = checkAndGetColumn<ColumnString>(haystack_column))
            dispatch<StringSource>(StringSource(*haystack), needle_column, vec_res);
        else if (const ColumnFixedString * haystack_fixed = checkAndGetColumn<ColumnFixedString>(haystack_column))
            dispatch<FixedStringSource>(FixedStringSource(*haystack_fixed), needle_column, vec_res);
        else if (const ColumnConst * haystack_const = checkAndGetColumnConst<ColumnString>(haystack_column))
            dispatch<ConstSource<StringSource>>(ConstSource<StringSource>(*haystack_const), needle_column, vec_res);
        else if (const ColumnConst * haystack_const_fixed = checkAndGetColumnConst<ColumnFixedString>(haystack_column))
            dispatch<ConstSource<FixedStringSource>>(ConstSource<FixedStringSource>(*haystack_const_fixed), needle_column, vec_res);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        return col_res;
    }

    template <typename HaystackSource>
    void dispatch(HaystackSource haystack_source, const IColumn * needle_column, PaddedPODArray<UInt8> & res_data) const
    {
        if (const ColumnString * needle = checkAndGetColumn<ColumnString>(needle_column))
            execute<HaystackSource, StringSource>(haystack_source, StringSource(*needle), res_data);
        else if (const ColumnFixedString * needle_fixed = checkAndGetColumn<ColumnFixedString>(needle_column))
            execute<HaystackSource, FixedStringSource>(haystack_source, FixedStringSource(*needle_fixed), res_data);
        else if (const ColumnConst * needle_const = checkAndGetColumnConst<ColumnString>(needle_column))
            execute<HaystackSource, ConstSource<StringSource>>(haystack_source, ConstSource<StringSource>(*needle_const), res_data);
        else if (const ColumnConst * needle_const_fixed = checkAndGetColumnConst<ColumnFixedString>(needle_column))
            execute<HaystackSource, ConstSource<FixedStringSource>>(haystack_source, ConstSource<FixedStringSource>(*needle_const_fixed), res_data);
        else
            throw Exception("Illegal combination of columns as arguments of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename HaystackSource, typename NeedleSource>
    static void execute(HaystackSource haystack_source, NeedleSource needle_source, PaddedPODArray<UInt8> & res_data)
    {
        size_t row_num = 0;

        while (!haystack_source.isEnd())
        {
            auto haystack = haystack_source.getWhole();
            auto needle = needle_source.getWhole();

            if (needle.size > haystack.size)
            {
                res_data[row_num] = false;
            }
            else
            {
                if constexpr (std::is_same_v<Name, NameStartsWith>)
                {
                    res_data[row_num] = StringRef(haystack.data, needle.size) == StringRef(needle.data, needle.size);
                }
                else    /// endsWith
                {
                    res_data[row_num] = StringRef(haystack.data + haystack.size - needle.size, needle.size) == StringRef(needle.data, needle.size);
                }
            }

            haystack_source.next();
            needle_source.next();
            ++row_num;
        }
    }
};

) // DECLARE_MULTITARGET_CODE

template <typename Name>
class FunctionStartsEndsWith : public TargetSpecific::Default::FunctionStartsEndsWith<Name>
{
public:
    explicit FunctionStartsEndsWith(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionStartsEndsWith<Name>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::SSE42,
            TargetSpecific::SSE42::FunctionStartsEndsWith<Name>>();
        selector.registerImplementation<TargetArch::AVX,
            TargetSpecific::AVX::FunctionStartsEndsWith<Name>>();
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionStartsEndsWith<Name>>();
        selector.registerImplementation<TargetArch::AVX512F,
            TargetSpecific::AVX512F::FunctionStartsEndsWith<Name>>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionStartsEndsWith<Name>>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

}
