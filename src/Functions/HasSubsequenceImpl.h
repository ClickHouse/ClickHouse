#pragma once

#include <Columns/ColumnString.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Functions/GatherUtils/Sinks.h>
namespace DB
{
namespace
{

using namespace GatherUtils;

template <typename Name, typename Impl>
class FunctionsHasSubsequenceImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsHasSubsequenceImpl>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {};}

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & column_needle = arguments[1].column;

        const ColumnConst * haystack_const_string = checkAndGetColumnConst<ColumnString>(column_haystack.get());
        const ColumnConst * needle_const_string = checkAndGetColumnConst<ColumnString>(column_needle.get());
        const ColumnString * haystack_string = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnString * needle_string = checkAndGetColumn<ColumnString>(&*column_needle);

        auto col_res = ColumnVector<UInt8>::create();
        typename ColumnVector<UInt8>::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        if (haystack_string && needle_string)
            execute(StringSource{*haystack_string}, StringSource{*needle_string}, vec_res);
        else if (haystack_string && needle_const_string)
            execute(StringSource{*haystack_string}, ConstSource<StringSource>{*needle_const_string}, vec_res);
        else if (haystack_const_string && needle_string)
            execute(ConstSource<StringSource>{*haystack_const_string}, StringSource{*needle_string}, vec_res);
        else if (haystack_const_string && needle_const_string)
            execute(ConstSource<StringSource>{*haystack_const_string}, ConstSource<StringSource>{*needle_const_string}, vec_res);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {}, first argument of function {} must be a string",
                arguments[0].column->getName(),
                getName());

        return col_res;
    }

private:

    template <typename SourceHaystack, typename SourceNeedle>
    void execute(
        SourceHaystack && haystacks,
        SourceNeedle && needles,
        PaddedPODArray<UInt8> & res_data) const
    {
        size_t row_num = 0;

        while (!haystacks.isEnd())
        {
            [[maybe_unused]] auto haystack_slice = haystacks.getWhole();
            [[maybe_unused]] auto needle_slice = needles.getWhole();

            auto haystack = std::string(reinterpret_cast<const char *>(haystack_slice.data), haystack_slice.size);
            auto needle = std::string(reinterpret_cast<const char *>(needle_slice.data), needle_slice.size);

            Impl::toLowerIfNeed(haystack);
            Impl::toLowerIfNeed(needle);

            res_data[row_num] = hasSubsequence(haystack.c_str(), haystack.size(), needle.c_str(), needle.size());
            haystacks.next();
            needles.next();
            ++row_num;
        }
    }

    static UInt8 hasSubsequence(const char * haystack, size_t haystack_size, const char * needle, size_t needle_size)
    {
        size_t j = 0;
        for (size_t i = 0; (i < haystack_size) && (j < needle_size); i++)
            if (needle[j] == haystack[i])
                ++j;
        return j == needle_size;
    }
};

}

}
