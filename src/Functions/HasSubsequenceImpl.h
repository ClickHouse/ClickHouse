#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/Sinks.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

using namespace GatherUtils;

template <typename Name, typename Impl>
class HasSubsequenceImpl : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<HasSubsequenceImpl>(); }

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
                "Illegal columns {} and {} of arguments of function {}",
                arguments[0].column->getName(),
                arguments[1].column->getName(),
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
        while (!haystacks.isEnd())
        {
            auto haystack_slice = haystacks.getWhole();
            auto needle_slice = needles.getWhole();
            size_t row_num = haystacks.rowNum();

            if constexpr (!Impl::is_utf8)
                res_data[row_num] = hasSubsequence(haystack_slice.data, haystack_slice.size, needle_slice.data, needle_slice.size);
            else
                res_data[row_num] = hasSubsequenceUTF8(haystack_slice.data, haystack_slice.size, needle_slice.data, needle_slice.size);

            haystacks.next();
            needles.next();
        }
    }

    static UInt8 hasSubsequence(const UInt8 * haystack, size_t haystack_size, const UInt8 * needle, size_t needle_size)
    {
        size_t j = 0;
        for (size_t i = 0; (i < haystack_size) && (j < needle_size); i++)
            if (Impl::toLowerIfNeed(needle[j]) == Impl::toLowerIfNeed(haystack[i]))
                ++j;
        return j == needle_size;
    }

    static UInt8 hasSubsequenceUTF8(const UInt8 * haystack, size_t haystack_size, const UInt8 * needle, size_t needle_size)
    {
        const auto * haystack_pos = haystack;
        const auto * needle_pos = needle;
        const auto * haystack_end = haystack + haystack_size;
        const auto * needle_end = needle + needle_size;

        if (!needle_size)
            return 1;

        auto haystack_code_point = UTF8::convertUTF8ToCodePoint(haystack_pos, haystack_end - haystack_pos);
        auto needle_code_point = UTF8::convertUTF8ToCodePoint(needle_pos, needle_end - needle_pos);
        if (!haystack_code_point || !needle_code_point)
            return 0;

        while (haystack_code_point && needle_code_point)
        {
            if (Impl::toLowerIfNeed(*needle_code_point) == Impl::toLowerIfNeed(*haystack_code_point))
            {
                needle_pos += UTF8::seqLength(*needle_pos);
                if (needle_pos >= needle_end)
                    break;
                needle_code_point = UTF8::convertUTF8ToCodePoint(needle_pos, needle_end - needle_pos);
            }
            haystack_pos += UTF8::seqLength(*haystack_pos);
            if (haystack_pos >= haystack_end)
                break;
            haystack_code_point = UTF8::convertUTF8ToCodePoint(haystack_pos, haystack_end - haystack_pos);
        }
        return needle_pos == needle_end;
    }
};

}
