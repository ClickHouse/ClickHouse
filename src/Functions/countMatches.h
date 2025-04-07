#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/Regexps.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

using Pos = const char *;

template <class CountMatchesBase>
class FunctionCountMatches : public IFunction
{
public:
    static constexpr auto name = CountMatchesBase::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCountMatches<CountMatchesBase>>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"haystack", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"},
            {"pattern", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), isColumnConst, "constant String"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IColumn * col_pattern = arguments[1].column.get();
        const ColumnConst * col_pattern_const = checkAndGetColumnConst<ColumnString>(col_pattern);
        if (col_pattern_const == nullptr)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Pattern argument is not const");

        const OptimizedRegularExpression re = Regexps::createRegexp</*is_like*/ false, /*no_capture*/ true, CountMatchesBase::case_insensitive>(col_pattern_const->getValue<String>());

        const IColumn * col_haystack = arguments[0].column.get();
        OptimizedRegularExpression::MatchVec matches;

        if (const ColumnConst * col_haystack_const = checkAndGetColumnConstStringOrFixedString(col_haystack))
        {
            std::string_view str = col_haystack_const->getDataColumn().getDataAt(0).toView();
            uint64_t matches_count = countMatches(str, re, matches);
            return result_type->createColumnConst(input_rows_count, matches_count);
        }
        if (const ColumnString * col_haystack_string = checkAndGetColumn<ColumnString>(col_haystack))
        {
            auto col_res = ColumnUInt64::create();

            const ColumnString::Chars & src_chars = col_haystack_string->getChars();
            const ColumnString::Offsets & src_offsets = col_haystack_string->getOffsets();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            ColumnString::Offset current_src_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
                current_src_offset = src_offsets[i];
                Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]) - 1;

                std::string_view str(pos, end - pos);
                vec_res[i] = countMatches(str, re, matches);
            }

            return col_res;
        }
        if (const ColumnFixedString * col_haystack_fixedstring = checkAndGetColumn<ColumnFixedString>(col_haystack))
        {
            auto col_res = ColumnUInt64::create();

            ColumnUInt64::Container & vec_res = col_res->getData();
            vec_res.resize(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                std::string_view str = col_haystack_fixedstring->getDataAt(i).toView();
                vec_res[i] = countMatches(str, re, matches);
            }

            return col_res;
        }
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Could not cast haystack argument to String or FixedString");
    }

    static uint64_t countMatches(std::string_view src, const OptimizedRegularExpression & re, OptimizedRegularExpression::MatchVec & matches)
    {
        /// Only one match is required, no need to copy more.
        static const unsigned matches_limit = 1;

        Pos pos = reinterpret_cast<Pos>(src.data());
        Pos end = reinterpret_cast<Pos>(src.data() + src.size());

        uint64_t match_count = 0;
        while (true)
        {
            if (pos >= end)
                break;
            if (!re.match(pos, end - pos, matches, matches_limit))
                break;
            /// Progress should be made, but with empty match the progress will not be done.
            /// Also note that simply check is pattern empty is not enough,
            /// since for example "'[f]{0}'" will match zero bytes:
            if (!matches[0].length)
                break;
            pos += matches[0].offset + matches[0].length;
            ++match_count;
        }

        return match_count;
    }
};

}
