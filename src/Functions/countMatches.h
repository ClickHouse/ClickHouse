#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/Regexps.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isStringOrFixedString(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument (pattern) of function {}. Must be String/FixedString.",
                arguments[1].type->getName(), getName());
        if (!isStringOrFixedString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument (haystack) of function {}. Must be String/FixedString.",
                arguments[0].type->getName(), getName());
        const auto * column = arguments[1].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The second argument of function {} should be a constant string with the pattern",
                getName());

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnConst * column_pattern = checkAndGetColumnConstStringOrFixedString(arguments[1].column.get());
        Regexps::Pool::Pointer re = Regexps::get<false /* like */, true /* is_no_capture */, CountMatchesBase::case_insensitive>(column_pattern->getValue<String>());
        OptimizedRegularExpression::MatchVec matches;

        const IColumn * column_haystack = arguments[0].column.get();

        if (const ColumnString * col_str = checkAndGetColumn<ColumnString>(column_haystack))
        {
            auto result_column = ColumnUInt64::create();

            const ColumnString::Chars & src_chars = col_str->getChars();
            const ColumnString::Offsets & src_offsets = col_str->getOffsets();

            ColumnUInt64::Container & vec_res = result_column->getData();
            vec_res.resize(input_rows_count);

            size_t size = src_offsets.size();
            ColumnString::Offset current_src_offset = 0;

            for (size_t i = 0; i < size; ++i)
            {
                Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
                current_src_offset = src_offsets[i];
                Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]) - 1;

                StringRef str(pos, end - pos);
                vec_res[i] = countMatches(str, re, matches);
            }

            return result_column;
        }
        else if (const ColumnConst * col_const_str = checkAndGetColumnConstStringOrFixedString(column_haystack))
        {
            StringRef str = col_const_str->getDataColumn().getDataAt(0);
            uint64_t matches_count = countMatches(str, re, matches);
            return result_type->createColumnConst(input_rows_count, matches_count);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Error in FunctionCountMatches::getReturnTypeImpl()");
    }

    static uint64_t countMatches(StringRef src, Regexps::Pool::Pointer & re, OptimizedRegularExpression::MatchVec & matches)
    {
        /// Only one match is required, no need to copy more.
        static const unsigned matches_limit = 1;

        Pos pos = reinterpret_cast<Pos>(src.data);
        Pos end = reinterpret_cast<Pos>(src.data + src.size);

        uint64_t match_count = 0;
        while (true)
        {
            if (pos >= end)
                break;
            if (!re->match(pos, end - pos, matches, matches_limit))
                break;
            /// Progress should be made, but with empty match the progress will not be done.
            /// Also note that simply check is pattern empty is not enough,
            /// since for example "'[f]{0}'" will match zero bytes:
            if (!matches[0].length)
                break;
            pos += matches[0].offset + matches[0].length;
            match_count++;
        }

        return match_count;
    }
};

}
