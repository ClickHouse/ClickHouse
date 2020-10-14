#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>

namespace DB
{
/** Search and replace functions in strings:
  *
  * position(haystack, needle)     - the normal search for a substring in a string, returns the position (in bytes) of the found substring starting with 1, or 0 if no substring is found.
  * positionUTF8(haystack, needle) - the same, but the position is calculated at code points, provided that the string is encoded in UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, pattern)        - search by the regular expression LIKE; Returns 0 or 1. Case-insensitive, but only for Latin.
  * notLike(haystack, pattern)
  *
  * match(haystack, pattern)       - search by regular expression re2; Returns 0 or 1.
  * multiMatchAny(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- search by re2 regular expressions pattern_i; Returns 0 or 1 if any pattern_i matches.
  * multiMatchAnyIndex(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- search by re2 regular expressions pattern_i; Returns index of any match or zero if none;
  * multiMatchAllIndices(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- search by re2 regular expressions pattern_i; Returns an array of matched indices in any order;
  *
  * Applies regexp re2 and pulls:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, pattern)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearch>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return Impl::supports_start_pos; }

    size_t getNumberOfArguments() const override
    {
        if (Impl::supports_start_pos)
            return 0;
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return Impl::use_default_implementation_for_constants; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if (!Impl::use_default_implementation_for_constants)
            return ColumnNumbers{};
        if (!Impl::supports_start_pos)
            return ColumnNumbers{1, 2};
        return ColumnNumbers{1, 2, 3};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || 3 < arguments.size())
            throw Exception("Number of arguments for function " + String(Name::name) + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() >= 3)
        {
            if (!isUnsignedInteger(arguments[2]))
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = columns[arguments[0]].column;
        const ColumnPtr & column_needle = columns[arguments[1]].column;

        ColumnPtr column_start_pos = nullptr;
        if (arguments.size() >= 3)
            column_start_pos = columns[arguments[2]].column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if constexpr (!Impl::use_default_implementation_for_constants)
        {
            bool is_col_start_pos_const = column_start_pos == nullptr || isColumnConst(*column_start_pos);
            if (col_haystack_const && col_needle_const)
            {
                auto col_res = ColumnVector<ResultType>::create();
                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(is_col_start_pos_const ? 1 : column_start_pos->size());

                Impl::constantConstant(
                    col_haystack_const->getValue<String>(),
                    col_needle_const->getValue<String>(),
                    column_start_pos,
                    vec_res);

                if (is_col_start_pos_const)
                    columns[result].column
                        = columns[result].type->createColumnConst(col_haystack_const->size(), toField(vec_res[0]));
                else
                    columns[result].column = std::move(col_res);

                return;
            }
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnFixedString * col_haystack_vector_fixed = checkAndGetColumn<ColumnFixedString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vectorVector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                column_start_pos,
                vec_res);
        else if (col_haystack_vector && col_needle_const)
            Impl::vectorConstant(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_const->getValue<String>(),
                column_start_pos,
                vec_res);
        else if (col_haystack_vector_fixed && col_needle_const)
            Impl::vectorFixedConstant(
                col_haystack_vector_fixed->getChars(),
                col_haystack_vector_fixed->getN(),
                col_needle_const->getValue<String>(),
                vec_res);
        else if (col_haystack_const && col_needle_vector)
            Impl::constantVector(
                col_haystack_const->getValue<String>(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                column_start_pos,
                vec_res);
        else
            throw Exception(
                "Illegal columns " + columns[arguments[0]].column->getName() + " and "
                    + columns[arguments[1]].column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        columns[result].column = std::move(col_res);
    }
};

}
