#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>

namespace DB
{
/** Search and replace functions in strings:
  * position(haystack, needle)     - the normal search for a substring in a string, returns the position (in bytes) of the found substring starting with 1, or 0 if no substring is found.
  * positionUTF8(haystack, needle) - the same, but the position is calculated at code points, provided that the string is encoded in UTF-8.
  * positionCaseInsensitive(haystack, needle)
  * positionCaseInsensitiveUTF8(haystack, needle)
  *
  * like(haystack, pattern)        - search by the regular expression LIKE; Returns 0 or 1. Case-insensitive, but only for Latin.
  * notLike(haystack, pattern)
  *
  * ilike(haystack, pattern) - like 'like' but case-insensitive
  * notIlike(haystack, pattern)
  *
  * match(haystack, pattern)       - search by regular expression re2; Returns 0 or 1.
  *
  * countSubstrings(haystack, needle) -- count number of occurrences of needle in haystack.
  * countSubstringsCaseInsensitive(haystack, needle)
  * countSubstringsCaseInsensitiveUTF8(haystack, needle)
  *
  * hasToken()
  * hasTokenCaseInsensitive()
  *
  * JSON stuff:
  * visitParamExtractBool()
  * simpleJSONExtractBool()
  * visitParamExtractFloat()
  * simpleJSONExtractFloat()
  * visitParamExtractInt()
  * simpleJSONExtractInt()
  * visitParamExtractUInt()
  * simpleJSONExtractUInt()
  * visitParamHas()
  * simpleJSONHas()
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

template <typename Impl>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsStringSearch>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return Impl::supports_start_pos; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override
    {
        if (Impl::supports_start_pos)
            return 0;
        return 2;
    }

    bool useDefaultImplementationForConstants() const override { return Impl::use_default_implementation_for_constants; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return Impl::getArgumentsThatAreAlwaysConstant();
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2 || 3 < arguments.size())
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & column_haystack = arguments[0].column;
        const ColumnPtr & column_needle = arguments[1].column;

        ColumnPtr column_start_pos = nullptr;
        if (arguments.size() >= 3)
            column_start_pos = arguments[2].column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        using ResultType = typename Impl::ResultType;

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
                    return result_type->createColumnConst(col_haystack_const->size(), toField(vec_res[0]));
                else
                    return col_res;
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
        else if (col_haystack_vector_fixed && col_needle_vector)
            Impl::vectorFixedVector(
                col_haystack_vector_fixed->getChars(),
                col_haystack_vector_fixed->getN(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
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
                "Illegal columns " + arguments[0].column->getName() + " and "
                    + arguments[1].column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        return col_res;
    }
};

}
