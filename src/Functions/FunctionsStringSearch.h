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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearch>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return Impl::use_default_implementation_for_constants; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return Impl::use_default_implementation_for_constants
            ? ColumnNumbers{1, 2}
            : ColumnNumbers{};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if constexpr (!Impl::use_default_implementation_for_constants)
        {
            if (col_haystack_const && col_needle_const)
            {
                ResultType res{};
                Impl::constantConstant(col_haystack_const->getValue<String>(), col_needle_const->getValue<String>(), res);
                block.getByPosition(result).column
                    = block.getByPosition(result).type->createColumnConst(col_haystack_const->size(), toField(res));
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
                vec_res);
        else if (col_haystack_vector && col_needle_const)
            Impl::vectorConstant(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), col_needle_const->getValue<String>(), vec_res);
        else if (col_haystack_vector_fixed && col_needle_const)
            Impl::vectorFixedConstant(
                col_haystack_vector_fixed->getChars(), col_haystack_vector_fixed->getN(), col_needle_const->getValue<String>(), vec_res);
        else if (col_haystack_const && col_needle_vector)
            Impl::constantVector(
                col_haystack_const->getValue<String>(), col_needle_vector->getChars(), col_needle_vector->getOffsets(), vec_res);
        else
            throw Exception(
                "Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " and "
                    + block.getByPosition(arguments[1]).column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }
};

}
