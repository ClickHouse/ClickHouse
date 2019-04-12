#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <common/StringRef.h>

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
  *
  * Applies regexp re2 and pulls:
  * - the first subpattern, if the regexp has a subpattern;
  * - the zero subpattern (the match part, otherwise);
  * - if not match - an empty string.
  * extract(haystack, pattern)
  *
  * replaceOne(haystack, pattern, replacement) - replacing the pattern with the specified rules, only the first occurrence.
  * replaceAll(haystack, pattern, replacement) - replacing the pattern with the specified rules, all occurrences.
  *
  * replaceRegexpOne(haystack, pattern, replacement) - replaces the pattern with the specified regexp, only the first occurrence.
  * replaceRegexpAll(haystack, pattern, replacement) - replaces the pattern with the specified type, all occurrences.
  *
  * multiSearchAllPositions(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- find first occurrences (positions) of all the const patterns inside haystack
  * multiSearchAllPositionsUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAllPositionsCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAllPositionsCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  *
  * multiSearchFirstPosition(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- returns the first position of the haystack matched by strings or zero if nothing was found
  * multiSearchFirstPositionUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  *
  * multiSearchAny(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- find any of the const patterns inside haystack and return 0 or 1
  * multiSearchAnyUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAnyCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchAnyCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])

  * multiSearchFirstIndex(haystack, [pattern_1, pattern_2, ..., pattern_n]) -- returns the first index of the matched string or zero if nothing was found
  * multiSearchFirstIndexUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstIndexCaseInsensitive(haystack, [pattern_1, pattern_2, ..., pattern_n])
  * multiSearchFirstIndexCaseInsensitiveUTF8(haystack, [pattern_1, pattern_2, ..., pattern_n])
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_NOT_ALLOWED;
}

template <typename Impl, typename Name>
class FunctionsStringSearch : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearch>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            Impl::constant_constant(col_haystack_const->getValue<String>(), col_needle_const->getValue<String>(), res);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(col_haystack_const->size(), toField(res));
            return;
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_vector)
            Impl::vector_vector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                vec_res);
        else if (col_haystack_vector && col_needle_const)
            Impl::vector_constant(
                col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), col_needle_const->getValue<String>(), vec_res);
        else if (col_haystack_const && col_needle_vector)
            Impl::constant_vector(
                col_haystack_const->getValue<String>(), col_needle_vector->getChars(), col_needle_vector->getOffsets(), vec_res);
        else
            throw Exception(
                "Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " and "
                    + block.getByPosition(arguments[1]).column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }
};


template <typename Impl, typename Name>
class FunctionsStringSearchToString : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSearchToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_needle = typeid_cast<const ColumnConst *>(&*column_needle);
        if (!col_needle)
            throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), col_needle->getValue<String>(), vec_res, offsets_res);

            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <typename Impl, typename Name>
class FunctionsMultiStringPosition : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsMultiStringPosition>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const ColumnPtr & arr_ptr = block.getByPosition(arguments[1]).column;
        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());

        if (!col_const_arr)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + ". The array is not const",
                ErrorCodes::ILLEGAL_COLUMN);

        Array src_arr = col_const_arr->getValue<Array>();

        if (src_arr.size() > std::numeric_limits<UInt8>::max())
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(src_arr.size())
                    + ", should be at most 255",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::vector<StringRef> refs;
        for (const auto & el : src_arr)
            refs.emplace_back(el.get<String>());

        const size_t column_haystack_size = column_haystack->size();

        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create(column_haystack_size);

        auto & vec_res = col_res->getData();
        auto & offsets_res = col_offsets->getData();

        vec_res.resize(column_haystack_size * refs.size());

        if (col_haystack_vector)
            Impl::vector_constant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), refs, vec_res);
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName(), ErrorCodes::ILLEGAL_COLUMN);

        size_t refs_size = refs.size();
        size_t accum = refs_size;

        for (size_t i = 0; i < column_haystack_size; ++i, accum += refs_size)
            offsets_res[i] = accum;

        block.getByPosition(result).column = ColumnArray::create(std::move(col_res), std::move(col_offsets));
    }
};

/// The argument limiting raises from Volnitsky searcher -- it is performance crucial to save only one byte for pattern number.
/// But some other searchers use this function, for example, multiMatchAny -- hyperscan does not have such restrictions
template <typename Impl, typename Name, size_t LimitArgs = std::numeric_limits<UInt8>::max()>
class FunctionsMultiStringSearch : public IFunction
{
    static_assert(LimitArgs > 0);

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context)
    {
        if (Impl::is_using_hyperscan && !context.getSettingsRef().allow_hyperscan)
            throw Exception(
                "Hyperscan functions are disabled, because setting 'allow_hyperscan' is set to 0", ErrorCodes::FUNCTION_NOT_ALLOWED);

        return std::make_shared<FunctionsMultiStringSearch>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type || !checkAndGetDataType<DataTypeString>(array_type->getNestedType().get()))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


        return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);

        const ColumnPtr & arr_ptr = block.getByPosition(arguments[1]).column;
        const ColumnConst * col_const_arr = checkAndGetColumnConst<ColumnArray>(arr_ptr.get());

        if (!col_const_arr)
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + ". The array is not const",
                ErrorCodes::ILLEGAL_COLUMN);

        Array src_arr = col_const_arr->getValue<Array>();

        if (src_arr.size() > LimitArgs)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + std::to_string(src_arr.size())
                    + ", should be at most " + std::to_string(LimitArgs),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        std::vector<StringRef> refs;
        refs.reserve(src_arr.size());

        for (const auto & el : src_arr)
            refs.emplace_back(el.get<String>());

        const size_t column_haystack_size = column_haystack->size();

        auto col_res = ColumnVector<ResultType>::create();

        auto & vec_res = col_res->getData();

        vec_res.resize(column_haystack_size);

        if (col_haystack_vector)
            Impl::vector_constant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), refs, vec_res);
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = std::move(col_res);
    }
};

}
