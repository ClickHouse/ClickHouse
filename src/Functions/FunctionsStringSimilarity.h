#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

namespace DB
{
/** Calculate similarity metrics:
  *
  * ngramDistance(haystack, needle) - calculate n-gram distance between haystack and needle.
  * Returns float number from 0 to 1 - the closer to zero, the more strings are similar to each other.
  * Also support CaseInsensitive and UTF8 formats.
  * ngramDistanceCaseInsensitive(haystack, needle)
  * ngramDistanceUTF8(haystack, needle)
  * ngramDistanceCaseInsensitiveUTF8(haystack, needle)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Impl, typename Name>
class FunctionsStringSimilarity : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringSimilarity>(); }

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column_haystack = block.getByPosition(arguments[0]).column;
        const ColumnPtr & column_needle = block.getByPosition(arguments[1]).column;

        const ColumnConst * col_haystack_const = typeid_cast<const ColumnConst *>(&*column_haystack);
        const ColumnConst * col_needle_const = typeid_cast<const ColumnConst *>(&*column_needle);

        if (col_haystack_const && col_needle_const)
        {
            ResultType res{};
            const String & needle = col_needle_const->getValue<String>();
            if (needle.size() > Impl::max_string_size)
            {
                throw Exception(
                    "String size of needle is too big for function " + getName() + ". Should be at most "
                        + std::to_string(Impl::max_string_size),
                    ErrorCodes::TOO_LARGE_STRING_SIZE);
            }
            Impl::constantConstant(col_haystack_const->getValue<String>(), needle, res);
            block.getByPosition(result).column
                = block.getByPosition(result).type->createColumnConst(col_haystack_const->size(), toField(res));
            return;
        }

        auto col_res = ColumnVector<ResultType>::create();

        typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
        vec_res.resize(column_haystack->size());

        const ColumnString * col_haystack_vector = checkAndGetColumn<ColumnString>(&*column_haystack);
        const ColumnString * col_needle_vector = checkAndGetColumn<ColumnString>(&*column_needle);

        if (col_haystack_vector && col_needle_const)
        {
            const String & needle = col_needle_const->getValue<String>();
            if (needle.size() > Impl::max_string_size)
            {
                throw Exception(
                    "String size of needle is too big for function " + getName() + ". Should be at most "
                        + std::to_string(Impl::max_string_size),
                    ErrorCodes::TOO_LARGE_STRING_SIZE);
            }
            Impl::vectorConstant(col_haystack_vector->getChars(), col_haystack_vector->getOffsets(), needle, vec_res);
        }
        else if (col_haystack_vector && col_needle_vector)
        {
            Impl::vectorVector(
                col_haystack_vector->getChars(),
                col_haystack_vector->getOffsets(),
                col_needle_vector->getChars(),
                col_needle_vector->getOffsets(),
                vec_res);
        }
        else if (col_haystack_const && col_needle_vector)
        {
            const String & haystack = col_haystack_const->getValue<String>();
            if (haystack.size() > Impl::max_string_size)
            {
                throw Exception(
                    "String size of haystack is too big for function " + getName() + ". Should be at most "
                        + std::to_string(Impl::max_string_size),
                    ErrorCodes::TOO_LARGE_STRING_SIZE);
            }
            Impl::constantVector(haystack, col_needle_vector->getChars(), col_needle_vector->getOffsets(), vec_res);
        }
        else
        {
            throw Exception(
                "Illegal columns " + block.getByPosition(arguments[0]).column->getName() + " and "
                    + block.getByPosition(arguments[1]).column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }

        block.getByPosition(result).column = std::move(col_res);
    }
};

}
