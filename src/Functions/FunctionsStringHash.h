#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}

// FunctionStringHash
// Simhash: String -> UInt64
// Minhash: String -> (UInt64, UInt64)
template <typename Impl, typename Name, bool is_simhash>
class FunctionsStringHash : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsStringHash>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if constexpr (is_simhash)
            return std::make_shared<DataTypeNumber<typename Impl::ResultType>>();
        auto element = DataTypeFactory::instance().get("UInt64");
        return std::make_shared<DataTypeTuple>(DataTypes{element, element});
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t) override
    {
        const ColumnPtr & column = block.getByPosition(arguments[0]).column;
        const ColumnConst * col_const = typeid_cast<const ColumnConst *>(&*column);
        using ResultType = typename Impl::ResultType;
        if constexpr (is_simhash)
        {
            if (col_const)
            {
                ResultType res{};
                const String & str_data = col_const->getValue<String>();
                if (str_data.size() > Impl::max_string_size)
                {
                    throw Exception(
                        "String size is too big for function " + getName() + ". Should be at most " + std::to_string(Impl::max_string_size),
                        ErrorCodes::TOO_LARGE_STRING_SIZE);
                }
                Impl::constant(str_data, res);
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(1, toField(res));
            }
            else
            {
                // non const string
                auto col_res = ColumnVector<ResultType>::create();
                typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
                vec_res.resize(column->size());
                const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
                Impl::vector(col_str_vector->getChars(), col_str_vector->getOffsets(), vec_res);
                block.getByPosition(result).column = std::move(col_res);
            }
        }
        else // Min hash
        {
            if (col_const)
            {
                ResultType h1, h2;
                const String & str_data = col_const->getValue<String>();
                if (str_data.size() > Impl::max_string_size)
                {
                    throw Exception(
                        "String size is too big for function " + getName() + ". Should be at most " + std::to_string(Impl::max_string_size),
                        ErrorCodes::TOO_LARGE_STRING_SIZE);
                }
                Impl::constant(str_data, h1, h2);
                auto h1_col = ColumnVector<ResultType>::create(1);
                auto h2_col = ColumnVector<ResultType>::create(1);
                typename ColumnVector<ResultType>::Container & h1_data = h1_col->getData();
                typename ColumnVector<ResultType>::Container & h2_data = h2_col->getData();
                h1_data[0] = h1;
                h2_data[0] = h2;
                MutableColumns tuple_columns;
                tuple_columns.emplace_back(std::move(h1_col));
                tuple_columns.emplace_back(std::move(h2_col));
                block.getByPosition(result).column = ColumnTuple::create(std::move(tuple_columns));
            }
            else
            {
                // non const string
                auto col_h1 = ColumnVector<ResultType>::create();
                auto col_h2 = ColumnVector<ResultType>::create();
                typename ColumnVector<ResultType>::Container & vec_h1 = col_h1->getData();
                typename ColumnVector<ResultType>::Container & vec_h2 = col_h2->getData();
                vec_h1.resize(column->size());
                vec_h2.resize(column->size());
                const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
                Impl::vector(col_str_vector->getChars(), col_str_vector->getOffsets(), vec_h1, vec_h2);
                MutableColumns tuple_columns;
                tuple_columns.emplace_back(std::move(col_h1));
                tuple_columns.emplace_back(std::move(col_h2));
                block.getByPosition(result).column = ColumnTuple::create(std::move(tuple_columns));
            }
        }
    }
};
}

