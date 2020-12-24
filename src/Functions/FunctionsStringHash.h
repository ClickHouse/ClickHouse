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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Function {} expect single String argument, got {}", getName(), arguments[0]->getName());

        auto type = std::make_shared<DataTypeUInt64>();
        if constexpr (is_simhash)
            return type;

        return std::make_shared<DataTypeTuple>(DataTypes{type, type});
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnPtr & column = arguments[0].column;

        if constexpr (is_simhash)
        {
            // non const string, const case is handled by useDefaultImplementationForConstants.
            auto col_res = ColumnVector<UInt64>::create();
            auto & vec_res = col_res->getData();
            vec_res.resize(column->size());
            const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
            Impl::apply(col_str_vector->getChars(), col_str_vector->getOffsets(), vec_res);
            return col_res;
        }
        else // Min hash
        {
            // non const string
            auto col_h1 = ColumnVector<UInt64>::create();
            auto col_h2 = ColumnVector<UInt64>::create();
            auto & vec_h1 = col_h1->getData();
            auto & vec_h2 = col_h2->getData();
            vec_h1.resize(column->size());
            vec_h2.resize(column->size());
            const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
            Impl::apply(col_str_vector->getChars(), col_str_vector->getOffsets(), vec_h1, vec_h2);
            MutableColumns tuple_columns;
            tuple_columns.emplace_back(std::move(col_h1));
            tuple_columns.emplace_back(std::move(col_h2));
            return ColumnTuple::create(std::move(tuple_columns));
        }
    }
};
}

