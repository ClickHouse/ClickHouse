#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

// FunctionStringHash
// Simhash: String -> UInt64
// Minhash: String -> (UInt64, UInt64)
template <typename Impl, typename Name, bool is_simhash, bool is_arg = false>
class FunctionsStringHash : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr size_t default_shingle_size = 3;
    static constexpr size_t default_num_hashes = 6;
    static constexpr size_t max_shingle_size = 25;
    static constexpr size_t max_num_hashes = 25;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionsStringHash>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (is_simhash)
            return {1};
        else
            return {1, 2};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expect at least one argument", getName());

        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument of function {} must be String, got {}", getName(), arguments[0].type->getName());

        size_t shingle_size = default_shingle_size;

        if (arguments.size() > 1)
        {
            if (!isUnsignedInteger(arguments[1].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Second argument (shingle size) of function {} must be unsigned integer, got {}",
                                getName(), arguments[1].type->getName());

            if (!arguments[1].column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Second argument (shingle size) of function {} must be constant", getName());

            shingle_size = arguments[1].column->getUInt(0);
        }

        size_t num_hashes = default_num_hashes;

        if (arguments.size() > 2)
        {
            if constexpr (is_simhash)
                throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                                "Function {} expect no more than two arguments (text, shingle size), got {}",
                                getName(), arguments.size());

            if (!isUnsignedInteger(arguments[2].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Third argument (num hashes) of function {} must be unsigned integer, got {}",
                                getName(), arguments[2].type->getName());

            if (!arguments[2].column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Third argument (num hashes) of function {} must be constant", getName());

            num_hashes = arguments[2].column->getUInt(0);
        }

        if (arguments.size() > 3)
        {
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                            "Function {} expect no more than three arguments (text, shingle size, num hashes), got {}",
                            getName(), arguments.size());
        }

        if (shingle_size == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Second argument (shingle size) of function {} cannot be zero", getName());
        if (num_hashes == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Third argument (num hashes) of function {} cannot be zero", getName());

        if (shingle_size > max_shingle_size)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Second argument (shingle size) of function {} cannot be greater then {}", getName(), max_shingle_size);
        if (num_hashes > max_num_hashes)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Third argument (num hashes) of function {} cannot be greater then {}", getName(), max_num_hashes);

        auto type = std::make_shared<DataTypeUInt64>();
        if constexpr (is_simhash)
            return type;

        if constexpr (is_arg)
        {
            DataTypePtr string_type = std::make_shared<DataTypeString>();
            DataTypes types(num_hashes, string_type);
            auto tuple_type = std::make_shared<DataTypeTuple>(types);
            return std::make_shared<DataTypeTuple>(DataTypes{tuple_type, tuple_type});
        }

        return std::make_shared<DataTypeTuple>(DataTypes{type, type});
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnPtr & column = arguments[0].column;

        size_t shingle_size = default_shingle_size;
        size_t num_hashes = default_num_hashes;

        if (arguments.size() > 1)
            shingle_size = arguments[1].column->getUInt(0);

        if (arguments.size() > 2)
            num_hashes = arguments[2].column->getUInt(0);

        if constexpr (is_simhash)
        {
            auto col_res = ColumnVector<UInt64>::create();
            auto & vec_res = col_res->getData();
            vec_res.resize(column->size());
            const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
            Impl::apply(col_str_vector->getChars(), col_str_vector->getOffsets(), shingle_size, vec_res);
            return col_res;
        }
        else if constexpr (is_arg) // Min hash arg
        {
            MutableColumns min_columns(num_hashes);
            MutableColumns max_columns(num_hashes);
            for (size_t i = 0; i < num_hashes; ++i)
            {
                min_columns[i] = ColumnString::create();
                max_columns[i] = ColumnString::create();
            }

            auto min_tuple = ColumnTuple::create(std::move(min_columns));
            auto max_tuple = ColumnTuple::create(std::move(max_columns));

            const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
            Impl::apply(col_str_vector->getChars(), col_str_vector->getOffsets(), shingle_size, num_hashes, nullptr, nullptr, min_tuple.get(), max_tuple.get());

            MutableColumns tuple_columns;
            tuple_columns.emplace_back(std::move(min_tuple));
            tuple_columns.emplace_back(std::move(max_tuple));
            return ColumnTuple::create(std::move(tuple_columns));
        }
        else // Min hash
        {
            auto col_h1 = ColumnVector<UInt64>::create();
            auto col_h2 = ColumnVector<UInt64>::create();
            auto & vec_h1 = col_h1->getData();
            auto & vec_h2 = col_h2->getData();
            vec_h1.resize(column->size());
            vec_h2.resize(column->size());
            const ColumnString * col_str_vector = checkAndGetColumn<ColumnString>(&*column);
            Impl::apply(col_str_vector->getChars(), col_str_vector->getOffsets(), shingle_size, num_hashes, &vec_h1, &vec_h2, nullptr, nullptr);
            MutableColumns tuple_columns;
            tuple_columns.emplace_back(std::move(col_h1));
            tuple_columns.emplace_back(std::move(col_h2));
            return ColumnTuple::create(std::move(tuple_columns));
        }
    }
};
}

