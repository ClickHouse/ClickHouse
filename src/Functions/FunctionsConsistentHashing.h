#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


template <typename Impl>
class FunctionConsistentHashImpl : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionConsistentHashImpl<Impl>>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isInteger(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments[0]->getSizeOfValueInMemory() > sizeof(HashType))
            throw Exception("Function " + getName() + " accepts " + std::to_string(sizeof(HashType) * 8) + "-bit integers at most"
                    + ", got " + arguments[0]->getName(),
                ErrorCodes::BAD_ARGUMENTS);

        if (!isInteger(arguments[1]))
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1};
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (isColumnConst(*arguments[1].column))
            return executeConstBuckets(arguments);
        else
            throw Exception(
                "The second argument of function " + getName() + " (number of buckets) must be constant", ErrorCodes::BAD_ARGUMENTS);
    }

private:
    using HashType = typename Impl::HashType;
    using ResultType = typename Impl::ResultType;
    using BucketsType = typename Impl::BucketsType;

    template <typename T>
    inline BucketsType checkBucketsRange(T buckets) const
    {
        if (unlikely(buckets <= 0))
            throw Exception(
                "The second argument of function " + getName() + " (number of buckets) must be positive number", ErrorCodes::BAD_ARGUMENTS);

        if (unlikely(static_cast<UInt64>(buckets) > Impl::max_buckets))
            throw Exception("The value of the second argument of function " + getName() + " (number of buckets) must not be greater than "
                    + std::to_string(Impl::max_buckets), ErrorCodes::BAD_ARGUMENTS);

        return static_cast<BucketsType>(buckets);
    }

    ColumnPtr executeConstBuckets(const ColumnsWithTypeAndName & arguments) const
    {
        Field buckets_field = (*arguments[1].column)[0];
        BucketsType num_buckets;

        if (buckets_field.getType() == Field::Types::Int64)
            num_buckets = checkBucketsRange(buckets_field.get<Int64>());
        else if (buckets_field.getType() == Field::Types::UInt64)
            num_buckets = checkBucketsRange(buckets_field.get<UInt64>());
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of the second argument of function {}",
                buckets_field.getTypeName(), getName());

        const auto & hash_col = arguments[0].column;
        const IDataType * hash_type = arguments[0].type.get();
        auto res_col = ColumnVector<ResultType>::create();

        WhichDataType which(hash_type);

        if (which.isUInt8())
            executeType<UInt8>(hash_col, num_buckets, res_col.get());
        else if (which.isUInt16())
            executeType<UInt16>(hash_col, num_buckets, res_col.get());
        else if (which.isUInt32())
            executeType<UInt32>(hash_col, num_buckets, res_col.get());
        else if (which.isUInt64())
            executeType<UInt64>(hash_col, num_buckets, res_col.get());
        else if (which.isInt8())
            executeType<Int8>(hash_col, num_buckets, res_col.get());
        else if (which.isInt16())
            executeType<Int16>(hash_col, num_buckets, res_col.get());
        else if (which.isInt32())
            executeType<Int32>(hash_col, num_buckets, res_col.get());
        else if (which.isInt64())
            executeType<Int64>(hash_col, num_buckets, res_col.get());
        else
            throw Exception("Illegal type " + hash_type->getName() + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res_col;
    }

    template <typename CurrentHashType>
    void executeType(const ColumnPtr & col_hash_ptr, BucketsType num_buckets, ColumnVector<ResultType> * col_result) const
    {
        auto col_hash = checkAndGetColumn<ColumnVector<CurrentHashType>>(col_hash_ptr.get());
        if (!col_hash)
            throw Exception("Illegal type of the first argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto & vec_result = col_result->getData();
        const auto & vec_hash = col_hash->getData();

        size_t size = vec_hash.size();
        vec_result.resize(size);
        for (size_t i = 0; i < size; ++i)
            vec_result[i] = Impl::apply(static_cast<HashType>(vec_hash[i]), num_buckets);
    }
};

}
