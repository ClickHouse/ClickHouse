#pragma once

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <yandex/consistent_hashing.h>
#include <iostream>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}


struct YandexConsistentHashImpl
{
    static constexpr auto name = "YandexConsistentHash";

    /// Actually it supports UInt64, but it is effective only if n < 65536
    using ResultType = UInt32;
    using BucketsCountType = ResultType;

    static inline ResultType apply(UInt64 hash, BucketsCountType n)
    {
        return ConsistentHashing(hash, n);
    }
};


/// Code from https://arxiv.org/pdf/1406.2294.pdf
static inline int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
    int64_t b = -1, j = 0;
    while (j < num_buckets) {
        b = j;
        key = key * 2862933555777941757ULL + 1;
        j = static_cast<int64_t>((b + 1) * (double(1LL << 31) / double((key >> 33) + 1)));
    }
    return static_cast<int32_t>(b);
}

struct JumpConsistentHashImpl
{
    static constexpr auto name = "JumpConsistentHash";

    using ResultType = Int32;
    using BucketsCountType = ResultType;

    static inline ResultType apply(UInt64 hash, BucketsCountType n)
    {
        return JumpConsistentHash(hash, n);
    }
};


template <typename Impl>
class FunctionConsistentHashImpl : public IFunction
{
public:

    static constexpr auto name = Impl::name;
    using ResultType = typename Impl::ResultType;
    using BucketsType = typename Impl::BucketsCountType;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConsistentHashImpl<Impl>>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isInteger())
            throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->isInteger())
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        auto buckets_col = block.getByPosition(arguments[1]).column.get();
        if (!buckets_col->isColumnConst())
            throw Exception("The second argument of function " + getName() + " (number of buckets) must be constant", ErrorCodes::BAD_ARGUMENTS);

        constexpr UInt64 max_buckets = static_cast<UInt64>(std::numeric_limits<BucketsType>::max());
        UInt64 num_buckets;

        auto check_range = [&] (auto buckets)
        {
            if (buckets <= 0)
                throw Exception("The second argument of function " + getName() + " (number of buckets) must be positive number",
                                ErrorCodes::BAD_ARGUMENTS);

            if (static_cast<UInt64>(buckets) > max_buckets)
                throw Exception("The value of the second argument of function " + getName() + " (number of buckets) is not fit to " +
                            DataTypeNumber<BucketsType>().getName(), ErrorCodes::BAD_ARGUMENTS);

            num_buckets = static_cast<UInt64>(buckets);
        };

        Field buckets_field = (*buckets_col)[0];
        if (buckets_field.getType() == Field::Types::Int64)
            check_range(buckets_field.safeGet<Int64>());
        else if (buckets_field.getType() == Field::Types::UInt64)
            check_range(buckets_field.safeGet<UInt64>());
        else
            throw Exception("Illegal type " + String(buckets_field.getTypeName()) + " of the second argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


        const auto & hash_col_source = block.getByPosition(arguments[0]).column;
        ColumnPtr hash_col = (hash_col_source->isColumnConst()) ? hash_col_source->convertToFullColumnIfConst() : hash_col_source;
        ColumnPtr & res_col = block.getByPosition(result).column;


        const IDataType * hash_type = block.getByPosition(arguments[0]).type.get();

        if      (checkDataType<DataTypeUInt8>(hash_type)) executeType<UInt8>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeUInt16>(hash_type)) executeType<UInt16>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeUInt32>(hash_type)) executeType<UInt32>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeUInt64>(hash_type)) executeType<UInt64>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeInt8>(hash_type)) executeType<Int8>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeInt16>(hash_type)) executeType<Int16>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeInt32>(hash_type)) executeType<Int32>(hash_col, res_col, num_buckets);
        else if (checkDataType<DataTypeInt64>(hash_type)) executeType<Int64>(hash_col, res_col, num_buckets);
        else
            throw Exception("Illegal type " + hash_type->getName() + " of the first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

private:

    template <typename HashType>
    void executeType(const ColumnPtr & col_hash_ptr, ColumnPtr & out_col_result, const UInt64 num_buckets)
    {
        auto col_hash = checkAndGetColumn<ColumnVector<HashType>>(col_hash_ptr.get());
        if (!col_hash)
            throw Exception("Illegal type of the first argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto col_result = ColumnVector<ResultType>::create();
        typename ColumnVector<ResultType>::Container & vec_result = col_result->getData();
        const auto & vec_hash = col_hash->getData();

        size_t size = vec_hash.size();
        vec_result.resize(size);
        for (size_t i = 0; i < size; ++i)
            vec_result[i] = Impl::apply(static_cast<UInt64>(vec_hash[i]), static_cast<BucketsType>(num_buckets));

        out_col_result = std::move(col_result);
    }
};


using FunctionYandexConsistentHash = FunctionConsistentHashImpl<YandexConsistentHashImpl>;
using FunctionJumpConsistentHas = FunctionConsistentHashImpl<JumpConsistentHashImpl>;


}
