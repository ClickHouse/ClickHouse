#pragma once

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <common/likely.h>

#include <yandex/consistent_hashing.h>
#include <mailru/sumbur.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}


/// An O(1) time and space consistent hash algorithm by Konstantin Oblakov
struct YandexConsistentHashImpl
{
    static constexpr auto name = "yandexConsistentHash";

    using HashType = UInt64;
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
    static constexpr auto name = "jumpConsistentHash";

    using HashType = UInt64;
    using ResultType = Int32;
    using BucketsCountType = ResultType;

    static inline ResultType apply(UInt64 hash, BucketsCountType n)
    {
        return JumpConsistentHash(hash, n);
    }
};


struct SumburConsistentHashImpl
{
    static constexpr auto name = "sumburConsistentHash";

    using HashType = UInt32;
    using ResultType = UInt16;
    using BucketsCountType = ResultType;

    static inline ResultType apply(HashType hash, BucketsCountType n)
    {
        return static_cast<ResultType>(sumburConsistentHash(hash, n));
    }
};


template <typename Impl>
class FunctionConsistentHashImpl : public IFunction
{
public:

    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionConsistentHashImpl<Impl>>(); };

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isInteger())
            throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments[0]->getSizeOfValueInMemory() > sizeof(HashType))
            throw Exception("Function " + getName() + " accepts " + std::to_string(sizeof(HashType) * 8) + "-bit integers at most"
                            + ", got " + arguments[0]->getName(), ErrorCodes::BAD_ARGUMENTS);

        if (!arguments[1]->isInteger())
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (block.getByPosition(arguments[1]).column->isColumnConst())
            executeConstBuckets(block, arguments, result);
        else
            throw Exception("The second argument of function " + getName() + " (number of buckets) must be constant", ErrorCodes::BAD_ARGUMENTS);
    }

private:

    using HashType = typename Impl::HashType;
    using ResultType = typename Impl::ResultType;
    using BucketsType = typename Impl::BucketsCountType;
    static constexpr auto max_buckets = static_cast<UInt64>(std::numeric_limits<BucketsType>::max());

    template <typename T>
    inline BucketsType checkBucketsRange(T buckets)
    {
        if (unlikely(buckets <= 0))
            throw Exception("The second argument of function " + getName() + " (number of buckets) must be positive number",
                            ErrorCodes::BAD_ARGUMENTS);

        if (unlikely(static_cast<UInt64>(buckets) > max_buckets))
            throw Exception("The value of the second argument of function " + getName() + " (number of buckets) is not fit to " +
                        DataTypeNumber<BucketsType>().getName(), ErrorCodes::BAD_ARGUMENTS);

        return static_cast<BucketsType>(buckets);
    }

    void executeConstBuckets(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        Field buckets_field = (*block.getByPosition(arguments[1]).column)[0];
        BucketsType num_buckets;

        if (buckets_field.getType() == Field::Types::Int64)
            num_buckets = checkBucketsRange(buckets_field.get<Int64>());
        else if (buckets_field.getType() == Field::Types::UInt64)
            num_buckets = checkBucketsRange(buckets_field.get<UInt64>());
        else
            throw Exception("Illegal type " + String(buckets_field.getTypeName()) + " of the second argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto & hash_col = block.getByPosition(arguments[0]).column;
        const IDataType * hash_type = block.getByPosition(arguments[0]).type.get();
        auto res_col = ColumnVector<ResultType>::create();

        if      (checkDataType<DataTypeUInt8>(hash_type)) executeType<UInt8>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeUInt16>(hash_type)) executeType<UInt16>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeUInt32>(hash_type)) executeType<UInt32>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeUInt64>(hash_type)) executeType<UInt64>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeInt8>(hash_type)) executeType<Int8>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeInt16>(hash_type)) executeType<Int16>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeInt32>(hash_type)) executeType<Int32>(hash_col, num_buckets, res_col.get());
        else if (checkDataType<DataTypeInt64>(hash_type)) executeType<Int64>(hash_col, num_buckets, res_col.get());
        else
            throw Exception("Illegal type " + hash_type->getName() + " of the first argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        block.getByPosition(result).column = std::move(res_col);
    }

    template <typename CurrentHashType>
    void executeType(const ColumnPtr & col_hash_ptr, BucketsType num_buckets, ColumnVector<ResultType> * col_result)
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


using FunctionYandexConsistentHash = FunctionConsistentHashImpl<YandexConsistentHashImpl>;
using FunctionJumpConsistentHash = FunctionConsistentHashImpl<JumpConsistentHashImpl>;
using FunctionSumburConsistentHash = FunctionConsistentHashImpl<SumburConsistentHashImpl>;


}
