#pragma once

#include <memory>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/PODArray_fwd.h>
#include <Common/logger_useful.h>

// Include this header last, because it is an auto-generated dump of questionable
// garbage that breaks the build (e.g. it changes _POSIX_C_SOURCE).
// TODO: find out what it is. On github, they have proper interface headers like
// this one: https://github.com/RoaringBitmap/CRoaring/blob/master/include/roaring/roaring.h
#include "AggregateFunctions/AggregateFunctionGroupBitmapData.h"

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}


#define FOR_NUMERIC_INDEXED_VECTOR_INDEX_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64)

#define FOR_NUMERIC_INDEXED_VECTOR_VALUE_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)


template <typename IT, typename VT>
class BSINumericIndexedVector
{
public:
    using IndexType = IT;
    using ValueType = VT;

    static constexpr auto type = "BSI";
    static constexpr UInt32 DEFAULT_INTEGER_BIT_NUM = 40;
    static constexpr UInt32 DEFAULT_FRACTION_BIT_NUM = 24;
    static constexpr UInt32 MAX_INTEGER_BIT_NUM = 64;
    static constexpr UInt32 MAX_FRACTION_BIT_NUM = 24;
    static constexpr UInt32 MAX_TOTAL_BIT_NUM = 64;
    static constexpr size_t max_size = 10_GiB;

    static constexpr UInt32 multiply_op_code = 2;
    static constexpr UInt32 divide_op_code = 3;

private:
    UInt32 integer_bit_num;
    UInt32 fraction_bit_num;

    // using RoaringBitmap = std::conditional_t<sizeof(IndexType) >= 8, roaring::Roaring64Map, roaring::Roaring>;
    using Roaring = RoaringBitmapWithSmallSet<IT, 32>;
    std::vector<std::shared_ptr<Roaring>> data_array;

public:
    BSINumericIndexedVector()
    {
        WhichDataType first_which(DataTypeNumber<IndexType>().getTypeId());
        WhichDataType second_which(DataTypeNumber<ValueType>().getTypeId());
        if (second_which.isUInt() or second_which.isInt())
        {
            switch (second_which.idx)
            {
                case TypeIndex::UInt8:
                    integer_bit_num = 8;
                    break;
                case TypeIndex::UInt16:
                    integer_bit_num = 16;
                    break;
                case TypeIndex::UInt32:
                    integer_bit_num = 32;
                    break;
                case TypeIndex::UInt64:
                    integer_bit_num = 64;
                    break;
                case TypeIndex::Int8:
                    integer_bit_num = 8;
                    break;
                case TypeIndex::Int16:
                    integer_bit_num = 16;
                    break;
                case TypeIndex::Int32:
                    integer_bit_num = 32;
                    break;
                case TypeIndex::Int64:
                    integer_bit_num = 64;
                    break;
                default:
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported ValueType: {}", DataTypeNumber<ValueType>().getName());
            }
            fraction_bit_num = 0;
        }
        else
        {
            integer_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_INTEGER_BIT_NUM;
            fraction_bit_num = BSINumericIndexedVector<UInt32, Float64>::DEFAULT_FRACTION_BIT_NUM;
        }
        initialize(integer_bit_num, fraction_bit_num);
    }

    std::string shortDebugString() const
    {
        std::string data_array_details;
        for (size_t i = 0; i < data_array.size(); ++i)
        {
            data_array_details += fmt::format("{}: {}", i, getDataArrayAt(i)->size());
            if (i + 1 < data_array.size())
                data_array_details += ", ";
        }

        std::string res = fmt::format(
            "vector_type: {}; integer_bit_num: {}; fraction_bit_num: {}; "
            "data_array size: {}; cardinality: {}.",
            type,
            integer_bit_num,
            fraction_bit_num,
            data_array.size(),
            data_array_details);
        return res;
    }

    static void checkIntergerFractionBitNum(UInt32 integer_bit_num, UInt32 fraction_bit_num)
    {
        if (integer_bit_num > MAX_INTEGER_BIT_NUM || fraction_bit_num > MAX_FRACTION_BIT_NUM)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "integer_bit_num({}) and fraction_bit_num({}) must <= {} and {} respectly",
                integer_bit_num,
                fraction_bit_num,
                MAX_INTEGER_BIT_NUM,
                MAX_FRACTION_BIT_NUM);
        if (integer_bit_num + fraction_bit_num > MAX_TOTAL_BIT_NUM)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "integer_bit_num({}) + fraction_bit_num({}) must <= {}",
                integer_bit_num,
                fraction_bit_num,
                MAX_TOTAL_BIT_NUM);
    }

    void initialize(UInt32 new_integer_bit_num, UInt32 new_fraction_bit_num)
    {
        checkIntergerFractionBitNum(new_integer_bit_num, new_fraction_bit_num);
        integer_bit_num = new_integer_bit_num;
        fraction_bit_num = new_fraction_bit_num;

        data_array.clear();
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<Roaring>());
        }
    }

    void initializeFromVectorAndValue(const BSINumericIndexedVector & rhs, ValueType value)
    {
        initialize(rhs.integer_bit_num, rhs.fraction_bit_num);

        auto all_index = rhs.getAllIndex();

        const UInt32 total_bit_num = getTotalBitNum();
        Int64 scaled_value = Int64(value * (1LL << fraction_bit_num));
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (scaled_value & (1L << i))
            {
                getDataArrayAt(i)->rb_or(*all_index);
            }
        }
    }

    std::shared_ptr<Roaring> getAllIndex() const
    {
        auto bm = std::make_shared<Roaring>();
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            bm->rb_or(*getDataArrayAt(i));
        }
        return bm;
    }

    void deepCopyFrom(const BSINumericIndexedVector & rhs)
    {
        integer_bit_num = rhs.integer_bit_num;
        fraction_bit_num = rhs.fraction_bit_num;
        const UInt32 total_bit_num = rhs.getTotalBitNum();
        data_array.clear();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<Roaring>());
            getDataArrayAt(i)->rb_or(*rhs.getDataArrayAt(i));
        }
    }

    void shallowCopyFrom(const BSINumericIndexedVector & rhs)
    {
        integer_bit_num = rhs.integer_bit_num;
        fraction_bit_num = rhs.fraction_bit_num;
        data_array.clear();
        data_array = rhs.data_array;
    }

    inline UInt32 getTotalBitNum() const
    {
        checkIntergerFractionBitNum(integer_bit_num, fraction_bit_num);
        return integer_bit_num + fraction_bit_num;
    }

    void changeSchema(UInt32 new_integer_bit_num, UInt32 new_fraction_bit_num)
    {
        if (new_integer_bit_num == integer_bit_num && new_fraction_bit_num == fraction_bit_num)
            return;
        const UInt32 total_bit_num = getTotalBitNum();
        const UInt32 new_total_bit_num = new_integer_bit_num + new_fraction_bit_num;

        auto tmp_data_array = data_array;

        data_array.clear();
        for (size_t i = 0; i < new_total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<Roaring>());
        }

        Int32 new_fraction_idx = static_cast<Int32>(new_fraction_bit_num) - 1;
        Int32 old_fraction_idx = static_cast<Int32>(fraction_bit_num) - 1;
        while (new_fraction_idx >= 0)
        {
            if (old_fraction_idx >= 0)
            {
                data_array[new_fraction_idx] = tmp_data_array[old_fraction_idx];
                --old_fraction_idx;
            }
            --new_fraction_idx;
        }

        UInt32 new_integer_idx = new_fraction_bit_num;
        UInt32 old_integer_idx = fraction_bit_num;
        while (new_integer_idx < new_total_bit_num)
        {
            if (old_integer_idx < total_bit_num)
            {
                data_array[new_integer_idx] = tmp_data_array[old_integer_idx];
                ++old_integer_idx;
            }
            ++new_integer_idx;
        }
        integer_bit_num = new_integer_bit_num;
        fraction_bit_num = new_fraction_bit_num;
    }

    static UInt32 promoteBitPrecisionInplace(BSINumericIndexedVector & lhs, BSINumericIndexedVector & rhs)
    {
        if (lhs.integer_bit_num == rhs.integer_bit_num && lhs.fraction_bit_num == rhs.fraction_bit_num)
        {
            return lhs.integer_bit_num + lhs.fraction_bit_num;
        }
        if (lhs.integer_bit_num <= rhs.integer_bit_num && lhs.fraction_bit_num <= rhs.fraction_bit_num)
        {
            lhs.changeSchema(rhs.integer_bit_num, rhs.fraction_bit_num);
            return rhs.integer_bit_num + rhs.fraction_bit_num;
        }
        if (lhs.integer_bit_num >= rhs.integer_bit_num && lhs.fraction_bit_num >= rhs.fraction_bit_num)
        {
            rhs.changeSchema(lhs.integer_bit_num, lhs.fraction_bit_num);
            return lhs.integer_bit_num + lhs.fraction_bit_num;
        }

        UInt32 max_integer_bit_num = std::max(lhs.integer_bit_num, rhs.integer_bit_num);
        UInt32 max_fraction_bit_num = std::max(lhs.fraction_bit_num, rhs.fraction_bit_num);

        checkIntergerFractionBitNum(max_integer_bit_num, max_fraction_bit_num);

        lhs.changeSchema(max_integer_bit_num, max_fraction_bit_num);
        rhs.changeSchema(max_integer_bit_num, max_fraction_bit_num);
        return max_integer_bit_num + max_fraction_bit_num;
    }

    std::shared_ptr<Roaring> & getDataArrayAt(size_t index)
    {
        if (index >= data_array.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array index out of bounds.");
        }
        return data_array[index];
    }
    const std::shared_ptr<Roaring> & getDataArrayAt(size_t index) const
    {
        if (index >= data_array.size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Array index out of bounds in const. index: {}; integer_bit_num: {}; fraction_bit_num: {}; data_array_size: {}",
                index,
                integer_bit_num,
                fraction_bit_num,
                data_array.size());
        }
        return data_array[index];
    }

    void pointwiseAddInplace(const BSINumericIndexedVector & rhs)
    {
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        const UInt32 total_bit_num = promoteBitPrecisionInplace(*this, rhs_ref);

        Roaring cin;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & augend = getDataArrayAt(i);
            const auto & addend = rhs_ref.getDataArrayAt(i);

            Roaring x_xor_y;
            x_xor_y.rb_or(*augend);
            x_xor_y.rb_xor(*addend);

            Roaring x_and_y;
            x_and_y.rb_or(*augend);
            x_and_y.rb_and(*addend);

            auto & sum = augend;

            sum->rb_xor(*addend);
            sum->rb_xor(cin);

            cin.rb_and(x_xor_y);
            cin.rb_or(x_and_y);
        }
    }

    // void pointwiseAdd(const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res) const
    // {
    //     res.deepCopyFrom(*this);
    //     res.pointwiseAddInplace(rhs);
    // }

    // void pointwiseAdd(const ValueType & rhs, BSINumericIndexedVector & res) const
    // {
    //     BSINumericIndexedVector rhs_vec;
    //     rhs_vec.initializeFromVectorAndValue(*this, rhs);
    //     pointwiseAdd(rhs_vec, res);
    // }

    static void pointwiseAdd(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseAddInplace(rhs);
    }

    static void pointwiseAdd(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res.deepCopyFrom(lhs);
        res.pointwiseAddInplace(rhs_vec);
    }

    void merge(const BSINumericIndexedVector & rhs) { pointwiseAddInplace(rhs); }

    void pointwiseSubtractInplace(const BSINumericIndexedVector & rhs)
    {
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        promoteBitPrecisionInplace(*this, rhs_ref);

        const UInt32 total_bit_num = getTotalBitNum();

        Roaring bin;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & minuend = getDataArrayAt(i);
            auto & subtrahend = rhs_ref.getDataArrayAt(i);

            Roaring subtrahend_or_bin;
            subtrahend_or_bin.rb_or(*subtrahend);
            subtrahend_or_bin.rb_or(bin);

            subtrahend_or_bin.rb_andnot(*minuend);

            minuend->rb_xor(*subtrahend);
            minuend->rb_xor(bin);

            bin.rb_and(*subtrahend);
            bin.rb_or(subtrahend_or_bin);
        }
    }

    static void pointwiseSubtract(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseSubtractInplace(rhs);
    }

    static void pointwiseSubtract(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res.deepCopyFrom(lhs);
        res.pointwiseSubtractInplace(rhs_vec);
    }

    bool allValuesEqualOne() const
    {
        const UInt32 total_bit_num = getTotalBitNum();
        if (total_bit_num == 0)
            return false;

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (i == fraction_bit_num)
                continue;
            if (getDataArrayAt(i)->size() > 0)
            {
                return false;
            }
        }
        return true;
    }

    void andBitmap(const Roaring & bm, BSINumericIndexedVector & res) const
    {
        res.initialize(integer_bit_num, fraction_bit_num);
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            res.getDataArrayAt(i)->merge(bm);
            res.getDataArrayAt(i)->rb_and(bm);
        }
    }

    std::shared_ptr<Roaring> allIndexes() const
    {
        auto res = std::make_shared<Roaring>();

        const UInt32 total_bit_num = getTotalBitNum();

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            res->rb_or(*getDataArrayAt(i));
        }
        return res;
    }

    static inline void setContainers(
        std::vector<roaring::internal::container_t *> & ctns,
        std::vector<UInt8> & types,
        UInt32 container_id,
        BSINumericIndexedVector & vector)
    {
        const UInt32 total_bit_num = vector.getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            vector.getDataArrayAt(i)->raSetContainer(ctns[i], container_id, types[i]);
        }
    }

    static void toVectorCompactArray(
        const PaddedPODArray<UInt32> & indexes,
        const PaddedPODArray<Float64> & values,
        const UInt32 & length,
        const UInt32 & container_id,
        PaddedPODArray<UInt64> & buffer,
        BSINumericIndexedVector & vector)
    {
        const UInt32 total_bit_num = vector.getTotalBitNum();

        if (total_bit_num > 64)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "total_bit_num must less than or equal 64");
        if (length > roaring::internal::DEFAULT_MAX_SIZE)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "keys size ({}) must be less than or equal roaring::internal::DEFAULT_MAX_SIZE({})",
                length,
                static_cast<UInt32>(roaring::internal::DEFAULT_MAX_SIZE));

        std::vector<roaring::internal::container_t *> ctns(total_bit_num);
        std::vector<UInt8> types(total_bit_num);
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            ctns[i] = roaring::internal::array_container_create_given_capacity(length);
            types[i] = ARRAY_CONTAINER_TYPE;
        }

        UInt64 mask = 0xFFFFFFFFFFFFFFFFULL;
        if (total_bit_num < 64)
        {
            mask = (1ULL << (total_bit_num)) - 1;
        }
        Float64 ratio = static_cast<Float64>(1ULL << vector.fraction_bit_num);
        for (size_t i = 0; i < length; ++i)
        {
            buffer[i] = static_cast<UInt64>(values[i] * ratio);
            buffer[i] &= mask;
        }

        constexpr UInt32 k_batch_size = 256;
        std::vector<std::vector<UInt16>> bit_buffer(64, std::vector<UInt16>(k_batch_size, 0));
        // number of keys in each bitmap of vector.
        std::vector<UInt16> cnt(64);

        for (UInt32 offset = 0; offset < length; offset += k_batch_size)
        {
            memset(cnt.data(), 0, sizeof(UInt16) * total_bit_num);
            UInt32 len = std::min(k_batch_size, length - offset);
            for (size_t j = 0; j < len; ++j)
            {
                UInt64 w = buffer[offset + j];
                while (w)
                {
                    UInt64 t = w & (~w + 1); // on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
                    int i = __builtin_ctzll(w); // on x64, should compile to TZCNT
                    bit_buffer[i][cnt[i]++] = indexes[offset + j];
                    w ^= t;
                }
            }
            for (size_t i = 0; i < total_bit_num; ++i)
            {
                if (cnt[i] == 0)
                {
                    continue;
                }
                auto * ctn = static_cast<roaring::internal::array_container_t *>(ctns[i]);
                memcpy(ctn->array + ctn->cardinality, bit_buffer[i].data(), cnt[i] * sizeof(UInt16));
                ctn->cardinality += cnt[i];
            }
        }
        setContainers(ctns, types, container_id, vector);
    }

    static UInt32 prepareBuffer(
        const PaddedPODArray<Float64> & values,
        const UInt32 length,
        const UInt32 fraction_bit_num,
        const UInt32 total_bit_num,
        PaddedPODArray<UInt64> & buffer,
        UInt32 & nonzero_cnt)
    {
        UInt64 mask = 0xFFFFFFFFFFFFFFFFULL;
        if (total_bit_num < 64)
        {
            mask = (1ULL << (total_bit_num)) - 1;
        }
        Float64 ratio = static_cast<Float64>(1ULL << fraction_bit_num);
        UInt32 number_of_1s = 0;
        nonzero_cnt = 0;
        for (size_t i = 0; i < length; ++i)
        {
            UInt64 tmp = static_cast<UInt64>(values[i] * ratio);
            tmp &= mask;
            buffer[i] = tmp;
            number_of_1s += __builtin_popcountll(tmp);
            if (tmp != 0)
            {
                nonzero_cnt++;
            }
        }
        return number_of_1s;
    }

    static void toVectorCompactBitsetDense(
        const PaddedPODArray<UInt32> & indexes,
        const PaddedPODArray<Float64> & values,
        const UInt32 & length,
        const UInt32 container_id,
        PaddedPODArray<UInt64> & buffer,
        BSINumericIndexedVector & vector)
    {
        if (vector.getTotalBitNum() > 64)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector.total_bit_num must less than or equal 64");
        if (indexes.size() != values.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "keys and values size must be equal. ");
        if (length <= roaring::internal::DEFAULT_MAX_SIZE)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "keys size ({}) must be large than roaring::internal::DEFAULT_MAX_SIZE", length);

        const UInt32 total_bit_num = vector.getTotalBitNum();

        std::vector<roaring::internal::container_t *> ctns(total_bit_num);
        std::vector<UInt8> types(total_bit_num);

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            ctns[i] = array_container_create_given_capacity(roaring::internal::DEFAULT_MAX_SIZE);
            types[i] = ARRAY_CONTAINER_TYPE;
        }

        constexpr UInt32 k_batch_size = 256;
        std::vector<UInt32> bit_buffer(64 * k_batch_size, 0);
        size_t cnt = 0;
        constexpr UInt64 shift = 6;
        for (UInt32 offset = 0; offset < length; offset += k_batch_size)
        {
            UInt32 len = std::min(k_batch_size, length - offset);
            cnt = roaring::internal::bitset_extract_setbits_avx2(buffer.data() + offset, len, bit_buffer.data(), k_batch_size * 64, 0);
            for (size_t i = 0; i < cnt; ++i)
            {
                UInt64 val = bit_buffer[i];
                UInt64 row;
                UInt64 col = val & 0x3f;
                ASM_SHIFT_RIGHT(val, shift, row);
                UInt64 index = indexes[offset + row];

                if (types[col] == ARRAY_CONTAINER_TYPE
                    && static_cast<roaring::internal::array_container_t *>(ctns[col])->cardinality + 1
                        > roaring::internal::DEFAULT_MAX_SIZE)
                {
                    auto * c = roaring::internal::bitset_container_from_array(
                        reinterpret_cast<roaring::internal::array_container_t *>(ctns[col]));
                    roaring::internal::container_free(ctns[col], types[col]);
                    ctns[col] = c;
                    types[col] = BITSET_CONTAINER_TYPE;
                }

                if (types[col] == ARRAY_CONTAINER_TYPE)
                {
                    auto * ctn = reinterpret_cast<roaring::internal::array_container_t *>(ctns[col]);
                    memcpy(ctn->array + ctn->cardinality, &index, sizeof(UInt16));
                    ctn->cardinality += 1;
                    continue;
                }
                auto * ctn = reinterpret_cast<roaring::internal::bitset_container_t *>(ctns[col]);
                roaring::internal::bitset_container_set(ctn, index);
            }
        }
        setContainers(ctns, types, container_id, vector);
    }

    static void toVectorCompactBitset(
        const PaddedPODArray<UInt32> & indexes,
        const PaddedPODArray<Float64> & values,
        const UInt32 & length,
        const UInt32 container_id,
        PaddedPODArray<UInt64> & buffer,
        BSINumericIndexedVector & vector)
    {
        if (vector.getTotalBitNum() > 64)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "vector.total_bit_num must less than or equal 64");
        if (indexes.size() != values.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "keys and values size must be equal. ");
        if (length <= roaring::internal::DEFAULT_MAX_SIZE)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "keys size ({}) must be large than roaring::internal::DEFAULT_MAX_SIZE", length);

        const UInt32 total_bit_num = vector.getTotalBitNum();

        std::vector<roaring::internal::container_t *> ctns(total_bit_num);
        std::vector<UInt8> types(total_bit_num);
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            ctns[i] = array_container_create_given_capacity(roaring::internal::DEFAULT_MAX_SIZE);
            types[i] = ARRAY_CONTAINER_TYPE;
        }
        constexpr UInt32 k_batch_size = 256;
        std::vector<std::vector<UInt16>> bit_buffer(total_bit_num, std::vector<UInt16>(k_batch_size, 0));
        std::vector<UInt16> cnt(total_bit_num);
        for (UInt32 offset = 0; offset < length; offset += k_batch_size)
        {
            memset(cnt.data(), 0, sizeof(UInt16) * total_bit_num);
            UInt32 len = std::min(k_batch_size, length - offset);
            for (UInt32 j = 0; j < len; ++j)
            {
                UInt64 w = buffer[offset + j];
                UInt16 key = indexes[offset + j];
                while (w)
                {
                    UInt64 t = w & (~w + 1); // on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
                    int i = __builtin_ctzll(w); // on x64, should compile to TZCNT
                    bit_buffer[i][cnt[i]++] = key;
                    w ^= t;
                }
            }
            for (size_t i = 0; i < total_bit_num; ++i)
            {
                if (cnt[i] == 0)
                {
                    continue;
                }
                if (types[i] == ARRAY_CONTAINER_TYPE
                    && static_cast<roaring::internal::array_container_t *>(ctns[i])->cardinality + cnt[i]
                        > roaring::internal::DEFAULT_MAX_SIZE)
                {
                    auto * c
                        = roaring::internal::bitset_container_from_array(reinterpret_cast<roaring::internal::array_container_t *>(ctns[i]));
                    roaring::internal::container_free(ctns[i], types[i]);
                    ctns[i] = c;
                    types[i] = BITSET_CONTAINER_TYPE;
                }

                if (types[i] == ARRAY_CONTAINER_TYPE)
                {
                    auto * ctn = reinterpret_cast<roaring::internal::array_container_t *>(ctns[i]);
                    memcpy(ctn->array + ctn->cardinality, bit_buffer[i].data(), cnt[i] * sizeof(UInt16));
                    ctn->cardinality += cnt[i];
                    continue;
                }

                auto * ctn = reinterpret_cast<roaring::internal::bitset_container_t *>(ctns[i]);
                constexpr UInt64 shift = 6;
                for (int j = 0; j < cnt[i]; ++j)
                {
                    UInt64 tmp_offset;
                    UInt64 p = bit_buffer[i][j];
                    // ASM optimized
                    ASM_SHIFT_RIGHT(p, shift, tmp_offset);
                    // ctn->words[tmp_offset] |= 1ULL << (p & 0x3f);
                    UInt64 load = ctn->words[tmp_offset];
                    load |= (1ULL << p);
                    ctn->words[tmp_offset] = load;
                }
                ctn->cardinality += cnt[i];
            }
        }
        setContainers(ctns, types, container_id, vector);
    }

    static void toVectorCompact(
        const PaddedPODArray<UInt32> & indexes,
        const PaddedPODArray<Float64> & values,
        const UInt32 & length,
        const UInt32 & container_id,
        PaddedPODArray<UInt64> & buffer,
        BSINumericIndexedVector & vector)
    {
        if (length <= roaring::internal::DEFAULT_MAX_SIZE)
        {
            return toVectorCompactArray(indexes, values, length, container_id, buffer, vector);
        }
        else
        {
            UInt32 nonzero_cnt = 0;
            UInt32 number_of_1s = prepareBuffer(values, length, vector.fraction_bit_num, vector.getTotalBitNum(), buffer, nonzero_cnt);
            if (number_of_1s * 8 > nonzero_cnt * 64)
            {
                // 1/8 of the bits are filled with 1s in nonzero elements, we use Dense version for acceleration
                return toVectorCompactBitsetDense(indexes, values, length, container_id, buffer, vector);
            }
            return toVectorCompactBitset(indexes, values, length, container_id, buffer, vector);
        }
    }

    static void toVector(
        const PaddedPODArray<UInt32> & indexes,
        const PaddedPODArray<Float64> & values,
        const UInt32 & length,
        const UInt32 & container_id,
        BSINumericIndexedVector & vector)
    {
        PaddedPODArray<UInt64> buffer(65536);
        toVectorCompact(indexes, values, length, container_id, buffer, vector);
    }

    static UInt16 valueToColumn(
        const BSINumericIndexedVector & vector, const std::shared_ptr<Roaring> & mask, const UInt32 & container_id, Float64 * output)
    {
        PaddedPODArray<UInt64> buffer(65536);
        PaddedPODArray<UInt32> bit_buffer(65536);
        UInt16 mask_container_cardinality = mask->raGetContainerCardinality(container_id);
        if (mask_container_cardinality == 0)
            return 0;
        memset(buffer.data(), 0, buffer.size() * sizeof(UInt64));

        const UInt32 total_bit_num = vector.getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & lhs_bm = vector.getDataArrayAt(i);
            auto bit_cnt = lhs_bm->containerAndToUInt32Array(mask.get(), container_id, 0, &bit_buffer);
            for (size_t j = 0; j < bit_cnt; ++j)
            {
                buffer[bit_buffer[j]] |= (1ULL << i);
            }
        }
        auto result_cnt = mask->containerToUInt32Array(container_id, 0, bit_buffer);
        for (size_t i = 0; i < result_cnt; ++i)
        {
            output[i] = static_cast<Float64>(buffer[bit_buffer[i]]) / (1ULL << vector.fraction_bit_num);
        }
        return result_cnt;
    }

    static void pointwiseRawBinaryOperate(
        const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, const UInt32 op_code, BSINumericIndexedVector & res)
    {
        auto and_all_indexes = lhs.allIndexes();
        and_all_indexes->rb_and(*rhs.allIndexes());

        PaddedPODArray<UInt32> indexes(65536);
        PaddedPODArray<Float64> lhs_values(65536);
        PaddedPODArray<Float64> rhs_values(65536);
        PaddedPODArray<Float64> res_values(65536);

        std::set<UInt16> container_ids = and_all_indexes->raGetAllContainerIDs();
        for (const auto & container_id : container_ids)
        {
            UInt32 indexes_size = and_all_indexes->containerToUInt32Array(container_id, container_id << 16, indexes);
            if (indexes_size == 0)
                continue;
            UInt32 lhs_size = valueToColumn(lhs, and_all_indexes, container_id, lhs_values.data());
            UInt32 rhs_size = valueToColumn(rhs, and_all_indexes, container_id, rhs_values.data());
            if (indexes_size != lhs_size || lhs_size != rhs_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "{} keys_size({}), lhs_size({}), rhs_size({}) must equal in valueToColumn",
                    __func__,
                    indexes_size,
                    lhs_size,
                    rhs_size);
            switch (op_code)
            {
                case multiply_op_code:
                    for (size_t i = 0; i < indexes_size; ++i)
                    {
                        res_values[i] = lhs_values[i] * rhs_values[i];
                    }
                    break;
                case divide_op_code:
                    for (size_t i = 0; i < indexes_size; ++i)
                    {
                        res_values[i] = lhs_values[i] / rhs_values[i];
                    }
            }
            toVector(indexes, res_values, indexes_size, container_id, res);
        }
    }

    static void pointwiseRawBinaryOperate(
        const BSINumericIndexedVector & lhs, const ValueType & rhs, const UInt32 op_code, BSINumericIndexedVector & res)
    {
        auto and_all_indexes = lhs.allIndexes();

        PaddedPODArray<UInt32> indexes(65536);
        PaddedPODArray<Float64> lhs_values(65536);
        PaddedPODArray<Float64> res_values(65536);

        std::set<UInt16> container_ids = and_all_indexes->raGetAllContainerIDs();
        for (const auto & container_id : container_ids)
        {
            UInt32 indexes_size = and_all_indexes->containerToUInt32Array(container_id, container_id << 16, indexes);
            if (indexes_size == 0)
                continue;
            UInt32 lhs_size = valueToColumn(lhs, and_all_indexes, container_id, lhs_values.data());
            if (indexes_size != lhs_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "{} keys_size({}), lhs_size({}) must equal in valueToColumn",
                    __func__,
                    indexes_size,
                    lhs_size);
            switch (op_code)
            {
                case multiply_op_code:
                    for (size_t i = 0; i < indexes_size; ++i)
                    {
                        res_values[i] = lhs_values[i] * rhs;
                    }
                    break;
                case divide_op_code:
                    for (size_t i = 0; i < indexes_size; ++i)
                    {
                        res_values[i] = lhs_values[i] / rhs;
                    }
            }
            toVector(indexes, res_values, indexes_size, container_id, res);
        }
    }

    // Multiply
    static void pointwiseMultiply(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        if (lhs.allValuesEqualOne())
        {
            rhs.andBitmap(*lhs.getDataArrayAt(lhs.fraction_bit_num), res);
            return;
        }
        else if (rhs.allValuesEqualOne())
        {
            lhs.andBitmap(*rhs.getDataArrayAt(lhs.fraction_bit_num), res);
            return;
        }
        UInt32 max_integer_bit_num = std::max(lhs.integer_bit_num, rhs.integer_bit_num);
        UInt32 max_fraction_bit_num = std::max(lhs.fraction_bit_num, rhs.fraction_bit_num);

        checkIntergerFractionBitNum(max_integer_bit_num, max_fraction_bit_num);

        res.initialize(max_integer_bit_num, max_fraction_bit_num);
        pointwiseRawBinaryOperate(lhs, rhs, multiply_op_code, res);
    }
    static void pointwiseMultiply(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        if (lhs.allValuesEqualOne())
        {
            res.initializeFromVectorAndValue(lhs, rhs);
            return;
        }
        res.initialize(lhs.integer_bit_num, lhs.fraction_bit_num);
        pointwiseRawBinaryOperate(lhs, rhs, multiply_op_code, res);
    }

    // Divide
    static void pointwiseDivide(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        if (rhs.allValuesEqualOne())
        {
            res.deepCopyFrom(lhs);
            return;
        }
        UInt32 max_integer_bit_num = std::max(lhs.integer_bit_num, rhs.integer_bit_num);
        UInt32 max_fraction_bit_num = std::max(lhs.fraction_bit_num, rhs.fraction_bit_num);

        checkIntergerFractionBitNum(max_integer_bit_num, max_fraction_bit_num);

        res.initialize(max_integer_bit_num, max_fraction_bit_num);
        pointwiseRawBinaryOperate(lhs, rhs, divide_op_code, res);
    }
    static void pointwiseDivide(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        if (rhs == 1)
        {
            res.deepCopyFrom(lhs);
            return;
        }
        res.initialize(lhs.integer_bit_num, lhs.fraction_bit_num);
        pointwiseRawBinaryOperate(lhs, rhs, divide_op_code, res);
    }

    static std::shared_ptr<Roaring> pointwiseEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        auto res_bm = lhs.allIndexes();

        BSINumericIndexedVector lhs_ref;
        lhs_ref.shallowCopyFrom(lhs);
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        UInt32 total_bit_num = promoteBitPrecisionInplace(lhs_ref, rhs_ref);
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            Roaring x_xor_y;
            x_xor_y.rb_or(*lhs_ref.getDataArrayAt(i));
            x_xor_y.rb_xor(*rhs_ref.getDataArrayAt(i));
            res_bm->rb_andnot(x_xor_y);
        }
        return res_bm;
    }

    static std::shared_ptr<Roaring> pointwiseEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs)
    {
        auto res_bm = lhs.allIndexes();

        UInt64 long_value = UInt64(std::floor(rhs));
        UInt64 decimal_value = static_cast<UInt64>((rhs - long_value) * (1ULL << lhs.fraction_bit_num));

        size_t i = 0;
        for (; i < lhs.fraction_bit_num; ++i)
        {
            if ((decimal_value & 1L) == 1)
            {
                res_bm->rb_and(*lhs.getDataArrayAt(i));
            }
            else
            {
                res_bm->rb_andnot(*lhs.getDataArrayAt(i));
            }
            decimal_value >>= 1;
        }
        const UInt32 total_bit_num = lhs.getTotalBitNum();
        for (; i < total_bit_num; ++i)
        {
            if ((long_value & 1L) == 1)
            {
                res_bm->rb_and(*lhs.getDataArrayAt(i));
            }
            else
            {
                res_bm->rb_andnot(*lhs.getDataArrayAt(i));
            }
            long_value >>= 1;
        }
        if (long_value != 0)
        {
            Roaring for_clear;
            res_bm->rb_and(for_clear);
        }
        return res_bm;
    }

    // Equal
    static void pointwiseEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        res.getDataArrayAt(res.fraction_bit_num)->rb_or(*pointwiseEqual(lhs, rhs));
    }


    static void pointwiseEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        res.getDataArrayAt(res.fraction_bit_num)->rb_or(*pointwiseEqual(lhs, rhs));
    }

    // Not equal
    static void pointwiseNotEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);

        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);

        BSINumericIndexedVector lhs_ref;
        lhs_ref.shallowCopyFrom(lhs);
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        UInt32 total_bit_num = promoteBitPrecisionInplace(lhs_ref, rhs_ref);

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            Roaring x_xor_y;
            x_xor_y.rb_or(*lhs_ref.getDataArrayAt(i));
            x_xor_y.rb_xor(*rhs_ref.getDataArrayAt(i));
            res_bm->rb_or(x_xor_y);
        }
    }

    static void pointwiseNotEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        pointwiseEqual(lhs, rhs, res);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);

        auto lhs_all_indexes = lhs.getAllIndex();
        lhs_all_indexes->rb_andnot(*res_bm);

        res_bm = lhs_all_indexes;
    }

    // Less
    static std::shared_ptr<Roaring> pointwiseLess(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        auto res_bm = std::make_shared<Roaring>();

        BSINumericIndexedVector lhs_ref;
        lhs_ref.shallowCopyFrom(lhs);
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        UInt32 total_bit_num = promoteBitPrecisionInplace(lhs_ref, rhs_ref);

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            const auto & minuend = lhs_ref.getDataArrayAt(i);
            const auto & subtrahend = rhs_ref.getDataArrayAt(i);

            Roaring bout;
            bout.rb_or(*res_bm);
            bout.rb_and(*subtrahend);

            res_bm->rb_or(*subtrahend);
            res_bm->rb_andnot(*minuend);

            res_bm->rb_or(bout);
        }
        return res_bm;
    }

    static std::shared_ptr<Roaring> pointwiseLess(const BSINumericIndexedVector & lhs, const ValueType & rhs)
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        return pointwiseLess(lhs, rhs_vec);
    }

    // Less
    static void pointwiseLess(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(lhs, rhs);
    }

    static void pointwiseLess(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(lhs, rhs);
    }


    // Less Equal
    static void pointwiseLessEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        auto lt_bm = pointwiseLess(lhs, rhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*lt_bm);
        res_bm->rb_or(*eq_bm);
    }

    static void pointwiseLessEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        auto lt_bm = pointwiseLess(lhs, rhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*lt_bm);
        res_bm->rb_or(*eq_bm);
    }

    // Greater
    static void pointwiseGreater(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(rhs, lhs);
    }

    static void pointwiseGreater(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);

        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res_bm = pointwiseLess(rhs_vec, lhs);
    }

    // Greater Equal
    static void
    pointwiseGreaterEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        auto gt_bm = pointwiseLess(rhs, lhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*gt_bm);
        res_bm->rb_or(*eq_bm);
    }

    static void pointwiseGreaterEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);

        auto gt_bm = pointwiseLess(rhs_vec, lhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*gt_bm);
        res_bm->rb_or(*eq_bm);
    }

    void addValue(IndexType index, ValueType value)
    {
        const UInt32 total_bit_num = getTotalBitNum();

        Int64 scaled_value = Int64(value * (1L << fraction_bit_num));

        UInt8 cin = false;
        for (size_t j = 0; j < total_bit_num; ++j)
        {
            UInt8 augend = 0;
            if (sizeof(IndexType) >= 8)
            {
                UInt64 ele = static_cast<UInt64>(index);
                augend = getDataArrayAt(j)->rb_contains(ele) ? 1 : 0;
            }
            else
            {
                UInt32 ele = static_cast<UInt32>(index);
                augend = getDataArrayAt(j)->rb_contains(ele) ? 1 : 0;
            }
            UInt8 addend = (scaled_value & (1L << j)) > 0 ? 1 : 0;

            UInt8 x_xor_y = augend ^ addend;
            UInt8 x_and_y = augend & addend;

            UInt8 sum = augend ^ addend ^ cin;

            if ((sum & 1) == 1)
            {
                if (sizeof(IndexType) >= 8)
                {
                    UInt64 ele = static_cast<UInt64>(index);
                    getDataArrayAt(j)->add(ele);
                }
                else
                {
                    UInt32 ele = static_cast<UInt32>(index);
                    getDataArrayAt(j)->add(ele);
                }
            }
            cin = cin & x_xor_y;
            cin = cin | x_and_y;
        }
    }

    ValueType getValue(IndexType index) const
    {
        const UInt32 total_bit_num = getTotalBitNum();
        if (total_bit_num == 0)
            return 0;

        UInt64 scaled_value = 0;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (getDataArrayAt(i)->rb_contains(index))
            {
                scaled_value |= (1ULL << i);
            }
        }
        return static_cast<ValueType>(scaled_value) / (1LL << fraction_bit_num);
    }

    Float64 getAllValueSum() const
    {
        const UInt32 total_bit_num = getTotalBitNum();
        if (total_bit_num == 0)
            return 0;
        Float64 value = 0;
        DataTypePtr value_type = std::make_shared<DataTypeNumber<ValueType>>();
        auto which = WhichDataType(value_type);
        if (which.isUInt())
        {
            for (size_t i = 0; i < total_bit_num; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));
                value += getDataArrayAt(i)->size() * bit_contribution;
            }
        }
        else if (which.isInt() || which.isFloat())
        {
            Int64 sign_bit_index = total_bit_num - 1;

            Roaring negative_indexes;
            negative_indexes.rb_or(*getDataArrayAt(sign_bit_index));

            // Handle positive indexes
            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));

                Roaring positive_indexes;
                positive_indexes.rb_or(*getDataArrayAt(i));

                positive_indexes.rb_andnot(negative_indexes);
                value += positive_indexes.size() * bit_contribution;
            }

            // Handle negative indexes
            Roaring cin;
            cin.rb_or(negative_indexes);

            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));
                Roaring augend;
                augend.rb_or(negative_indexes);
                augend.rb_andnot(*getDataArrayAt(i));

                Roaring sum;
                sum.rb_or(augend);
                sum.rb_xor(cin);

                value -= sum.size() * bit_contribution;

                cin.rb_and(augend);
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported value type for getAllValueSum()");
        }
        return value;
    }

    UInt64 getCardinality() const
    {
        Roaring total_bm;
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            total_bm.rb_or(*getDataArrayAt(i));
        }
        return total_bm.size();
    }

    UInt64 toIndexValueMap(PaddedPODArray<IndexType> & indexes_pod, PaddedPODArray<ValueType> & values_pod) const
    {
        const UInt32 total_bit_num = getTotalBitNum();
        if (total_bit_num == 0)
            return 0;
        DataTypePtr value_type = std::make_shared<DataTypeNumber<ValueType>>();
        auto which = WhichDataType(value_type);
        std::map<IndexType, Int64> index2value;
        if (which.isUInt())
        {
            for (size_t i = 0; i < total_bit_num; ++i)
            {
                PaddedPODArray<IndexType> indexes;
                getDataArrayAt(i)->rb_to_array(indexes);
                for (size_t j = 0; j < indexes.size(); ++j)
                {
                    index2value[indexes[j]] |= (1ULL << i);
                }
            }
        }
        else if (which.isInt() || which.isFloat())
        {
            UInt32 sign_bit_index = total_bit_num - 1;
            Roaring all_negative_indexes;
            all_negative_indexes.rb_or(*getDataArrayAt(sign_bit_index));
            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Roaring cur_positive_indexes;
                cur_positive_indexes.rb_or(*getDataArrayAt(i));
                cur_positive_indexes.rb_andnot(all_negative_indexes);

                PaddedPODArray<IndexType> cur_positive_indexes_array;
                cur_positive_indexes.rb_to_array(cur_positive_indexes_array);
                for (size_t j = 0; j < cur_positive_indexes_array.size(); ++j)
                {
                    index2value[cur_positive_indexes_array[j]] |= (1ULL << i);
                }

                Roaring cur_negative_indexes;
                cur_negative_indexes.rb_or(*getDataArrayAt(i));
                cur_negative_indexes.rb_andnot(cur_positive_indexes);
                cur_negative_indexes.rb_xor(all_negative_indexes);

                PaddedPODArray<IndexType> cur_negative_indexes_array;
                cur_negative_indexes.rb_to_array(cur_negative_indexes_array);
                for (size_t j = 0; j < cur_negative_indexes_array.size(); ++j)
                {
                    index2value[cur_negative_indexes_array[j]] |= (1ULL << i);
                }
            }
            PaddedPODArray<IndexType> all_negative_indexes_array;
            all_negative_indexes.rb_to_array(all_negative_indexes_array);
            for (size_t i = 0; i < all_negative_indexes_array.size(); ++i)
            {
                index2value[all_negative_indexes_array[i]] += 1;
                index2value[all_negative_indexes_array[i]] *= -1;
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported value type for getAllValueSum()");
        }

        for (auto & [index, value] : index2value)
        {
            indexes_pod.emplace_back(index);
            values_pod.emplace_back(static_cast<ValueType>(value / (std::pow(2.0, fraction_bit_num))));
        }
        return index2value.size();
    }

    void read(DB::ReadBuffer & in)
    {
        readBinary(integer_bit_num, in);
        readBinary(fraction_bit_num, in);
        const UInt32 total_bit_num = getTotalBitNum();
        data_array.clear();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<Roaring>());
        }


        for (size_t i = 0; i < total_bit_num; ++i)
        {
            UInt8 is_empty = 0;
            readBinary(is_empty, in);
            if (is_empty == 1)
                continue;
            else if (is_empty != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown value of is_empty: {}", toString(is_empty));
            getDataArrayAt(i)->read(in);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        writeBinary(integer_bit_num, out);
        writeBinary(fraction_bit_num, out);

        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            UInt8 is_empty = 0;
            if (getDataArrayAt(i)->size() == 0)
                is_empty = 1;
            writeBinary(is_empty, out);
            if (is_empty == 1)
                continue;
            getDataArrayAt(i)->write(out);
        }
    }
};

template <typename VectorImpl>
class NumericIndexedVector
{
private:
    VectorImpl impl;

    using IndexType = typename VectorImpl::IndexType;
    using ValueType = typename VectorImpl::ValueType;

public:
    template <typename... TArgs>
    void initialize(TArgs... args)
    {
        impl.initialize(std::forward<TArgs>(args)...);
    }
    void deepCopyFrom(const NumericIndexedVector & rhs) { impl.deepCopyFrom(rhs.impl); }

    void merge(const NumericIndexedVector & rhs) { impl.merge(rhs.impl); }

    static void pointwiseAdd(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseAdd(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseAdd(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseAdd(lhs.impl, rhs, res.impl);
    }

    static void pointwiseSubtract(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseSubtract(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseSubtract(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseSubtract(lhs.impl, rhs, res.impl);
    }

    static void pointwiseMultiply(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseMultiply(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseMultiply(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseMultiply(lhs.impl, rhs, res.impl);
    }

    static void pointwiseDivide(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseDivide(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseDivide(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseDivide(lhs.impl, rhs, res.impl);
    }

    static void pointwiseEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseNotEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseNotEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseNotEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseNotEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseLess(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLess(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseLess(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLess(lhs.impl, rhs, res.impl);
    }

    static void pointwiseLessEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLessEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseLessEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseLessEqual(lhs.impl, rhs, res.impl);
    }

    static void pointwiseGreater(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreater(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseGreater(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreater(lhs.impl, rhs, res.impl);
    }

    static void pointwiseGreaterEqual(const NumericIndexedVector & lhs, const NumericIndexedVector & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreaterEqual(lhs.impl, rhs.impl, res.impl);
    }

    static void pointwiseGreaterEqual(const NumericIndexedVector & lhs, const ValueType & rhs, NumericIndexedVector & res)
    {
        VectorImpl::pointwiseGreaterEqual(lhs.impl, rhs, res.impl);
    }

    void addValue(IndexType index, ValueType value) { impl.addValue(index, value); }

    ValueType getValue(IndexType index) const { return impl.getValue(index); }

    Float64 getAllValueSum() const { return impl.getAllValueSum(); }

    UInt64 getCardinality() const { return impl.getCardinality(); }

    String shortDebugString() const { return impl.shortDebugString(); }

    UInt64 toIndexValueMap(PaddedPODArray<IndexType> & indexes_pod, PaddedPODArray<ValueType> & values_pod) const
    {
        return impl.toIndexValueMap(indexes_pod, values_pod);
    }

    void read(DB::ReadBuffer & in) { impl.read(in); }

    void write(DB::WriteBuffer & out) const { impl.write(out); }
};

template <typename VectorImpl>
struct AggregateFunctionGroupNumericIndexedVectorData
{
    bool init = false;
    NumericIndexedVector<VectorImpl> vector;
    static const char * name() { return "groupNumericIndexedVector"; }
};

}
