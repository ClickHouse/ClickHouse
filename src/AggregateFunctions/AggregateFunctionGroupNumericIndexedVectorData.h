#pragma once

#include <chrono>
#include <memory>
#include <type_traits>
#include <unordered_map>
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
#include <roaring/roaring.h>
#include <roaring/roaring_types.h>

#include <roaring.hh>
#include <roaring64map.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int TOO_LARGE_ARRAY_SIZE;
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


template <typename IndexType, typename ValueType>
class BSINumericIndexedVector : private boost::noncopyable
{
private:
    UInt32 integer_bit_num;
    UInt32 fraction_bit_num;

    using RoaringBitmap = std::conditional_t<sizeof(IndexType) >= 8, roaring::Roaring64Map, roaring::Roaring>;
    std::vector<std::shared_ptr<RoaringBitmap>> data_array;

public:
    static constexpr auto type = "BSI";
    static constexpr UInt32 DEFAULT_INTEGER_BIT_NUM = 32;
    static constexpr UInt32 DEFAULT_FRACTION_BIT_NUM = 14;
    static constexpr UInt32 MAX_INTEGER_BIT_NUM = 40;
    static constexpr UInt32 MAX_FRACTION_BIT_NUM = 24;
    static constexpr size_t max_size = 10_GiB;

    BSINumericIndexedVector() = default;

    ~BSINumericIndexedVector() = default;

    std::string shortDebugString() const
    {
        std::string data_array_details;
        for (size_t i = 0; i < data_array.size(); ++i)
        {
            data_array_details += fmt::format("{}: {}, ", i, getDataArrayAt(i)->cardinality());
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

    void initialize(UInt32 new_integer_bit_num, UInt32 new_fraction_bit_num)
    {
        if (new_integer_bit_num > MAX_INTEGER_BIT_NUM || new_fraction_bit_num > MAX_FRACTION_BIT_NUM)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "integer_bit_num and fraction_bit_num must be less than or equal to {} and {} respectly",
                MAX_INTEGER_BIT_NUM,
                MAX_FRACTION_BIT_NUM);

        integer_bit_num = new_integer_bit_num;
        fraction_bit_num = new_fraction_bit_num;

        data_array.clear();
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<RoaringBitmap>());
        }
    }

    void initializeFromVectorAndValue(const BSINumericIndexedVector & rhs, ValueType value)
    {
        initialize(rhs.integer_bit_num, rhs.fraction_bit_num);

        RoaringBitmap all_index = rhs.getAllIndex();

        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        Int64 scaled_value = Int64(value * (1LL << fraction_bit_num));
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (scaled_value & (1L << i))
            {
                *getDataArrayAt(i) |= all_index;
            }
        }
    }

    RoaringBitmap getAllIndex() const
    {
        RoaringBitmap bm;
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            bm |= *getDataArrayAt(i);
        }
        return bm;
    }

    void deepCopyFrom(const BSINumericIndexedVector & rhs)
    {
        integer_bit_num = rhs.integer_bit_num;
        fraction_bit_num = rhs.fraction_bit_num;
        const UInt32 total_bit_num = rhs.integer_bit_num + rhs.fraction_bit_num;
        data_array.clear();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<RoaringBitmap>());
            *data_array[i] |= *rhs.getDataArrayAt(i);
        }
    }

    void shallowCopyFrom(const BSINumericIndexedVector & rhs)
    {
        integer_bit_num = rhs.integer_bit_num;
        fraction_bit_num = rhs.fraction_bit_num;
        data_array.clear();
        data_array = rhs.data_array;
    }

    void changeSchema(UInt32 new_integer_bit_num, UInt32 new_fraction_bit_num)
    {
        if (new_integer_bit_num == integer_bit_num && new_fraction_bit_num == fraction_bit_num)
            return;
        UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        UInt32 new_total_bit_num = new_integer_bit_num + new_fraction_bit_num;

        auto tmp_data_array = data_array;

        data_array.clear();
        for (size_t i = 0; i < new_total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<RoaringBitmap>());
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

    static void promoteBitPrecisionInplace(BSINumericIndexedVector & lhs, BSINumericIndexedVector & rhs)
    {
        if (lhs.integer_bit_num == rhs.integer_bit_num && lhs.integer_bit_num == rhs.fraction_bit_num)
        {
            return;
        }
        if (lhs.integer_bit_num <= rhs.integer_bit_num && lhs.fraction_bit_num <= rhs.fraction_bit_num)
        {
            lhs.changeSchema(rhs.integer_bit_num, rhs.fraction_bit_num);
            return;
        }
        if (lhs.integer_bit_num >= rhs.integer_bit_num && lhs.fraction_bit_num >= rhs.fraction_bit_num)
        {
            rhs.changeSchema(lhs.integer_bit_num, lhs.fraction_bit_num);
            return;
        }

        UInt32 max_integer_bit_num = std::max(lhs.integer_bit_num, rhs.integer_bit_num);
        UInt32 max_fraction_bit_num = std::max(lhs.fraction_bit_num, rhs.fraction_bit_num);
        if (max_integer_bit_num > MAX_INTEGER_BIT_NUM || max_fraction_bit_num > MAX_FRACTION_BIT_NUM)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "integer_bit_num and fraction_bit_num must be less than or equal to {} and {} respectly",
                MAX_INTEGER_BIT_NUM,
                MAX_FRACTION_BIT_NUM);
        lhs.changeSchema(max_integer_bit_num, max_fraction_bit_num);
        rhs.changeSchema(max_integer_bit_num, max_fraction_bit_num);
    }

    std::shared_ptr<RoaringBitmap> & getDataArrayAt(size_t index)
    {
        if (index >= data_array.size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array index out of bounds.");
        }
        return data_array[index];
    }
    const std::shared_ptr<RoaringBitmap> & getDataArrayAt(size_t index) const
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
        promoteBitPrecisionInplace(*this, rhs_ref);

        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;

        RoaringBitmap cin;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & augend = getDataArrayAt(i);
            const auto & addend = rhs_ref.getDataArrayAt(i);

            RoaringBitmap x_and_y;
            x_and_y |= *augend;
            x_and_y &= *addend;

            *augend ^= *addend;

            RoaringBitmap backup_cin = cin;
            cin &= *augend;
            cin |= x_and_y;

            *augend ^= backup_cin;
        }
    }

    void pointwiseAdd(const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res) const
    {
        res.deepCopyFrom(*this);
        res.pointwiseAddInplace(rhs);
    }

    void pointwiseAdd(const ValueType & rhs, BSINumericIndexedVector & res) const
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(*this, rhs);
        pointwiseAdd(rhs_vec, res);
    }

    static void pointwiseAdd(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseAddInplace(rhs);
    }

    void merge(const BSINumericIndexedVector & rhs) { pointwiseAddInplace(rhs); }

    void pointwiseSubtract(const BSINumericIndexedVector & rhs)
    {
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        promoteBitPrecisionInplace(*this, rhs_ref);

        UInt32 total_bit_num = integer_bit_num + fraction_bit_num;

        RoaringBitmap bin;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & minuend = getDataArrayAt(i);
            auto & subtrahend = rhs_ref.getDataArrayAt(i);

            RoaringBitmap subtrahend_or_bin;
            subtrahend_or_bin |= *subtrahend;
            subtrahend_or_bin |= bin;

            subtrahend_or_bin -= *minuend;

            *minuend ^= *subtrahend;
            *minuend ^= bin;

            bin &= *subtrahend;
            bin |= subtrahend_or_bin;
        }
    }

    void pointwiseSubtract(const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res) const
    {
        res.deepCopyFrom(*this);
        res.pointwiseSubtract(rhs);
    }

    static void pointwiseSubtract(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseSubtract(rhs);
    }

    void addIndexValue(IndexType index, ValueType value)
    {
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;

        Int64 scaled_value = Int64(value * (1L << fraction_bit_num));

        UInt8 cin = false;
        for (size_t j = 0; j < total_bit_num; ++j)
        {
            UInt8 augend = 0;
            if (sizeof(IndexType) >= 8)
            {
                UInt64 ele = static_cast<UInt64>(index);
                augend = getDataArrayAt(j)->contains(ele) ? 1 : 0;
            }
            else
            {
                UInt32 ele = static_cast<UInt32>(index);
                augend = getDataArrayAt(j)->contains(ele) ? 1 : 0;
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

    Float64 getAllValueSum() const
    {
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        Float64 value = 0;
        DataTypePtr value_type = std::make_shared<DataTypeNumber<ValueType>>();
        auto which = WhichDataType(value_type);
        if (which.isUInt())
        {
            for (size_t i = 0; i < total_bit_num; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));
                value += getDataArrayAt(i)->cardinality() * bit_contribution;
            }
        }
        else if (which.isInt() || which.isFloat())
        {
            Int64 sign_bit_index = total_bit_num - 1;

            RoaringBitmap negative_indexes;
            negative_indexes |= *getDataArrayAt(sign_bit_index);

            // Handle positive indexes
            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));

                RoaringBitmap positive_indexes;
                positive_indexes |= *getDataArrayAt(i);

                positive_indexes -= negative_indexes;
                value += positive_indexes.cardinality() * bit_contribution;
            }

            // Handle negative indexes
            RoaringBitmap cin;
            cin |= negative_indexes;

            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));
                RoaringBitmap augend;
                augend |= negative_indexes;
                augend -= *getDataArrayAt(i);

                RoaringBitmap sum;
                sum |= augend;
                sum ^= cin;

                value -= sum.cardinality() * bit_contribution;

                cin &= augend;
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
        RoaringBitmap total_bm;
        UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            total_bm |= *getDataArrayAt(i);
        }
        return total_bm.cardinality();
    }

    UInt64 toIndexValueMap(PaddedPODArray<IndexType> & indexes_pod, PaddedPODArray<ValueType> & values_pod) const
    {
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;

        std::map<IndexType, Int64> index2value;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            PaddedPODArray<IndexType> indexes;
            const auto & rb = getDataArrayAt(i);
            for (auto it = rb->begin(); it != rb->end(); ++it)
            {
                indexes.emplace_back(*it);
            }
            for (size_t j = 0; j < indexes.size(); ++j)
            {
                index2value[indexes[j]] |= (1ULL << i);
            }
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
        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        data_array.clear();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<RoaringBitmap>());
        }


        for (size_t i = 0; i < total_bit_num; ++i)
        {
            UInt8 is_empty = 0;
            readBinary(is_empty, in);
            if (is_empty == 1)
                continue;
            else if (is_empty != 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown value of is_empty: {}", toString(is_empty));

            size_t size;
            readVarUInt(size, in);
            if (size == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size (0) in RoaringBitmap.");
            if (size > max_size)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in RoaringBitmap.");
            std::unique_ptr<char[]> buf(new char[size]);
            in.readStrict(buf.get(), size);
            getDataArrayAt(i) = std::make_shared<RoaringBitmap>(std::move(RoaringBitmap::read(buf.get())));
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        writeBinary(integer_bit_num, out);
        writeBinary(fraction_bit_num, out);

        const UInt32 total_bit_num = integer_bit_num + fraction_bit_num;
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            UInt8 is_empty = 0;
            if (getDataArrayAt(i)->cardinality() == 0)
                is_empty = 1;
            writeBinary(is_empty, out);
            if (is_empty == 1)
                continue;
            auto size = getDataArrayAt(i)->getSizeInBytes();
            writeVarUInt(size, out);
            std::unique_ptr<char[]> buf(new char[size]);
            getDataArrayAt(i)->write(buf.get());
            out.write(buf.get(), size);
        }
    }
};

template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
class NumericIndexedVector
{
private:
    VectorImpl<IndexType, ValueType> impl;

public:
    NumericIndexedVector() = default;

    template <typename... TArgs>
    void initialize(TArgs... args)
    {
        impl.initialize(std::forward<TArgs>(args)...);
    }
    void deepCopyFrom(const NumericIndexedVector & rhs) { impl.deepCopyFrom(rhs.impl); }

    void merge(const NumericIndexedVector & rhs) { impl.merge(rhs.impl); }

    void pointwiseAdd(const NumericIndexedVector & rhs, NumericIndexedVector & res) const { impl.pointwiseAdd(rhs.impl, res.impl); }

    void pointwiseAdd(const ValueType & rhs, NumericIndexedVector & res) const { impl.pointwiseAdd(rhs, res.impl); }

    void addIndexValue(IndexType index, ValueType value) { impl.addIndexValue(index, value); }

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

template <typename IndexType, typename ValueType, template <typename, typename> class VectorImpl>
struct AggregateFunctionGroupNumericIndexedVectorData
{
    bool init = false;
    NumericIndexedVector<IndexType, ValueType, VectorImpl> vector;
    static const char * name() { return "groupNumericIndexedVector"; }
};

}
