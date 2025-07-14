#pragma once

#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <base/demangle.h>
#include <Common/JSONBuilder.h>

/// Include this last — see the reason inside
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <roaring/containers/containers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

/** The following example demonstrates the Bit-Sliced Index (BSI) storage mechanism.
 * This is implementation of https://dl.acm.org/doi/10.14778/3685800.3685823.
 * Less dense explanation is here: https://github.com/ClickHouse/ClickHouse/issues/70582.
 * Original Vector:
 *  Suppose we have a sparse vector with:
 *  - Length: 4294967295 (UINT32_MAX).
 *  - Value range: [0, 15] with 3 non-zero elements and 2 zero element. other elements do not exists.
 *  - Non-zero indexes: 1000, 2000, 3000.
 *  - zero indexes: 1, 2.
 *           index           value
 *               0               -
 *               1               0
 *               2               0
 *             ...             ...
 *            1000            5.25
 *             ...             ...
 *            2000             8.5
 *             ...             ...
 *            3000           7.625
 *             ...             ...
 *           10000               -
 *             ...             ...
 *      4294967295               -
 * Binary Conversion
 *  (Because value range in [0, 15], 4 bits for integer is enough),
 *  (For the decimal part, a fixed number of bits is used to represent it, we use 3 in this example),
 *      index      value      binary(value)
 *       1000       5.25           0101.010 (5.25 = 5 + 0.25)
 *       2000        8.5           1000.100 (8.5 = 8 + 0.5)
 *       3000      7.625           0111.101 (7.625 = 7 + 0.625)
 * From right to left(LSB -> MSB), each bit using RoargingBitmap to stores indexes which value is 1
 *      Bit position            Roaring Bitmap                  Converge
 *        bm0 (bit0)       bitmapBuild([3000])        Fractional 1st bit
 *        bm1 (bit1)       bitmapBuild([1000])        Fractional 2nd bit
 *        bm2 (bit2) bitmapBuild([2000, 3000])        Fractional 3rd bit
 *        bm3 (bit3) bitmapBuild([1000, 3000])           Integer 1st bit
 *        bm4 (bit4)       bitmapBuild([3000])           Integer 2nd bit
 *        bm5 (bit5) bitmapBuild([1000, 3000])           Integer 3rd bit
 *        bm6 (bit6)       bitmapBuild([2000])           Integer 4th bit
 * Data array organization
 *      zero_indexes = bitmapBuild([1, 2])
 *      data_array = [bm0, bm1, bm2, bm3, bm4, bm5, bm6]
 *                    \___________/  \________________/
 *             fraction_bit_num = 3  interger_bit_num = 4
 * In subsequent comments, we denote the original vector v stored in a NumericIndexedVector object obj as
 *      v = original_vector(obj).
 */
template <typename IT, typename VT>
class BSINumericIndexedVector
{
public:
    using IndexType = IT;
    using ValueType = VT;

    static constexpr auto type = "BSI";

    /** For Float ValueType:
     * - Use 40-bit fixed-point representation for integer part.
     *   Which means supported value range is [-2^39, 2^39 - 1] in the signed scenario.
     * - Use 24-bit represent decimal part, provides about 10^-7~10^-8(2^-24) resolution.
     */
    static constexpr UInt32 DEFAULT_INTEGER_BIT_NUM = 40;
    static constexpr UInt32 DEFAULT_FRACTION_BIT_NUM = 24;

    static constexpr UInt32 MAX_INTEGER_BIT_NUM = 64;
    static constexpr UInt32 MAX_FRACTION_BIT_NUM = 24;

    /// Another constraint: integer_bit_num + fraction_bit_num <= MAX_TOTAL_BIT_NUM.
    static constexpr UInt32 MAX_TOTAL_BIT_NUM = 64;

    static constexpr size_t max_size = 10_GiB;

    static constexpr UInt32 multiply_op_code = 2;
    static constexpr UInt32 divide_op_code = 3;

private:
    UInt32 integer_bit_num;
    UInt32 fraction_bit_num;

    using Roaring = RoaringBitmapWithSmallSet<IT, 32>;

    /** We distinguish between the cases where index's value does not exist and index's value is 0.
     * zero_indexes is used to store all indexes whose value equal 0.
     * data_array stores all indexes and values whose value is not 0 using roaringBitmap and Bit-Sliced Index.
     */
    std::shared_ptr<Roaring> zero_indexes = std::make_shared<Roaring>();
    std::vector<std::shared_ptr<Roaring>> data_array;

    /// The only way NaN and Inf values can enter BSI is if user adds them as they cannot appear in BSI by any permitted operation.
    /// Do not allow user to do this as it achieves nothing and is very likely by mistake.
    constexpr inline static void checkValidValue(const ValueType & value)
    {
        if constexpr (std::is_floating_point_v<ValueType>)
        {
            if (isnan(value))
                throw Exception(ErrorCodes::INCORRECT_DATA, "NumericIndexedVector does not support NaN");
            if (isinf(value))
                throw Exception(ErrorCodes::INCORRECT_DATA, "NumericIndexedVector does not support Inf");
        }
    }


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
        JSONBuilder::JSONMap debug_info_map;

        /// Basic information.
        debug_info_map.add("vector_type", type);
        debug_info_map.add("index_type", demangle(typeid(IndexType).name()));
        debug_info_map.add("value_type", demangle(typeid(ValueType).name()));
        debug_info_map.add("integer_bit_num", integer_bit_num);
        debug_info_map.add("fraction_bit_num", fraction_bit_num);
        /// Zero indexes information.
        auto zero_indexes_info_map = std::make_unique<JSONBuilder::JSONMap>();
        zero_indexes_info_map->add("cardinality", zero_indexes->size());

        debug_info_map.add("zero_indexes_info", std::move(zero_indexes_info_map));

        /// Non-zero indexes information.
        auto non_zero_indexes_info_map = std::make_unique<JSONBuilder::JSONMap>();
        non_zero_indexes_info_map->add("total_cardinality", getAllIndex()->size());
        non_zero_indexes_info_map->add("all_value_sum", getAllValueSum());
        non_zero_indexes_info_map->add("number_of_bitmaps", data_array.size());

        auto data_array_info_map = std::make_unique<JSONBuilder::JSONMap>();
        auto bitmap_cardinality_map = std::make_unique<JSONBuilder::JSONMap>();
        for (size_t i = 0; i < data_array.size(); ++i)
        {
            bitmap_cardinality_map->add(fmt::format("{}", i), getDataArrayAt(i)->size());
        }
        data_array_info_map->add("cardinality", std::move(bitmap_cardinality_map));

        non_zero_indexes_info_map->add("bitmap_info", std::move(data_array_info_map));

        debug_info_map.add("non_zero_indexes_info", std::move(non_zero_indexes_info_map));

        WriteBufferFromOwnString buf;

        JSONBuilder::FormatSettings settings = {{}, 2, true, true};
        JSONBuilder::FormatContext context = {buf, 0};
        debug_info_map.format(settings, context);
        return buf.str();
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

    bool isEmpty() const
    {
        if (zero_indexes->size() > 0)
            return false;
        UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (getDataArrayAt(i)->size() > 0)
                return false;
        }
        return true;
    }

    bool isValueTypeSigned() const
    {
        WhichDataType second_which(DataTypeNumber<ValueType>().getTypeId());
        if (second_which.isInt() || second_which.isFloat())
            return true;
        return false;
    }
    void initialize(UInt32 new_integer_bit_num, UInt32 new_fraction_bit_num)
    {
        checkIntergerFractionBitNum(new_integer_bit_num, new_fraction_bit_num);
        integer_bit_num = new_integer_bit_num;
        fraction_bit_num = new_fraction_bit_num;

        zero_indexes = std::make_shared<Roaring>();

        data_array.clear();
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            data_array.push_back(std::make_shared<Roaring>());
        }
    }

    /**
     * Initialize the BSINumericIndexedVector from another BSINumericIndexedVector and a value.
     * The result vector's index is the same as source vector rhs and set all indexes to a specific value.
     */
    void initializeFromVectorAndValue(const BSINumericIndexedVector & rhs, ValueType value)
    {
        checkValidValue(value);
        initialize(rhs.integer_bit_num, rhs.fraction_bit_num);

        auto all_index = rhs.getAllIndex();

        if (value == 0)
        {
            zero_indexes->rb_or(*all_index);
            return;
        }

        const UInt32 total_bit_num = getTotalBitNum();

        /** This converts a floating-point value into a fixed-point representation, then store it in data_array using bit-sliced index.
          * - When value is an UInt/Int, fraction_bit_num is usually set to 0. So when integer_bit_num is set to the number of
          *   storage bits of Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64(set integer_bit_num = 8 when value type is UInt8/Int8
          *   and set integer_bit_num = 64 when value type is UInt64/Int64 etc.), the expression of numericIndexedVector is not
          *   limited and overflow will not occur.
          * - When value is a Float32/Float64, fraction_bit_num indicates how many bits are used to represent the decimal, Because the
          *   maximum value of total_bit_num(integer_bit_num + fraction_bit_num) is 64, overflow may occur.
          */
        Int64 scaled_value = Int64(value * (1ULL << fraction_bit_num));
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            if (scaled_value & (1ULL << i))
            {
                getDataArrayAt(i)->rb_or(*all_index);
            }
        }
    }

    std::shared_ptr<Roaring> getAllNonZeroIndex() const
    {
        auto bm = std::make_shared<Roaring>();
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            bm->rb_or(*getDataArrayAt(i));
        }
        return bm;
    }

    std::shared_ptr<Roaring> getAllIndex() const
    {
        auto bm = getAllNonZeroIndex();
        bm->rb_or(*zero_indexes);
        return bm;
    }

    void deepCopyFrom(const BSINumericIndexedVector & rhs)
    {
        integer_bit_num = rhs.integer_bit_num;
        fraction_bit_num = rhs.fraction_bit_num;
        zero_indexes = std::make_shared<Roaring>();
        zero_indexes->rb_or(*rhs.zero_indexes);
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
        zero_indexes = rhs.zero_indexes;
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
            else if (isValueTypeSigned() and total_bit_num > 0)
            {
                data_array[new_integer_idx]->rb_or(*tmp_data_array[total_bit_num - 1]);
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
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Array index out of bounds. index: {}; integer_bit_num: {}; fraction_bit_num: {}; data_array_size: {}",
                index,
                integer_bit_num,
                fraction_bit_num,
                data_array.size());
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

    /** Performs pointwise addition between two origin vectors directly on the BSI bitmap data_array.
     * The result is stored in the current object. (original_vector(this) += original_vector(rhs))
     * Reference full adder implementation: https://en.wikipedia.org/wiki/Adder_(electronics)#Full_adder
     * - An index that exists in only one operand will be considered to have a value of 0 in the other operand.
     */
    void pointwiseAddInplace(const BSINumericIndexedVector & rhs)
    {
        if (isEmpty())
        {
            deepCopyFrom(rhs);
            return;
        }

        if (rhs.isEmpty())
        {
            return;
        }

        auto total_indexes = getAllIndex();
        total_indexes->rb_or(*rhs.getAllIndex());

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

        /// For any of the total_indexes, if it is not in the non-zero index of the result, the result is 0.
        total_indexes->rb_andnot(*getAllNonZeroIndex());
        zero_indexes = total_indexes;
    }

    static void pointwiseAdd(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseAddInplace(rhs);
    }

    /** Performs pointwise addition between a vector and a scalar value rhs.
     * The result is stored in the current object. (original_vector(this) += rhs)
     */
    static void pointwiseAdd(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        if (rhs == 0)
        {
            res.deepCopyFrom(lhs);
            return;
        }
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res.deepCopyFrom(lhs);
        res.pointwiseAddInplace(rhs_vec);
    }

    void merge(const BSINumericIndexedVector & rhs) { pointwiseAddInplace(rhs); }

    /** Performs pointwise subtraction between two origin vectors directly on the BSI structure.
     * The result is stored in the current object. (original_vector(this) -= original_vector(rhs))
     * Reference full subtractor implementation: https://en.wikipedia.org/wiki/Subtractor#Full_subtractor
     */
    void pointwiseSubtractInplace(const BSINumericIndexedVector & rhs)
    {
        auto total_indexes = getAllIndex();
        total_indexes->rb_or(*rhs.getAllIndex());

        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        const UInt32 total_bit_num = promoteBitPrecisionInplace(*this, rhs_ref);

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

        /// For any of the total_indexes, if it is not in the non-zero index of the result, the result is 0.
        total_indexes->rb_andnot(*getAllNonZeroIndex());
        zero_indexes = total_indexes;
    }

    static void pointwiseSubtract(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.deepCopyFrom(lhs);
        res.pointwiseSubtractInplace(rhs);
    }

    static void pointwiseSubtract(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        if (rhs == 0)
        {
            res.deepCopyFrom(lhs);
            return;
        }
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res.deepCopyFrom(lhs);
        res.pointwiseSubtractInplace(rhs_vec);
    }

    bool allValuesEqualOne() const
    {
        if (zero_indexes->size() > 0)
            return false;

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
            res.getDataArrayAt(i)->merge(*getDataArrayAt(i));
            res.getDataArrayAt(i)->rb_and(bm);
        }

        res.zero_indexes->merge(*zero_indexes);
        res.zero_indexes->rb_and(bm);
    }

    /// Set Roaring containers to RoaringBitmapWithSmallSet
    static inline void setContainers(
        std::vector<roaring::internal::container_t *> & ctns,
        std::vector<UInt8> & types,
        UInt32 container_id,
        BSINumericIndexedVector & vector)
    {
        const UInt32 total_bit_num = vector.getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            vector.getDataArrayAt(i)->ra_set_container(ctns[i], container_id, types[i]);
        }
    }

    /// Convert Float64 to UInt64. If it is negative, convert to Int64 first and then to UInt64 for correct bit representation.
    static inline UInt64 float64ToUInt64(Float64 d)
    {
        if (d >= static_cast<Float64>(std::numeric_limits<UInt64>::max()))
            return std::numeric_limits<UInt64>::max();
        if (d <= static_cast<Float64>(std::numeric_limits<Int64>::lowest()))
            return std::numeric_limits<UInt64>::lowest();
        if (d >= 0)
            return static_cast<UInt64>(d);
        return static_cast<UInt64>(static_cast<Int64>(d));
    }

    /** There are three main types of containers in Roaring Bitmap: array, bitset and run.
     * This function converts the specified indexes and values (belonging to container_id) into BSI format and
     *  saves them in the container of each bitmap of data_array of NumericIndexedVector vector.
     *  The format of container is Array.
     **/
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
            buffer[i] = float64ToUInt64(values[i] * ratio);
            buffer[i] &= mask;
        }

        constexpr UInt32 k_batch_size = 256;
        std::vector<std::vector<UInt16>> bit_buffer(64, std::vector<UInt16>(k_batch_size, 0));
        /// number of keys in each bitmap of vector.
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
                    /// on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
                    UInt64 t = w & (~w + 1);
                    /// on x64, should compile to TZCNT
                    int i = __builtin_ctzll(w);
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
            UInt64 tmp = float64ToUInt64(values[i] * ratio);
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

    /** There are three main types of containers in Roaring Bitmap: array, bitset and run.
     * This function converts the specified indexes and values (belonging to container_id) into BSI format and
     *  saves them in the container of each bitmap of data_array of NumericIndexedVector vector.
     *  The format of container is BitsetDense.
     **/
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

#if defined(__AVX512__)
            cnt = roaring::internal::bitset_extract_setbits_avx512(buffer.data() + offset, len, bit_buffer.data(), k_batch_size * 64, 0);
#elif defined(__AVX2__)
            cnt = roaring::internal::bitset_extract_setbits_avx2(buffer.data() + offset, len, bit_buffer.data(), k_batch_size * 64, 0);
#else
            cnt = roaring::internal::bitset_extract_setbits(buffer.data() + offset, len, bit_buffer.data(), 0);
#endif
            for (size_t i = 0; i < cnt; ++i)
            {
                UInt64 val = bit_buffer[i];
                UInt64 row;
                UInt64 col = val & 0x3f;
#if defined(__BMI2__)
                ASM_SHIFT_RIGHT(val, shift, row);
#else
                row = val >> shift;
#endif
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

    /** There are three main types of containers in Roaring Bitmap: array, bitset and run.
     * This function converts the specified indexes and values (belonging to container_id) into BSI format and
     *  saves them in the container of each bitmap of data_array of NumericIndexedVector vector.
     *  The format of container is Bitset.
     **/
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
                    /// on x64, should compile to BLSI (careful: the Intel compiler seems to fail)
                    UInt64 t = w & (~w + 1);
                    /// on x64, should compile to TZCNT
                    int i = __builtin_ctzll(w);
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
#if defined(__BMI2__)
                    ASM_SHIFT_RIGHT(p, shift, tmp_offset);
#else
                    tmp_offset = p >> shift;
#endif
                    ctn->words[tmp_offset] |= 1ULL << (p & 0x3f);
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
                /// 1/8 of the bits are filled with 1s in nonzero elements, we use Dense version for acceleration
                return toVectorCompactBitsetDense(indexes, values, length, container_id, buffer, vector);
            }
            return toVectorCompactBitset(indexes, values, length, container_id, buffer, vector);
        }
    }

    /** This function converts the specified indexes and values (belonging to container_id) into BSI format and
     *  saves them in the container of each bitmap of data_array of NumericIndexedVector vector.
     * Only convert the target container_id.
     */
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


    /** Extract index and value from the vector.
     *  The selected index belongs to the container_id and in mask bitmap.
     *  The results are sorted from small to large by index and saved in the output array.
     * Returns the number of extracted values. Since a container has a maximum of 2^16 elements,
     *  return type UInt16 is sufficient.
     */
    static UInt16 valueToColumn(
        const BSINumericIndexedVector & vector, const std::shared_ptr<Roaring> & mask, const UInt32 & container_id, Float64 * output)
    {
        PaddedPODArray<UInt64> buffer(65536);
        PaddedPODArray<UInt32> bit_buffer(65536);
        UInt16 mask_container_cardinality = mask->ra_get_container_cardinality(container_id);
        if (mask_container_cardinality == 0)
            return 0;
        memset(buffer.data(), 0, buffer.size() * sizeof(UInt64));

        const UInt32 total_bit_num = vector.getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            auto & lhs_bm = vector.getDataArrayAt(i);
            auto bit_cnt = lhs_bm->container_and_to_uint32_array(mask.get(), container_id, 0, &bit_buffer);
            for (size_t j = 0; j < bit_cnt; ++j)
            {
                if (bit_buffer[j] >= 65536)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "bit_buffer index out of bounds. bit_buffer[j]: {}", bit_buffer[j]);
                buffer[bit_buffer[j]] |= (1ULL << i);
            }
        }
        auto result_cnt = mask->container_to_uint32_array(container_id, 0, bit_buffer);
        if (vector.isValueTypeSigned() && total_bit_num < 64)
        {
            UInt64 bit_mask = ~((1ULL << total_bit_num) - 1);
            for (size_t i = 0; i < result_cnt; ++i)
            {
                if (bit_buffer[i] >= 65536)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "bit_buffer index out of bounds. bit_buffer[i]: {}", bit_buffer[i]);
                if ((buffer[bit_buffer[i]] & (1ULL << (total_bit_num - 1))) != 0)
                {
                    /// sign extend
                    buffer[bit_buffer[i]] |= bit_mask;
                }
                output[i] = static_cast<Float64>(static_cast<ValueType>(static_cast<Int64>(buffer[bit_buffer[i]])))
                    / (1ULL << vector.fraction_bit_num);
            }
        }
        else
        {
            for (size_t i = 0; i < result_cnt; ++i)
            {
                if (bit_buffer[i] >= 65536)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "bit_buffer index out of bounds. bit_buffer[i]: {}", bit_buffer[i]);
                output[i] = static_cast<Float64>(static_cast<ValueType>(static_cast<Int64>(buffer[bit_buffer[i]])))
                    / (1ULL << vector.fraction_bit_num);
            }
        }
        return result_cnt;
    }

    /** Addition and subtraction are implemented directly in the compressed domain of BSI using
     *      hardware full adders and full subtractors.
     *  The simulation of hardware multiplication and division is relatively inefficient. Here we
     *      convert NumericIndexedVector to the original vectors and then do multiplication and division.
     */
    static void pointwiseRawBinaryOperate(
        const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, const UInt32 op_code, BSINumericIndexedVector & res)
    {
        auto lhs_non_zero_indexes = lhs.getAllNonZeroIndex();
        auto rhs_non_zero_indexes = rhs.getAllNonZeroIndex();

        auto and_non_zero_indexes = std::make_shared<Roaring>();
        and_non_zero_indexes->rb_or(*lhs_non_zero_indexes);
        and_non_zero_indexes->rb_and(*rhs_non_zero_indexes);

        PaddedPODArray<UInt32> indexes(65536);
        PaddedPODArray<Float64> lhs_values(65536);
        PaddedPODArray<Float64> rhs_values(65536);
        PaddedPODArray<Float64> res_values(65536);

        std::set<UInt16> container_ids = and_non_zero_indexes->ra_get_all_container_ids();
        for (const auto & container_id : container_ids)
        {
            UInt32 indexes_size = and_non_zero_indexes->container_to_uint32_array(container_id, container_id << 16, indexes);
            if (indexes_size == 0)
                continue;
            UInt32 lhs_size = valueToColumn(lhs, and_non_zero_indexes, container_id, lhs_values.data());
            UInt32 rhs_size = valueToColumn(rhs, and_non_zero_indexes, container_id, rhs_values.data());
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
                    break;
                /// If you want to add other operations such as subtraction, please pay attention to the handling of 0.
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported op_code: {}", op_code);
            }
            toVector(indexes, res_values, indexes_size, container_id, res);
        }
        /// zero indexes;
        res.zero_indexes = std::make_shared<Roaring>();
        res.zero_indexes->rb_or(*lhs_non_zero_indexes);
        res.zero_indexes->rb_xor(*rhs_non_zero_indexes);
        res.zero_indexes->rb_or(*lhs.zero_indexes);
        res.zero_indexes->rb_or(*rhs.zero_indexes);
    }

    /** Performs pointwise multiplication and division of the original vector and a scalar.
     *  res = lhs * rhs(Example: v_res = v * 3; v_res = v / 3).
     */
    static void pointwiseRawBinaryOperate(
        const BSINumericIndexedVector & lhs, const ValueType & rhs, const UInt32 op_code, BSINumericIndexedVector & res)
    {
        if (rhs == 0)
        {
            res.zero_indexes->rb_or(*lhs.getAllIndex());
            return;
        }
        checkValidValue(rhs);

        auto lhs_non_zero_indexes = lhs.getAllNonZeroIndex();

        PaddedPODArray<UInt32> indexes(65536);
        PaddedPODArray<Float64> lhs_values(65536);
        PaddedPODArray<Float64> res_values(65536);

        std::set<UInt16> container_ids = lhs_non_zero_indexes->ra_get_all_container_ids();
        for (const auto & container_id : container_ids)
        {
            UInt32 indexes_size = lhs_non_zero_indexes->container_to_uint32_array(container_id, container_id << 16, indexes);
            if (indexes_size == 0)
                continue;
            UInt32 lhs_size = valueToColumn(lhs, lhs_non_zero_indexes, container_id, lhs_values.data());
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
                    break;
                /// If you want to add other operations such as subtraction, please pay attention to the handling of 0.
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported op_code: {}", op_code);
            }
            toVector(indexes, res_values, indexes_size, container_id, res);
        }
        res.zero_indexes->merge(*lhs.zero_indexes);
    }

    /** Performs pointwise multiplication of two original vectors.
     * The result is stored in the res.
     */
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
    /** Performs pointwise multiplication of vector and scalar.
     * The result is stored in the res.
     */
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

    /** Performs pointwise division of two original vectors.
     * The result is stored in the res.
     */
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

    /** Performs pointwise division of vector and scalar.
     * The result is stored in the res.
     */
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

    /** Performs pointwise equality comparison between two original vectors.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] == original_vector(rhs)[index].
     */
    static std::shared_ptr<Roaring> pointwiseEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        auto res_bm = lhs.getAllIndex();
        res_bm->rb_or(*rhs.zero_indexes);

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

    /** Performs pointwise equality comparison between original vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] == rhs. Treat 0 and non-existence differently.
     */
    static std::shared_ptr<Roaring> pointwiseEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs)
    {
        std::shared_ptr<Roaring> res_bm = std::make_shared<Roaring>();
        if (rhs == 0)
        {
            res_bm->rb_or(*lhs.zero_indexes);
            return res_bm;
        }
        checkValidValue(rhs);

        res_bm = lhs.getAllNonZeroIndex();

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

    static void pointwiseEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        res.getDataArrayAt(res.fraction_bit_num)->rb_or(*pointwiseEqual(lhs, rhs));
    }


    static void pointwiseEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        checkValidValue(rhs);
        res.initialize(2, 0);
        res.getDataArrayAt(res.fraction_bit_num)->rb_or(*pointwiseEqual(lhs, rhs));
    }

    /** Performs pointwise inequality comparison between two origin vectors.
     *  The returned Roaring Bitmap contains indexes that satisfy:
     *      original_vector(lhs)[index] != original_vector(rhs)[index].
     */
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

    /** Performs pointwise inequality comparison between vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy: original_vector(lhs)[index] != rhs
     */
    static void pointwiseNotEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        /// Do not need checkValidValue(rhs) as this is checked within pointwiseEqual
        pointwiseEqual(lhs, rhs, res);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);

        auto lhs_all_indexes = lhs.getAllIndex();
        lhs_all_indexes->rb_andnot(*res_bm);

        res_bm = lhs_all_indexes;
    }

    static std::shared_ptr<Roaring> pointwiseLessUnsigned(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        BSINumericIndexedVector lhs_ref;
        lhs_ref.shallowCopyFrom(lhs);
        BSINumericIndexedVector rhs_ref;
        rhs_ref.shallowCopyFrom(rhs);
        UInt32 total_bit_num = promoteBitPrecisionInplace(lhs_ref, rhs_ref);

        auto bin = std::make_shared<Roaring>();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            const auto & minuend = lhs_ref.getDataArrayAt(i);
            const auto & subtrahend = rhs_ref.getDataArrayAt(i);

            Roaring subtrahend_or_bin;
            subtrahend_or_bin.rb_or(*subtrahend);
            subtrahend_or_bin.rb_or(*bin);

            subtrahend_or_bin.rb_andnot(*minuend);

            bin->rb_and(*subtrahend);
            bin->rb_or(subtrahend_or_bin);
        }
        return bin;
    }

    static void filterWithMask(const BSINumericIndexedVector & lhs, const std::shared_ptr<Roaring> & mask, BSINumericIndexedVector & res)
    {
        res.initialize(lhs.integer_bit_num, lhs.fraction_bit_num);
        for (size_t i = 0; i < lhs.getTotalBitNum(); ++i)
        {
            res.getDataArrayAt(i)->rb_or(*lhs.getDataArrayAt(i));
            res.getDataArrayAt(i)->rb_and(*mask);
        }
        res.zero_indexes->merge(*lhs.zero_indexes);
        res.zero_indexes->rb_and(*mask);
    }

    static std::shared_ptr<Roaring> numericIndexedVectorAnd(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        auto res_bm = std::make_shared<Roaring>();
        res_bm->rb_or(*lhs.getAllIndex());
        res_bm->rb_and(*rhs.getAllIndex());
        return res_bm;
    }

    /** Performs pointwise less comparison between two origin vectors.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] < original_vector(rhs)[index]
     */
    static std::shared_ptr<Roaring> pointwiseLess(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs)
    {
        if (!lhs.isValueTypeSigned() && !rhs.isValueTypeSigned())
        {
            return pointwiseLessUnsigned(lhs, rhs);
        }
        else if (lhs.isValueTypeSigned() && rhs.isValueTypeSigned())
        {
            std::shared_ptr<Roaring> lhs_negative_indexes = std::make_shared<Roaring>();
            if (lhs.getTotalBitNum() > 0)
            {
                lhs_negative_indexes->rb_or(*lhs.getDataArrayAt(lhs.getTotalBitNum() - 1));
            }
            auto lhs_positive_indexes = std::make_shared<Roaring>();
            lhs_positive_indexes->rb_or(*lhs.getAllNonZeroIndex());
            lhs_positive_indexes->rb_andnot(*lhs_negative_indexes);
            BSINumericIndexedVector lhs_positive_vector;
            filterWithMask(lhs, lhs_positive_indexes, lhs_positive_vector);
            BSINumericIndexedVector lhs_negative_vector;
            filterWithMask(lhs, lhs_negative_indexes, lhs_negative_vector);

            std::shared_ptr<Roaring> rhs_negative_indexes = std::make_shared<Roaring>();
            if (rhs.getTotalBitNum() > 0)
            {
                rhs_negative_indexes->rb_or(*rhs.getDataArrayAt(rhs.getTotalBitNum() - 1));
            }
            auto rhs_positive_indexes = std::make_shared<Roaring>();
            rhs_positive_indexes->rb_or(*rhs.getAllNonZeroIndex());
            rhs_positive_indexes->rb_andnot(*rhs_negative_indexes);
            BSINumericIndexedVector rhs_positive_vector;
            filterWithMask(rhs, rhs_positive_indexes, rhs_positive_vector);
            BSINumericIndexedVector rhs_negative_vector;
            filterWithMask(rhs, rhs_negative_indexes, rhs_negative_vector);

            /// (lhs_zero_indexes | lhs_negative_indexes) & rhs_positive_indexes
            auto bm1 = std::make_shared<Roaring>();
            bm1->merge(*lhs.zero_indexes);
            bm1->merge(*lhs_negative_indexes);
            bm1->rb_and(*rhs_positive_indexes);
            /// lhs_negative_indexes & rhs_zero_indexes
            auto bm2 = std::make_shared<Roaring>();
            bm2->merge(*lhs_negative_indexes);
            bm2->rb_and(*rhs.zero_indexes);
            /// lhs_positive_vector less than rhs_positive_vector;
            auto bm3 = pointwiseLessUnsigned(lhs_positive_vector, rhs_positive_vector);
            /// lhs_negative_vector less than rhs_negative_vector;


            auto and_negative_indexes = lhs_negative_vector.getAllNonZeroIndex();
            and_negative_indexes->rb_and(*rhs_negative_vector.getAllNonZeroIndex());
            BSINumericIndexedVector filter_lhs_negative_vector;
            filterWithMask(lhs_negative_vector, and_negative_indexes, filter_lhs_negative_vector);
            BSINumericIndexedVector filter_rhs_negative_vector;
            filterWithMask(rhs_negative_vector, and_negative_indexes, filter_rhs_negative_vector);
            auto bm4 = pointwiseLessUnsigned(filter_lhs_negative_vector, filter_rhs_negative_vector);
            auto res = std::make_shared<Roaring>();
            res->rb_or(*bm1);
            res->rb_or(*bm2);
            res->rb_or(*bm3);
            res->rb_or(*bm4);
            PaddedPODArray<IndexType> res_array;
            res->rb_to_array(res_array);
            return res;
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "lhs and rhs isValueTypeSigned should be same");
        }
    }

    static std::shared_ptr<Roaring> pointwiseLess(const BSINumericIndexedVector & lhs, const ValueType & rhs)
    {
        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        return pointwiseLess(lhs, rhs_vec);
    }

    static void pointwiseLess(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(lhs, rhs);
    }

    /** Performs pointwise less comparison between vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] < rhs
     */
    static void pointwiseLess(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(lhs, rhs);
    }


    /** Performs pointwise less than or equal comparison between two origin vectors.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] <= original_vector(rhs)[index]
     */
    static void pointwiseLessEqual(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        auto lt_bm = pointwiseLess(lhs, rhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*lt_bm);
        res_bm->rb_or(*eq_bm);
    }

    /** Performs pointwise less than or equal comparison between vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] <= rhs
     */
    static void pointwiseLessEqual(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        /// Do not need checkValidValue(rhs) as this is checked within pointwiseLess
        auto lt_bm = pointwiseLess(lhs, rhs);
        auto eq_bm = pointwiseEqual(lhs, rhs);

        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm->rb_or(*lt_bm);
        res_bm->rb_or(*eq_bm);
    }

    /** Performs pointwise greater than comparison between two origin vectors.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] > original_vector(rhs)[index]
     */
    static void pointwiseGreater(const BSINumericIndexedVector & lhs, const BSINumericIndexedVector & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);
        res_bm = pointwiseLess(rhs, lhs);
    }

    /** Performs pointwise greater than comparison between vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] > rhs
     */
    static void pointwiseGreater(const BSINumericIndexedVector & lhs, const ValueType & rhs, BSINumericIndexedVector & res)
    {
        res.initialize(2, 0);
        auto & res_bm = res.getDataArrayAt(res.fraction_bit_num);

        BSINumericIndexedVector rhs_vec;
        rhs_vec.initializeFromVectorAndValue(lhs, rhs);
        res_bm = pointwiseLess(rhs_vec, lhs);
    }

    /** Performs pointwise greater than or equal comparison between two origin vectors.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] >= original_vector(rhs)[index]
     */
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

    /** Performs pointwise greater than or equal comparison between vector and a scalar value.
     * The returned Roaring Bitmap contains indexes that satisfy:
     *  original_vector(lhs)[index] >= rhs
     */
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

    /// original_vector(this)[index] += value.
    void addValue(IndexType index, ValueType value)
    {
        checkValidValue(value);

        if (sizeof(IndexType) > 4)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IndexType must be at most 32 bits in BSI format");
        }

        if (value == 0)
        {
            zero_indexes->add(index);
            return;
        }

        const UInt32 total_bit_num = getTotalBitNum();

        UInt32 ele = static_cast<UInt32>(index);

        /** This converts a floating-point value into a fixed-point representation, then store it in data_array using bit-sliced index.
          * - When value is an UInt/Int, fraction_bit_num is usually set to 0. So when integer_bit_num is set to the number of
          *   storage bits of Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64(set integer_bit_num = 8 when value type is UInt8/Int8
          *   and set integer_bit_num = 64 when value type is UInt64/Int64 etc.), the expression of numericIndexedVector is not
          *   limited and overflow will not occur.
          * - When value is a Float32/Float64, fraction_bit_num indicates how many bits are used to represent the decimal, Because the
          *   maximum value of total_bit_num(integer_bit_num + fraction_bit_num) is 64, overflow may occur.
          */
        Int64 scaled_value = Int64(value * (1L << fraction_bit_num));

        UInt8 cin = 0;
        for (size_t j = 0; j < total_bit_num; ++j)
        {
            UInt8 augend = getDataArrayAt(j)->rb_contains(ele) ? 1 : 0;
            UInt8 addend = (scaled_value & (1LL << j)) != 0 ? 1 : 0;

            UInt8 x_xor_y = augend ^ addend;
            UInt8 x_and_y = augend & addend;

            UInt8 sum = augend ^ addend ^ cin;

            if ((sum & 1) == 1)
            {
                getDataArrayAt(j)->add(ele);
            }

            cin = cin & x_xor_y;
            cin = cin | x_and_y;
        }
    }

    /// return origin_vector(this)[index]
    ValueType getValue(IndexType index) const
    {
        if (zero_indexes->rb_contains(index))
            return 0;

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

    /// sum(origin_vector(this))
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

            /// Handle positive indexes
            for (size_t i = 0; i < total_bit_num - 1; ++i)
            {
                Float64 bit_contribution = std::pow(2.0, int(i) - int(fraction_bit_num));

                Roaring positive_indexes;
                positive_indexes.rb_or(*getDataArrayAt(i));

                positive_indexes.rb_andnot(negative_indexes);
                value += positive_indexes.size() * bit_contribution;
            }

            /// Handle negative indexes
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

    /// return the number of unique indexes in the vector whose value is exist
    UInt64 getCardinality() const
    {
        Roaring total_bm;
        const UInt32 total_bit_num = getTotalBitNum();
        for (size_t i = 0; i < total_bit_num; ++i)
        {
            total_bm.rb_or(*getDataArrayAt(i));
        }
        total_bm.rb_or(*zero_indexes);
        return total_bm.size();
    }


    /** Converts the internal bit-level representation into an index-to-value mapping.
     * The mapping is then stored into the provided `indexes_pod` and `values_pod` arrays.
     *
     * @param indexes_pod    [out] Array that will be filled with the indexes.
     * @param values_pod     [out] Array that will be filled with the values.
     * @return               The number of unique indexes in the mapping.
     */
    UInt64 toIndexValueMap(PaddedPODArray<IndexType> & indexes_pod, PaddedPODArray<ValueType> & values_pod) const
    {
        const UInt32 total_bit_num = getTotalBitNum();
        if (total_bit_num == 0)
            return 0;
        DataTypePtr value_type = std::make_shared<DataTypeNumber<ValueType>>();
        auto which = WhichDataType(value_type);
        if ((which.isUInt() or which.isInt()) and fraction_bit_num > 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "fraction_bit_num should be zero when value type is Int/UInt");

        std::map<IndexType, UInt64> index2value;

        PaddedPODArray<IndexType> zero_indexes_array;
        zero_indexes->rb_to_array(zero_indexes_array);

        for (size_t i = 0; i < zero_indexes_array.size(); ++i)
        {
            index2value[zero_indexes_array[i]] = 0;
        }

        for (size_t i = 0; i < total_bit_num; ++i)
        {
            PaddedPODArray<IndexType> indexes;
            getDataArrayAt(i)->rb_to_array(indexes);
            for (size_t j = 0; j < indexes.size(); ++j)
            {
                index2value[indexes[j]] |= (1ULL << i);
            }
        }
        if (which.isUInt())
        {
            for (auto & [index, value] : index2value)
            {
                indexes_pod.emplace_back(index);
                values_pod.emplace_back(static_cast<ValueType>(value));
            }
        }
        else if (which.isInt() || which.isFloat())
        {
            Roaring neg_idx_bm;
            if (total_bit_num > 0)
            {
                neg_idx_bm.rb_or(*getDataArrayAt(total_bit_num - 1));
            }
            PaddedPODArray<IndexType> neg_idx_array;
            neg_idx_bm.rb_to_array(neg_idx_array);
            for (size_t i = total_bit_num; i < 64; ++i)
            {
                for (auto index : neg_idx_array)
                {
                    index2value[index] |= (1ULL << i);
                }
            }
            for (auto & [index, value] : index2value)
            {
                indexes_pod.emplace_back(index);
                if (fraction_bit_num == 0)
                {
                    values_pod.emplace_back(static_cast<ValueType>(static_cast<Int64>(value)));
                }
                else
                {
                    values_pod.emplace_back(
                        static_cast<ValueType>(static_cast<Int64>(value) / static_cast<Float64>(1ULL << fraction_bit_num)));
                }
            }
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported value type for toIndexValueMap()");
        }
        return index2value.size();
    }

    void read(DB::ReadBuffer & in)
    {
        readBinary(integer_bit_num, in);
        readBinary(fraction_bit_num, in);

        zero_indexes = std::make_shared<Roaring>();
        UInt8 is_zero_indexes_empty = 0;
        readBinary(is_zero_indexes_empty, in);
        if (is_zero_indexes_empty == 0)
        {
            zero_indexes->read(in);
        }

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

        UInt8 is_zero_indexes_empty = 0;
        if (zero_indexes->size() == 0)
            is_zero_indexes_empty = 1;
        writeBinary(is_zero_indexes_empty, out);
        if (is_zero_indexes_empty == 0)
            zero_indexes->write(out);

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

}
