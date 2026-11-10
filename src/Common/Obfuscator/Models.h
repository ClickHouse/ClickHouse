#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeUUID.h>
#include <Common/SipHash.h>
#include <Common/UTF8Helpers.h>
#include <Common/StringUtils.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Common/DateLUT.h>
#include <base/arithmeticOverflow.h>
#include <base/extended_types.h>
#include <limits>
#include <memory>
#include <optional>
#include <cmath>
#include <boost/container/flat_map.hpp>
#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

/// Model is used to transform columns with source data to columns
///  with similar by structure and by probability distributions but anonymized data.
class IModel
{
public:
    /// Call train iteratively for each block to train a model.
    virtual void train(const IColumn & column) = 0;

    /// Call finalize one time after training before generating.
    virtual void finalize() = 0;

    /// Call generate: pass source data column to obtain a column with anonymized data as a result.
    virtual ColumnPtr generate(const IColumn & column) = 0;

    /// Deterministically change seed to some other value. This can be used to generate more values than were in source.
    virtual void updateSeed() = 0;

    /// Save into file. Binary, platform-dependent, version-dependent serialization.
    virtual void serialize(WriteBuffer & out) const = 0;

    /// Read from file
    virtual void deserialize(ReadBuffer & in) = 0;

    virtual ~IModel() = default;
};

using ModelPtr = std::unique_ptr<IModel>;


template <typename... Ts>
static UInt64 hash(Ts... xs)
{
    SipHash hash;
    (hash.update(xs), ...);
    return hash.get64();
}


static UInt64 maskBits(UInt64 x, size_t num_bits)
{
    return x & ((1ULL << num_bits) - 1);
}


/// Apply Feistel network round to least significant num_bits part of x.
static UInt64 feistelRound(UInt64 x, size_t num_bits, UInt64 seed, size_t round)
{
    size_t num_bits_left_half = num_bits / 2;
    size_t num_bits_right_half = num_bits - num_bits_left_half;

    UInt64 left_half = maskBits(x >> num_bits_right_half, num_bits_left_half);
    UInt64 right_half = maskBits(x, num_bits_right_half);

    UInt64 new_left_half = right_half;
    UInt64 new_right_half = left_half ^ maskBits(hash(right_half, seed, round), num_bits_left_half);

    return (new_left_half << num_bits_left_half) ^ new_right_half;
}


/// Apply Feistel network with num_rounds to least significant num_bits part of x.
static UInt64 feistelNetwork(UInt64 x, size_t num_bits, UInt64 seed, size_t num_rounds = 4)
{
    UInt64 bits = maskBits(x, num_bits);
    for (size_t i = 0; i < num_rounds; ++i)
        bits = feistelRound(bits, num_bits, seed, i);
    return (x & ~((1ULL << num_bits) - 1)) ^ bits;
}


/// Pseudorandom permutation within set of numbers with the same log2(x).
static UInt64 transform(UInt64 x, UInt64 seed)
{
    /// Keep 0 and 1 as is.
    if (x == 0 || x == 1)
        return x;

    /// Pseudorandom permutation of two elements.
    if (x == 2 || x == 3)
        return x ^ (seed & 1);

    size_t num_leading_zeros = std::countl_zero(x);

    return feistelNetwork(x, 64 - num_leading_zeros - 1, seed);
}


class UnsignedIntegerModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit UnsignedIntegerModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        MutableColumnPtr res = column.cloneEmpty();

        size_t size = column.size();
        res->reserve(size);

        for (size_t i = 0; i < size; ++i)
            res->insert(transform(column.getUInt(i), seed));

        return res;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Keep sign and apply pseudorandom permutation after converting to unsigned as above.
static Int64 transformSigned(Int64 x, UInt64 seed)
{
    if (x >= 0)
        return transform(x, seed);

    /// The minimum signed number is the only value in its log2 bucket whose magnitude
    /// is representable: permuting it within the bucket and negating would wrap the result
    /// into a positive number, breaking the "keep sign" invariant. Keep it as is.
    if (x == std::numeric_limits<Int64>::min())
        return x;

    /// Negate using unsigned arithmetic to obtain the magnitude;
    /// `-x` on the signed integer would be undefined behavior on the boundary value.
    return -transform(-static_cast<UInt64>(x), seed);
}


/// The same as `maskBits`, `feistelRound`, `feistelNetwork` and `transform`, but for the wide unsigned
/// integers `UInt128` and `UInt256`. The round function produces as many pseudorandom bits as the
/// half it is applied to, so that every bit of a wide value takes part in the permutation.
template <typename UInt>
static UInt maskBitsWide(UInt x, size_t num_bits)
{
    return x & ((UInt(1) << num_bits) - 1);
}

template <typename UInt>
static UInt hashWide(UInt x, UInt64 seed, size_t round)
{
    UInt res = 0;
    for (size_t i = 0; i < sizeof(UInt) / sizeof(UInt64); ++i)
        res |= UInt(hash(x, seed, round, i)) << (64 * i);
    return res;
}

template <typename UInt>
static UInt feistelRoundWide(UInt x, size_t num_bits, UInt64 seed, size_t round)
{
    size_t num_bits_left_half = num_bits / 2;
    size_t num_bits_right_half = num_bits - num_bits_left_half;

    UInt left_half = maskBitsWide(x >> num_bits_right_half, num_bits_left_half);
    UInt right_half = maskBitsWide(x, num_bits_right_half);

    UInt new_left_half = right_half;
    UInt new_right_half = left_half ^ maskBitsWide(hashWide(right_half, seed, round), num_bits_left_half);

    return (new_left_half << num_bits_left_half) ^ new_right_half;
}

template <typename UInt>
static UInt transformWide(UInt x, UInt64 seed)
{
    if (x == 0 || x == 1)
        return x;

    if (x == 2 || x == 3)
        return x ^ UInt(seed & 1);

    /// The number of significant bits of `x`.
    size_t num_bits = 0;
    for (size_t i = sizeof(UInt) / sizeof(UInt64); i > 0; --i)
    {
        UInt64 limb = static_cast<UInt64>(x >> (64 * (i - 1)));
        if (limb)
        {
            num_bits = 64 * i - std::countl_zero(limb);
            break;
        }
    }

    /// Keep the most significant bit and permute the bits below it.
    size_t num_permuted_bits = num_bits - 1;
    UInt bits = maskBitsWide(x, num_permuted_bits);
    for (size_t round = 0; round < 4; ++round)
        bits = feistelRoundWide(bits, num_permuted_bits, seed, round);
    return (UInt(1) << num_permuted_bits) | bits;
}


/// Pseudorandom permutation of an integer of any width inside the set of numbers with the same log2 of
/// the absolute value, keeping the sign. The minimum value of a signed type is kept as is, because its
/// magnitude is not representable as a positive value of the same width (see `SignedIntegerModel`).
template <typename T>
static T transformInteger(T x, UInt64 seed)
{
    if constexpr (is_unsigned_v<T>)
    {
        if constexpr (sizeof(T) <= sizeof(UInt64))
            return static_cast<T>(transform(x, seed));
        else
            return transformWide(x, seed);
    }
    else
    {
        if (x == std::numeric_limits<T>::min())
            return x;

        if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            return static_cast<T>(transformSigned(x, seed));
        }
        else
        {
            using UInt = make_unsigned_t<T>;
            if (x >= 0)
                return static_cast<T>(transformWide(static_cast<UInt>(x), seed));
            return -static_cast<T>(transformWide(static_cast<UInt>(-x), seed));
        }
    }
}


class SignedIntegerModel : public IModel
{
private:
    UInt64 seed;

    /// The minimum representable value of the source integer type. Its magnitude is not representable
    /// as a positive value of the same width, so permuting it within its log2 bucket and negating would
    /// wrap the result into a positive number. Keep it unchanged to preserve the "keep sign" invariant
    /// for narrow types as well, e.g. `Int8`'s `-128`.
    Int64 min_value;

public:
    SignedIntegerModel(UInt64 seed_, Int64 min_value_) : seed(seed_), min_value(min_value_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        MutableColumnPtr res = column.cloneEmpty();

        size_t size = column.size();
        res->reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            Int64 value = column.getInt(i);
            res->insert(value == min_value ? value : transformSigned(value, seed));
        }

        return res;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Pseudorandom permutation of mantissa.
template <typename Float>
static Float transformFloatMantissa(Float x, UInt64 seed)
{
    using UInt = std::conditional_t<std::is_same_v<Float, BFloat16>, UInt16, std::conditional_t<std::is_same_v<Float, Float32>, UInt32, UInt64>>;
    constexpr size_t mantissa_num_bits = std::is_same_v<Float, BFloat16> ? 7 : (std::is_same_v<Float, Float32> ? 23 : 52);

    UInt x_uint = std::bit_cast<UInt>(x);
    x_uint = static_cast<UInt>(feistelNetwork(x_uint, mantissa_num_bits, seed));
    return std::bit_cast<Float>(x_uint);
}


/// Transform difference from previous number by applying pseudorandom permutation to mantissa part of it.
/// It allows to retain some continuity property of source data.
template <typename Float>
class FloatModel : public IModel
{
private:
    UInt64 seed;
    Float src_prev_value{};
    Float res_prev_value{};

public:
    explicit FloatModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnVector<Float> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<Float>::create(size);
        auto & res_data = assert_cast<ColumnVector<Float> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            res_data[i] = res_prev_value + transformFloatMantissa(src_data[i] - src_prev_value, seed);
            src_prev_value = src_data[i];
            res_prev_value = res_data[i];
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Leave all data as is. For example, it is used for columns of type Date.
class IdentityModel : public IModel
{
public:
    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        return column.cloneResized(column.size());
    }

    void updateSeed() override
    {
    }
};


/// Pseudorandom function, but keep word characters as word characters.
static void transformFixedString(const UInt8 * src, UInt8 * dst, size_t size, UInt64 seed)
{
    {
        SipHash hash;
        hash.update(seed);
        hash.update(reinterpret_cast<const char *>(src), size);
        seed = hash.get64();
    }

    UInt8 * pos = dst;
    UInt8 * end = dst + size;

    size_t i = 0;
    while (pos < end)
    {
        SipHash hash;
        hash.update(seed);
        hash.update(i);

        const auto checksum = getSipHash128AsArray(hash);
        if (size >= 16)
        {
            auto * hash_dst = std::min(pos, end - 16);
            memcpy(hash_dst, checksum.data(), checksum.size());
        }
        else
            memcpy(dst, checksum.data(), end - dst);

        pos += 16;
        ++i;
    }

    for (size_t j = 0; j < size; ++j)
    {
        if (isWordCharASCII(src[j]))
        {
            static constexpr char word_chars[] = "_01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            dst[j] = word_chars[dst[j] % (sizeof(word_chars) - 1)];
        }
    }
}

static void transformUUID(const UUID & src_uuid, UUID & dst_uuid, UInt64 seed)
{
    auto src_copy = src_uuid;
    transformEndianness<std::endian::little, std::endian::native>(src_copy);

    const UInt128 & src = src_copy.toUnderType();
    UInt128 & dst = dst_uuid.toUnderType();

    SipHash hash;
    hash.update(seed);
    hash.update(reinterpret_cast<const char *>(&src), sizeof(UUID));

    /// Saving version and variant from an old UUID
    dst = hash.get128();

    const UInt64 trace[2] = {0x000000000000f000ull, 0xe000000000000000ull};
    UUIDHelpers::getLowBytes(dst_uuid) = (UUIDHelpers::getLowBytes(dst_uuid) & (0xffffffffffffffffull - trace[1])) | (UUIDHelpers::getLowBytes(src_uuid) & trace[1]);
    UUIDHelpers::getHighBytes(dst_uuid) = (UUIDHelpers::getHighBytes(dst_uuid) & (0xffffffffffffffffull - trace[0])) | (UUIDHelpers::getHighBytes(src_uuid) & trace[0]);
}

class FixedStringModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit FixedStringModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnFixedString & column_fixed_string = assert_cast<const ColumnFixedString &>(column);
        const size_t string_size = column_fixed_string.getN();

        const auto & src_data = column_fixed_string.getChars();
        size_t size = column_fixed_string.size();

        auto res_column = ColumnFixedString::create(string_size);
        auto & res_data = res_column->getChars();

        res_data.resize(src_data.size());

        for (size_t i = 0; i < size; ++i)
            transformFixedString(&src_data[i * string_size], &res_data[i * string_size], string_size, seed);

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};

class UUIDModel : public IModel
{
private:
    UInt64 seed;

public:
    explicit UUIDModel(UInt64 seed_) : seed(seed_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnUUID & src_column = assert_cast<const ColumnUUID &>(column);
        const auto & src_data = src_column.getData();

        auto res_column = ColumnUUID::create();
        auto & res_data = res_column->getData();

        res_data.resize(src_data.size());
        for (size_t i = 0; i < src_column.size(); ++i)
            transformUUID(src_data[i], res_data[i], seed);

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Leave date part as is and apply pseudorandom permutation to time difference with previous value within the same log2 class.
class DateTimeModel : public IModel
{
private:
    UInt64 seed;
    UInt32 src_prev_value = 0;
    UInt32 res_prev_value = 0;

    const DateLUTImpl & date_lut;

public:
    explicit DateTimeModel(UInt64 seed_, const DateLUTImpl & date_lut_) : seed(seed_), date_lut(date_lut_) {}

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnVector<UInt32> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<UInt32>::create(size);
        auto & res_data = assert_cast<ColumnVector<UInt32> &>(*res_column).getData();

        for (size_t i = 0; i < size; ++i)
        {
            UInt32 src_datetime = src_data[i];

            const Int64 src_diff = static_cast<Int64>(src_datetime) - src_prev_value;
            const Int64 res_diff = transformSigned(src_diff, seed);
            const Int64 new_datetime = static_cast<Int64>(res_prev_value) + res_diff;

            /// Keep the obfuscated time of day, but re-attach it to the source date as displayed in the
            /// column's timezone. Constructing the result from the source date plus the local wall-clock
            /// time of `new_datetime` keeps it inside the source local day even across DST transitions,
            /// where day lengths differ from 24 hours and a raw "seconds since midnight" offset added to
            /// the source midnight could otherwise spill onto a neighbouring displayed date.
            res_data[i] = static_cast<UInt32>(date_lut.makeDateTime(
                date_lut.toYear(src_datetime),
                date_lut.toMonth(src_datetime),
                date_lut.toDayOfMonth(src_datetime),
                static_cast<UInt8>(date_lut.toHour(new_datetime)),
                static_cast<UInt8>(date_lut.toMinute(new_datetime)),
                static_cast<UInt8>(date_lut.toSecond(new_datetime))));

            src_prev_value = src_datetime;
            res_prev_value = res_data[i];
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// The same as `DateTimeModel` for the whole seconds of `DateTime64`: keep the date part and obfuscate the
/// time of day. The sub-second part is permuted inside its log2 bucket, restricted to the valid range
/// [0, 10^scale) by cycle walking: the permutation is reapplied until the value falls inside the range,
/// which terminates because the starting value lies in the range and belongs to the same permutation cycle.
class DateTime64Model : public IModel
{
private:
    UInt64 seed;
    Int64 scale_multiplier;
    Int64 src_prev_value = 0;
    Int64 res_prev_value = 0;

    const DateLUTImpl & date_lut;

public:
    DateTime64Model(UInt64 seed_, Int64 scale_multiplier_, const DateLUTImpl & date_lut_)
        : seed(seed_), scale_multiplier(scale_multiplier_), date_lut(date_lut_)
    {
    }

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnDateTime64 &>(column).getData();
        size_t size = src_data.size();

        auto res_column = column.cloneEmpty();
        auto & res_data = assert_cast<ColumnDateTime64 &>(*res_column).getData();
        res_data.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            Int64 src_value = src_data[i].value;
            Int64 src_whole = src_value / scale_multiplier;
            Int64 src_fraction = src_value % scale_multiplier;
            if (src_fraction < 0)
            {
                src_fraction += scale_multiplier;
                --src_whole;
            }

            /// Wrapping arithmetic: the values of arbitrary decimals may be far outside the range of the
            /// date lookup table, and the result is only required to be deterministic.
            const Int64 src_diff = static_cast<Int64>(static_cast<UInt64>(src_whole) - static_cast<UInt64>(src_prev_value));
            const Int64 res_diff = transformSigned(src_diff, seed);
            const Int64 new_datetime = static_cast<Int64>(static_cast<UInt64>(res_prev_value) + static_cast<UInt64>(res_diff));

            const auto year = date_lut.toYear(src_whole);
            const auto month = date_lut.toMonth(src_whole);
            const auto day_of_month = date_lut.toDayOfMonth(src_whole);

            Int64 res_whole = date_lut.makeDateTime(
                year,
                month,
                day_of_month,
                static_cast<UInt8>(date_lut.toHour(new_datetime)),
                static_cast<UInt8>(date_lut.toMinute(new_datetime)),
                static_cast<UInt8>(date_lut.toSecond(new_datetime)));

            UInt64 res_fraction = static_cast<UInt64>(src_fraction);
            do
                res_fraction = transform(res_fraction, seed);
            while (res_fraction >= static_cast<UInt64>(scale_multiplier));

            Int128 res_value = Int128(res_whole) * scale_multiplier + Int128(res_fraction);

            /// At the ends of the range of `Int64`, the source local day can be representable only in part,
            /// e.g. `2262-04-12` in `DateTime64(9, 'Pacific/Kiritimati')`, and the obfuscated time of day can
            /// fall outside of this part. Fold the value into the representable part of the source day.
            /// The range is extended to contain the source value, so it is never empty, even for a value
            /// outside of the date lookup table.
            if (res_value < std::numeric_limits<Int64>::min() || res_value > std::numeric_limits<Int64>::max())
            {
                Int128 day_begin = Int128(date_lut.makeDateTime(year, month, day_of_month, 0, 0, 0)) * scale_multiplier;
                Int128 day_end = (Int128(date_lut.makeDateTime(year, month, day_of_month, 23, 59, 59)) + 1) * scale_multiplier - 1;

                Int128 range_begin = std::min<Int128>(std::max<Int128>(day_begin, std::numeric_limits<Int64>::min()), src_value);
                Int128 range_end = std::max<Int128>(std::min<Int128>(day_end, std::numeric_limits<Int64>::max()), src_value);
                Int128 range_size = range_end - range_begin + 1;

                Int128 offset = (res_value - range_begin) % range_size;
                if (offset < 0)
                    offset += range_size;
                res_value = range_begin + offset;
            }

            res_data[i] = static_cast<Int64>(res_value);

            src_prev_value = src_whole;
            res_prev_value = res_whole;
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Apply the permutation of `transformInteger` to the underlying integer representation of the values.
/// It is used for the wide integers, `Decimal`, `IPv4` and `IPv6`.
///
/// A `Decimal` must not exceed its precision, while a log2 bucket can span beyond it, e.g. [2^29, 2^30)
/// for `Decimal(9, S)`. If `max_abs_value` is set, the permutation is restricted to the values not
/// exceeding it in magnitude by cycle walking (as in `DateTime64Model`), applied to the source values
/// that are in this range.
template <typename ColumnType, typename Integer>
class UnderlyingIntegerModel : public IModel
{
private:
    UInt64 seed;
    std::optional<Integer> max_abs_value;

    bool isInRange(Integer x) const
    {
        if constexpr (is_unsigned_v<Integer>)
            return x <= *max_abs_value;
        else
            return x <= *max_abs_value && x >= -*max_abs_value;
    }

    Integer transformValue(Integer x) const
    {
        Integer res = transformInteger(x, seed);
        if (max_abs_value && isInRange(x))
            while (!isInRange(res))
                res = transformInteger(res, seed);
        return res;
    }

public:
    explicit UnderlyingIntegerModel(UInt64 seed_, std::optional<Integer> max_abs_value_ = {})
        : seed(seed_), max_abs_value(max_abs_value_)
    {
    }

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnType &>(column).getData();
        size_t size = src_data.size();

        /// `cloneEmpty` keeps the scale of a `Decimal` column.
        auto res_column = column.cloneEmpty();
        auto & res_data = assert_cast<ColumnType &>(*res_column).getData();
        res_data.resize(size);

        using Value = typename std::decay_t<decltype(src_data)>::value_type;
        static_assert(sizeof(Value) == sizeof(Integer));

        for (size_t i = 0; i < size; ++i)
            res_data[i] = std::bit_cast<Value>(transformValue(std::bit_cast<Integer>(src_data[i])));

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }
};


/// Pseudorandom permutation of the set of the values of an `Enum` type, so that every value is replaced
/// by another (or the same) valid value, and the distribution of the values is kept up to the labels.
template <typename T>
class EnumModel : public IModel
{
private:
    UInt64 seed;

    /// The values of the type, sorted, and the image of each of them under the permutation.
    std::vector<T> values;
    std::vector<T> permuted_values;

    void updatePermutation()
    {
        permuted_values = values;
        for (size_t i = permuted_values.size(); i > 1; --i)
            std::swap(permuted_values[i - 1], permuted_values[hash(seed, i) % i]);
    }

public:
    EnumModel(UInt64 seed_, const typename EnumValues<T>::Values & enum_values) : seed(seed_)
    {
        values.reserve(enum_values.size());
        for (const auto & [name, value] : enum_values)
            values.push_back(value);
        std::ranges::sort(values);
        updatePermutation();
    }

    void train(const IColumn &) override {}
    void finalize() override {}
    void serialize(WriteBuffer &) const override {}
    void deserialize(ReadBuffer &) override {}

    ColumnPtr generate(const IColumn & column) override
    {
        const auto & src_data = assert_cast<const ColumnVector<T> &>(column).getData();
        size_t size = src_data.size();

        auto res_column = ColumnVector<T>::create(size);
        auto & res_data = res_column->getData();

        for (size_t i = 0; i < size; ++i)
        {
            /// A column can hold a value outside of the type, e.g. the default zero. Keep it as is.
            auto it = std::ranges::lower_bound(values, src_data[i]);
            if (it != values.end() && *it == src_data[i])
                res_data[i] = permuted_values[it - values.begin()];
            else
                res_data[i] = src_data[i];
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
        updatePermutation();
    }
};


struct MarkovModelParameters
{
    size_t order;
    size_t frequency_cutoff;
    size_t num_buckets_cutoff;
    size_t frequency_add;
    double frequency_desaturate;
    size_t determinator_sliding_window_size;

    void serialize(WriteBuffer & out) const
    {
        writeBinary(order, out);
        writeBinary(frequency_cutoff, out);
        writeBinary(num_buckets_cutoff, out);
        writeBinary(frequency_add, out);
        writeBinary(frequency_desaturate, out);
        writeBinary(determinator_sliding_window_size, out);
    }

    void deserialize(ReadBuffer & in)
    {
        readBinary(order, in);
        readBinary(frequency_cutoff, in);
        readBinary(num_buckets_cutoff, in);
        readBinary(frequency_add, in);
        readBinary(frequency_desaturate, in);
        readBinary(determinator_sliding_window_size, in);
    }

    /// Validates the user-provided parameters. Shared between the `obfuscate` table function and the
    /// standalone `clickhouse obfuscator` tool so both surfaces enforce the same contract.
    /// `order_name` and `frequency_desaturate_name` are the user-facing names of the corresponding
    /// parameters (a setting name for the table function, a command-line option for the tool).
    void validate(std::string_view order_name, std::string_view frequency_desaturate_name) const
    {
        if (order == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must be greater than zero", order_name);

        /// The Markov model preallocates a vector of `order` code points and performs work proportional to
        /// `order` for every code point it consumes and generates, so an unbounded value would either exhaust
        /// memory or throw `std::length_error` from the allocation. Reject absurd values with a clear error.
        static constexpr size_t max_order = 1000;
        if (order > max_order)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must not exceed {}, got {}", order_name, max_order, order);

        /// NaN compares false with both bounds and would also silently skip the `> 0` application
        /// branch, so non-finite values have to be rejected explicitly to enforce the [0, 1] contract.
        if (!std::isfinite(frequency_desaturate) || frequency_desaturate < 0.0 || frequency_desaturate > 1.0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must be in the range [0, 1]", frequency_desaturate_name);
    }
};


/** Actually it's not an order-N model, but a mix of order-{0..N} models.
  *
  * We calculate code point counts for every context of 0..N previous code points.
  * Then throw off some context with low amount of statistics.
  *
  * When generating data, we try to find statistics for a context of maximum order.
  * And if not found - use context of smaller order, up to 0.
  */
class MarkovModel
{
private:
    using CodePoint = UInt32;
    using NGramHash = UInt32;

    struct Histogram
    {
        UInt64 total = 0;   /// Not including count_end.
        UInt64 count_end = 0;
        using Buckets = boost::container::flat_map<CodePoint, UInt64>;
        Buckets buckets;

        void add(CodePoint code)
        {
            ++total;
            ++buckets[code];
        }

        void addEnd()
        {
            ++count_end;
        }

        CodePoint sample(UInt64 random, double end_multiplier) const
        {
            UInt64 range = total + static_cast<UInt64>(static_cast<double>(count_end) * end_multiplier);
            if (range == 0)
                return END;

            random %= range;

            UInt64 sum = 0;
            for (const auto & elem : buckets)
            {
                sum += elem.second;
                if (sum > random)
                    return elem.first;
            }

            return END;
        }

        void serialize(WriteBuffer & out) const
        {
            writeBinary(total, out);
            writeBinary(count_end, out);

            size_t size = buckets.size();
            writeBinary(size, out);

            for (const auto & elem : buckets)
            {
                writeBinary(elem.first, out);
                writeBinary(elem.second, out);
            }
        }

        void deserialize(ReadBuffer & in)
        {
            readBinary(total, in);
            readBinary(count_end, in);

            size_t size = 0;
            readBinary(size, in);

            buckets.reserve(size);
            for (size_t i = 0; i < size; ++i)
            {
                Buckets::value_type elem;
                readBinary(elem.first, in);
                readBinary(elem.second, in);
                buckets.emplace(std::move(elem));
            }
        }
    };

    using Table = HashMap<NGramHash, Histogram, TrivialHash>;
    Table table;

    MarkovModelParameters params;

    std::vector<CodePoint> code_points;

    /// Special code point to form context before beginning of string.
    static constexpr CodePoint BEGIN = -1;
    /// Special code point to indicate end of string.
    static constexpr CodePoint END = -2;


    static NGramHash hashContext(const CodePoint * begin, const CodePoint * end)
    {
        return static_cast<NGramHash>(CRC32Hash()(std::string_view(reinterpret_cast<const char *>(begin), (end - begin) * sizeof(CodePoint))));
    }

    /// By the way, we don't have to use actual Unicode numbers. We use just arbitrary bijective mapping.
    static CodePoint readCodePoint(const char *& pos, const char * end)
    {
        size_t length = UTF8::seqLength(*pos);

        if (pos + length > end)
            length = end - pos;
        length = std::min(length, sizeof(CodePoint));

        CodePoint res = 0;
        memcpy(&res, pos, length);
        pos += length;
        return res;
    }

    static bool writeCodePoint(CodePoint code, char *& pos, const char * end)
    {
        size_t length
            = (code & 0xFF000000) ? 4
            : (code & 0xFFFF0000) ? 3
            : (code & 0xFFFFFF00) ? 2
            : 1;

        if (pos + length > end)
            return false;

        memcpy(pos, &code, length);
        pos += length;
        return true;
    }

public:
    explicit MarkovModel(MarkovModelParameters params_)
        : params(std::move(params_)), code_points(params.order, BEGIN) {}

    void serialize(WriteBuffer & out) const
    {
        params.serialize(out);

        size_t size = table.size();
        writeBinary(size, out);

        for (const auto & elem : table)
        {
            writeBinary(elem.getKey(), out);
            elem.getMapped().serialize(out);
        }
    }

    void deserialize(ReadBuffer & in)
    {
        params.deserialize(in);

        size_t size = 0;
        readBinary(size, in);

        table.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            NGramHash key{};
            readBinary(key, in);
            Histogram & histogram = table[key];
            histogram.deserialize(in);
        }
    }

    void consume(const char * data, size_t size)
    {
        /// First 'order' number of code points are pre-filled with BEGIN.
        code_points.resize(params.order);

        const char * pos = data;
        const char * end = data + size;

        while (true)
        {
            const bool inside = pos < end;

            CodePoint next_code_point {};

            if (inside)
                next_code_point = readCodePoint(pos, end);

            for (size_t context_size = 0; context_size < params.order; ++context_size)
            {
                NGramHash context_hash = hashContext(code_points.data() + code_points.size() - context_size, code_points.data() + code_points.size());

                if (inside)
                    table[context_hash].add(next_code_point);
                else    /// if (context_size != 0 || order == 0)     /// Don't allow to break string without context (except order-0 model).
                    table[context_hash].addEnd();
            }

            if (inside)
                code_points.push_back(next_code_point);
            else
                break;
        }
    }

    void finalize()
    {
        if (params.num_buckets_cutoff)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();

                if (histogram.buckets.size() < params.num_buckets_cutoff)
                {
                    histogram.buckets.clear();
                    histogram.total = 0;
                }
            }
        }

        if (params.frequency_cutoff)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                if (histogram.total + histogram.count_end < params.frequency_cutoff)
                {
                    histogram.buckets.clear();
                    histogram.total = 0;
                }
                else
                {
                    Histogram::Buckets new_buckets;
                    UInt64 erased_count = 0;

                    for (const auto & bucket : histogram.buckets)
                    {
                        if (bucket.second >= params.frequency_cutoff)
                            new_buckets.emplace(bucket);
                        else
                            erased_count += bucket.second;
                    }

                    histogram.buckets.swap(new_buckets);
                    histogram.total -= erased_count;
                }
            }
        }

        if (params.frequency_add)
        {
            /// The additions below are on unvalidated user input (`frequency_add` is unbounded), so do them
            /// with checked arithmetic and fail closed: a silent `UInt64` wrap would corrupt the histogram
            /// counters and could degenerate the model into sampling only `END` instead of lowering skew.
            auto throw_overflow = [&]
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The frequency-add parameter value {} is too large: it overflows the frequency counters of the trained Markov model",
                    params.frequency_add);
            };

            /// `frequency_add` is `size_t`, which is a type distinct from `UInt64` on some platforms
            /// (e.g. Darwin), and the checked arithmetic functions require identical operand types.
            const UInt64 frequency_add = params.frequency_add;

            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                for (auto & bucket : histogram.buckets)
                    if (common::addOverflow(bucket.second, frequency_add, bucket.second))
                        throw_overflow();

                UInt64 total_addition = 0;
                if (common::addOverflow(histogram.count_end, frequency_add, histogram.count_end)
                    || common::mulOverflow(frequency_add, static_cast<UInt64>(histogram.buckets.size()), total_addition)
                    || common::addOverflow(histogram.total, total_addition, histogram.total))
                    throw_overflow();
            }
        }

        if (params.frequency_desaturate > 0.0)
        {
            for (auto & elem : table)
            {
                Histogram & histogram = elem.getMapped();
                if (!histogram.total)
                    continue;

                double average = static_cast<double>(histogram.total) / static_cast<double>(histogram.buckets.size());

                UInt64 new_total = 0;
                for (auto & bucket : histogram.buckets)
                {
                    bucket.second = static_cast<UInt64>(static_cast<double>(bucket.second) * (1.0 - params.frequency_desaturate) + average * params.frequency_desaturate);
                    new_total += bucket.second;
                }

                histogram.total = new_total;
            }
        }
    }


    size_t generate(char * data, size_t desired_size, size_t buffer_size,
        UInt64 seed, const char * determinator_data, size_t determinator_size)
    {
        code_points.resize(params.order);

        char * pos = data;
        char * end = data + buffer_size;

        while (pos < end)
        {
            Table::LookupResult it = {};

            size_t context_size = params.order;
            while (true)
            {
                it = table.find(hashContext(code_points.data() + code_points.size() - context_size, code_points.data() + code_points.size()));
                if (it && it->getMapped().total + it->getMapped().count_end != 0)
                    break;

                if (context_size == 0)
                    break;
                --context_size;
            }

            if (!it)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error in markov model");

            size_t offset_from_begin_of_string = pos - data;
            size_t determinator_sliding_window_size = std::min(params.determinator_sliding_window_size, determinator_size);

            size_t determinator_sliding_window_overflow = offset_from_begin_of_string + determinator_sliding_window_size > determinator_size
                ? offset_from_begin_of_string + determinator_sliding_window_size - determinator_size : 0;

            const char * determinator_sliding_window_begin = determinator_data + offset_from_begin_of_string - determinator_sliding_window_overflow;

            SipHash hash;
            hash.update(seed);
            hash.update(determinator_sliding_window_begin, determinator_sliding_window_size);
            hash.update(determinator_sliding_window_overflow);
            UInt64 determinator = hash.get64();

            /// If string is greater than desired_size, increase probability of end.
            double end_probability_multiplier = 0;
            Int64 num_bytes_after_desired_size = (pos - data) - desired_size;

            if (num_bytes_after_desired_size > 0)
                end_probability_multiplier = std::pow(1.25, num_bytes_after_desired_size);

            CodePoint code = it->getMapped().sample(determinator, end_probability_multiplier);

            if (code == END)
                break;

            if (num_bytes_after_desired_size > 0)
            {
                /// Heuristic: break at ASCII non-alnum code point.
                /// This allows to be close to desired_size but not break natural looking words.
                if (code < 128 && !isAlphaNumericASCII(static_cast<char>(code)))
                    break;
            }

            if (!writeCodePoint(code, pos, end))
                break;

            code_points.push_back(code);
        }

        return pos - data;
    }
};


/// Generate length of strings as above.
/// To generate content of strings, use
///  order-N Markov model on Unicode code points,
///  and to generate next code point use deterministic RNG
///  determined by hash of a sliding window (default 8 bytes) of source string.
/// This is intended to generate locally-similar strings from locally-similar sources.
class StringModel : public IModel
{
private:
    UInt64 seed;
    MarkovModel markov_model;

public:
    StringModel(UInt64 seed_, MarkovModelParameters params_) : seed(seed_), markov_model(std::move(params_)) {}

    void train(const IColumn & column) override
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        for (size_t i = 0; i < size; ++i)
        {
            auto string = column_string.getDataAt(i);
            markov_model.consume(string.data(), string.size());
        }
    }

    void finalize() override
    {
        markov_model.finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnString & column_string = assert_cast<const ColumnString &>(column);
        size_t size = column_string.size();

        auto res_column = ColumnString::create();
        res_column->reserve(size);

        std::string new_string;
        for (size_t i = 0; i < size; ++i)
        {
            auto src_string = column_string.getDataAt(i);
            size_t desired_string_size = transform(src_string.size(), seed);
            new_string.resize(desired_string_size * 2);

            size_t actual_size = 0;
            if (desired_string_size != 0)
                actual_size = markov_model.generate(new_string.data(), desired_string_size, new_string.size(), seed, src_string.data(), src_string.size());

            res_column->insertData(new_string.data(), actual_size);
        }

        return res_column;
    }

    void updateSeed() override
    {
        seed = hash(seed);
    }

    void serialize(WriteBuffer & out) const override
    {
        markov_model.serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        markov_model.deserialize(in);
    }
};


class ArrayModel : public IModel
{
private:
    ModelPtr nested_model;

public:
    explicit ArrayModel(ModelPtr nested_model_) : nested_model(std::move(nested_model_)) {}

    void train(const IColumn & column) override
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const IColumn & nested_column = column_array.getData();

        nested_model->train(nested_column);
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
        const IColumn & nested_column = column_array.getData();

        ColumnPtr new_nested_column = nested_model->generate(nested_column);

        return ColumnArray::create(IColumn::mutate(std::move(new_nested_column)), IColumn::mutate(column_array.getOffsetsPtr()));
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }

    void serialize(WriteBuffer & out) const override
    {
        nested_model->serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        nested_model->deserialize(in);
    }
};


class NullableModel : public IModel
{
private:
    ModelPtr nested_model;

public:
    explicit NullableModel(ModelPtr nested_model_) : nested_model(std::move(nested_model_)) {}

    void train(const IColumn & column) override
    {
        const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
        const IColumn & nested_column = column_nullable.getNestedColumn();

        nested_model->train(nested_column);
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnNullable & column_nullable = assert_cast<const ColumnNullable &>(column);
        const IColumn & nested_column = column_nullable.getNestedColumn();

        ColumnPtr new_nested_column = nested_model->generate(nested_column);

        return ColumnNullable::create(IColumn::mutate(std::move(new_nested_column)), IColumn::mutate(column_nullable.getNullMapColumnPtr()));
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }

    void serialize(WriteBuffer & out) const override
    {
        nested_model->serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        nested_model->deserialize(in);
    }
};


/// Obfuscate `LowCardinality` columns as their dictionary type: the column is converted to a full column,
/// obfuscated by the model of the dictionary type and converted back. Training on the full column keeps
/// the frequencies of the values, which the models of the strings rely on.
class LowCardinalityModel : public IModel
{
private:
    ModelPtr nested_model;
    DataTypePtr type;

public:
    LowCardinalityModel(ModelPtr nested_model_, DataTypePtr type_) : nested_model(std::move(nested_model_)), type(std::move(type_)) {}

    void train(const IColumn & column) override
    {
        nested_model->train(*column.convertToFullColumnIfLowCardinality());
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        ColumnPtr new_full_column = nested_model->generate(*column.convertToFullColumnIfLowCardinality());

        auto res_column = type->createColumn();
        assert_cast<ColumnLowCardinality &>(*res_column).insertRangeFromFullColumn(*new_full_column, 0, new_full_column->size());
        return res_column;
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }

    void serialize(WriteBuffer & out) const override
    {
        nested_model->serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        nested_model->deserialize(in);
    }
};


/// Obfuscate every element of a `Tuple` independently, as if it were a separate column.
class TupleModel : public IModel
{
private:
    std::vector<ModelPtr> element_models;

public:
    explicit TupleModel(std::vector<ModelPtr> element_models_) : element_models(std::move(element_models_)) {}

    void train(const IColumn & column) override
    {
        const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
        for (size_t i = 0; i < element_models.size(); ++i)
            element_models[i]->train(column_tuple.getColumn(i));
    }

    void finalize() override
    {
        for (auto & model : element_models)
            model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);

        /// An empty tuple has no elements to carry the number of rows.
        if (element_models.empty())
            return column.cloneResized(column.size());

        Columns new_columns(element_models.size());
        for (size_t i = 0; i < element_models.size(); ++i)
            new_columns[i] = element_models[i]->generate(column_tuple.getColumn(i));

        return ColumnTuple::create(std::move(new_columns));
    }

    void updateSeed() override
    {
        for (auto & model : element_models)
            model->updateSeed();
    }

    void serialize(WriteBuffer & out) const override
    {
        for (const auto & model : element_models)
            model->serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        for (auto & model : element_models)
            model->deserialize(in);
    }
};


/// Obfuscate a `Map` as its nested `Array(Tuple(keys, values))`. The sizes of the maps are kept, and the
/// keys and the values are obfuscated independently, so different keys may become equal.
class MapModel : public IModel
{
private:
    ModelPtr nested_model;

public:
    explicit MapModel(ModelPtr nested_model_) : nested_model(std::move(nested_model_)) {}

    void train(const IColumn & column) override
    {
        nested_model->train(assert_cast<const ColumnMap &>(column).getNestedColumn());
    }

    void finalize() override
    {
        nested_model->finalize();
    }

    ColumnPtr generate(const IColumn & column) override
    {
        return ColumnMap::create(nested_model->generate(assert_cast<const ColumnMap &>(column).getNestedColumn()));
    }

    void updateSeed() override
    {
        nested_model->updateSeed();
    }

    void serialize(WriteBuffer & out) const override
    {
        nested_model->serialize(out);
    }

    void deserialize(ReadBuffer & in) override
    {
        nested_model->deserialize(in);
    }
};


class ModelFactory
{
private:
    template <typename T>
    static ModelPtr getDecimalModel(const IDataType & data_type, UInt64 seed)
    {
        const auto & type = assert_cast<const DataTypeDecimal<T> &>(data_type);
        using Native = typename T::NativeType;
        Native max_abs_value = DataTypeDecimal<T>::getScaleMultiplier(type.getPrecision()).value - 1;
        return std::make_unique<UnderlyingIntegerModel<ColumnDecimal<T>, Native>>(seed, max_abs_value);
    }

public:
    ModelPtr get(const IDataType & data_type, UInt64 seed, MarkovModelParameters markov_model_params) const
    {
        WhichDataType which(data_type);

        /// The integer models below permute values using 64-bit arithmetic (`getUInt`/`getInt` and
        /// a `UInt64`/`Int64` `Field`), so the wide integers have a model of their own.
        if (which.isUInt128())
            return std::make_unique<UnderlyingIntegerModel<ColumnUInt128, UInt128>>(seed);
        if (which.isInt128())
            return std::make_unique<UnderlyingIntegerModel<ColumnInt128, Int128>>(seed);
        if (which.isUInt256())
            return std::make_unique<UnderlyingIntegerModel<ColumnUInt256, UInt256>>(seed);
        if (which.isInt256())
            return std::make_unique<UnderlyingIntegerModel<ColumnInt256, Int256>>(seed);

        if (isInteger(data_type))
        {
            if (isNativeUInt(data_type))
                return std::make_unique<UnsignedIntegerModel>(seed);

            Int64 min_value = std::numeric_limits<Int64>::min();
            if (which.isInt8())
                min_value = std::numeric_limits<Int8>::min();
            else if (which.isInt16())
                min_value = std::numeric_limits<Int16>::min();
            else if (which.isInt32())
                min_value = std::numeric_limits<Int32>::min();

            return std::make_unique<SignedIntegerModel>(seed, min_value);
        }

        if (which.isBFloat16())
            return std::make_unique<FloatModel<BFloat16>>(seed);

        if (typeid_cast<const DataTypeFloat32 *>(&data_type))
            return std::make_unique<FloatModel<Float32>>(seed);

        if (typeid_cast<const DataTypeFloat64 *>(&data_type))
            return std::make_unique<FloatModel<Float64>>(seed);

        if (which.isDecimal32())
            return getDecimalModel<Decimal32>(data_type, seed);
        if (which.isDecimal64())
            return getDecimalModel<Decimal64>(data_type, seed);
        if (which.isDecimal128())
            return getDecimalModel<Decimal128>(data_type, seed);
        if (which.isDecimal256())
            return getDecimalModel<Decimal256>(data_type, seed);

        if (typeid_cast<const DataTypeDate *>(&data_type) || typeid_cast<const DataTypeDate32 *>(&data_type))
            return std::make_unique<IdentityModel>();

        /// `DateTime` columns can carry an explicit timezone that differs from the server timezone.
        /// The obfuscator preserves the date component as displayed in the column's timezone, so the
        /// model must use that timezone (which falls back to the server timezone when none is set).
        if (const auto * type = typeid_cast<const DataTypeDateTime *>(&data_type))
            return std::make_unique<DateTimeModel>(seed, type->getTimeZone());

        if (const auto * type = typeid_cast<const DataTypeDateTime64 *>(&data_type))
            return std::make_unique<DateTime64Model>(seed, type->getScaleMultiplier().value, type->getTimeZone());

        if (typeid_cast<const DataTypeString *>(&data_type))
            return std::make_unique<StringModel>(seed, markov_model_params);

        if (typeid_cast<const DataTypeFixedString *>(&data_type))
            return std::make_unique<FixedStringModel>(seed);

        if (typeid_cast<const DataTypeUUID *>(&data_type))
            return std::make_unique<UUIDModel>(seed);

        if (typeid_cast<const DataTypeIPv4 *>(&data_type))
            return std::make_unique<UnderlyingIntegerModel<ColumnIPv4, UInt32>>(seed);

        if (typeid_cast<const DataTypeIPv6 *>(&data_type))
            return std::make_unique<UnderlyingIntegerModel<ColumnIPv6, UInt128>>(seed);

        if (const auto * type = typeid_cast<const DataTypeEnum8 *>(&data_type))
            return std::make_unique<EnumModel<Int8>>(seed, type->getValues());

        if (const auto * type = typeid_cast<const DataTypeEnum16 *>(&data_type))
            return std::make_unique<EnumModel<Int16>>(seed, type->getValues());

        if (typeid_cast<const DataTypeNothing *>(&data_type))
            return std::make_unique<IdentityModel>();

        if (const auto * type = typeid_cast<const DataTypeArray *>(&data_type))
            return std::make_unique<ArrayModel>(get(*type->getNestedType(), seed, markov_model_params));

        if (const auto * type = typeid_cast<const DataTypeNullable *>(&data_type))
            return std::make_unique<NullableModel>(get(*type->getNestedType(), seed, markov_model_params));

        if (const auto * type = typeid_cast<const DataTypeLowCardinality *>(&data_type))
            return std::make_unique<LowCardinalityModel>(get(*type->getDictionaryType(), seed, markov_model_params), data_type.getPtr());

        if (const auto * type = typeid_cast<const DataTypeTuple *>(&data_type))
        {
            const auto & elements = type->getElements();
            const auto & names = type->getElementNames();

            std::vector<ModelPtr> element_models;
            element_models.reserve(elements.size());
            for (size_t i = 0; i < elements.size(); ++i)
                element_models.emplace_back(get(*elements[i], hash(seed, names[i]), markov_model_params));

            return std::make_unique<TupleModel>(std::move(element_models));
        }

        if (const auto * type = typeid_cast<const DataTypeMap *>(&data_type))
            return std::make_unique<MapModel>(get(*type->getNestedType(), seed, markov_model_params));

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Obfuscation is not implemented for data type {}", data_type.getName());
    }
};

}
