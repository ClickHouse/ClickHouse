#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <bit>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int TOO_MANY_ROWS;
}

/// Exact binary64 accumulation in units of 2^-1074, rounded once at finalization.
/// At most UInt64::max finite inputs have magnitude below 2^2162. The signed
/// 2,176-bit two's-complement integer therefore cannot overflow for valid states.
struct AggregateFunctionSumPromQLData
{
    void add(Float64 value)
    {
        if (input_count == std::numeric_limits<UInt64>::max())
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "sumPromQL input count exceeds UInt64");

        const UInt64 bits = std::bit_cast<UInt64>(value);
        const UInt64 exponent = (bits >> 52) & 0x7ff;
        UInt64 significand = bits & FRACTION_MASK;
        const bool negative = (bits >> 63) != 0;
        if (exponent == 0x7ff)
        {
            flags = static_cast<UInt8>(flags | (significand != 0 ? NAN_FLAG : negative ? NEGATIVE_INFINITY_FLAG : POSITIVE_INFINITY_FLAG));
        }
        else
        {
            size_t shift = 0;
            if (exponent != 0)
            {
                significand |= UInt64{1} << 52;
                shift = exponent - 1;
            }
            const size_t word = shift / 64;
            const size_t offset = shift % 64;
            addWord(word, significand << offset, negative);
            if (offset != 0)
                addWord(word + 1, significand >> (64 - offset), negative);
        }
        ++input_count;
    }

    void merge(const AggregateFunctionSumPromQLData & rhs)
    {
        if (rhs.input_count > std::numeric_limits<UInt64>::max() - input_count)
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "sumPromQL merged input count exceeds UInt64");

        UInt64 carry = 0;
        for (size_t i = 0; i < LIMB_COUNT; ++i)
        {
            const unsigned __int128 value = static_cast<unsigned __int128>(limbs[i]) + rhs.limbs[i] + carry;
            limbs[i] = static_cast<UInt64>(value);
            carry = static_cast<UInt64>(value >> 64);
        }
        /// Internal self-merge is safe; the aggregate API still requires distinct states.
        input_count += rhs.input_count;
        flags = static_cast<UInt8>(flags | rhs.flags);
    }

    Float64 get() const
    {
        const bool positive_infinity = (flags & POSITIVE_INFINITY_FLAG) != 0;
        const bool negative_infinity = (flags & NEGATIVE_INFINITY_FLAG) != 0;
        if ((flags & NAN_FLAG) != 0 || (positive_infinity && negative_infinity))
            return std::bit_cast<Float64>(UInt64{0x7ff8000000000000});
        if (positive_infinity || negative_infinity)
            return std::bit_cast<Float64>(INFINITY_BITS | (static_cast<UInt64>(negative_infinity) << 63));

        const bool negative = (limbs.back() >> 63) != 0;
        const Limbs magnitude = getMagnitude();
        size_t top_word = LIMB_COUNT;
        while (top_word != 0 && magnitude[top_word - 1] == 0)
            --top_word;
        if (top_word == 0)
            return 0.;

        size_t top_bit = (top_word - 1) * 64 + 63 - std::countl_zero(magnitude[top_word - 1]);
        const UInt64 sign = static_cast<UInt64>(negative) << 63;
        if (top_bit < 52)
            return std::bit_cast<Float64>(sign | magnitude[0]);
        if (top_bit > 2097)
            return std::bit_cast<Float64>(sign | INFINITY_BITS);

        const size_t shift = top_bit - 52;
        const size_t word = shift / 64;
        const size_t offset = shift % 64;
        UInt64 significand = magnitude[word] >> offset;
        if (offset != 0 && word + 1 < LIMB_COUNT)
            significand |= magnitude[word + 1] << (64 - offset);
        significand &= (UInt64{1} << 53) - 1;

        if (shift != 0 && bitAt(magnitude, shift - 1) && (anyBelow(magnitude, shift - 1) || (significand & 1) != 0))
        {
            ++significand;
            if (significand == (UInt64{1} << 53))
            {
                significand >>= 1;
                ++top_bit;
            }
        }
        if (top_bit > 2097)
            return std::bit_cast<Float64>(sign | INFINITY_BITS);
        const UInt64 exponent = top_bit - 51;
        return std::bit_cast<Float64>(sign | (exponent << 52) | (significand & FRACTION_MASK));
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(FORMAT_VERSION, buf);
        writeBinaryLittleEndian(input_count, buf);
        writeBinary(flags, buf);
        for (UInt64 word : limbs)
            writeBinaryLittleEndian(word, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        UInt8 layout_version = 0;
        readBinary(layout_version, buf);
        if (layout_version != FORMAT_VERSION)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported sumPromQL state layout version {}", static_cast<unsigned int>(layout_version));

        AggregateFunctionSumPromQLData restored;
        readBinaryLittleEndian(restored.input_count, buf);
        readBinary(restored.flags, buf);
        for (UInt64 & word : restored.limbs)
            readBinaryLittleEndian(word, buf);
        restored.validate();
        *this = restored;
    }

private:
    static constexpr size_t LIMB_COUNT = 34;
    static constexpr UInt64 FRACTION_MASK = (UInt64{1} << 52) - 1;
    static constexpr UInt64 INFINITY_BITS = UInt64{0x7ff} << 52;
    static constexpr UInt8 FORMAT_VERSION = 2;
    static constexpr UInt8 NAN_FLAG = 1;
    static constexpr UInt8 POSITIVE_INFINITY_FLAG = 2;
    static constexpr UInt8 NEGATIVE_INFINITY_FLAG = 4;
    using Limbs = std::array<UInt64, LIMB_COUNT>;

    Limbs limbs{};
    UInt64 input_count = 0;
    UInt8 flags = 0;

    void addWord(size_t index, UInt64 value, bool negative)
    {
        if (value == 0)
            return;
        for (; index < LIMB_COUNT; ++index)
        {
            const UInt64 previous = limbs[index];
            limbs[index] = negative ? previous - value : previous + value;
            const bool carry_or_borrow = negative ? previous < value : limbs[index] < previous;
            if (!carry_or_borrow)
                break;
            value = 1;
        }
        /// Only modular sign extension can escape the checked signed magnitude bound.
    }

    Limbs getMagnitude() const
    {
        Limbs magnitude = limbs;
        if ((limbs.back() >> 63) != 0)
        {
            UInt64 carry = 1;
            for (UInt64 & word : magnitude)
            {
                word = ~word + carry;
                carry = carry != 0 && word == 0;
            }
        }
        return magnitude;
    }

    static bool bitAt(const Limbs & words, size_t index)
    {
        return ((words[index / 64] >> (index % 64)) & 1) != 0;
    }

    static bool anyBelow(const Limbs & words, size_t exclusive_end)
    {
        const size_t full_words = exclusive_end / 64;
        for (size_t i = 0; i < full_words; ++i)
            if (words[i] != 0)
                return true;
        const size_t partial_bits = exclusive_end % 64;
        return partial_bits != 0 && (words[full_words] & ((UInt64{1} << partial_bits) - 1)) != 0;
    }

    void validate() const
    {
        if ((flags & ~(NAN_FLAG | POSITIVE_INFINITY_FLAG | NEGATIVE_INFINITY_FLAG)) != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid sumPromQL state flags");
        const UInt64 nonfinite_minimum = std::popcount(static_cast<unsigned int>(flags));
        if (nonfinite_minimum > input_count)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid sumPromQL state input count");

        /// Each flagged kind requires at least one nonfinite input. The remaining
        /// slots bound the finite magnitude, without claiming a realizable history.
        const unsigned __int128 product = static_cast<unsigned __int128>(input_count - nonfinite_minimum) * ((UInt64{1} << 53) - 1);
        const UInt64 low = static_cast<UInt64>(product);
        const UInt64 high = static_cast<UInt64>(product >> 64);
        Limbs bound{};
        /// max_finite = (2^53 - 1) << 2045, whose first limb is 31 with offset 61.
        bound[31] = low << 61;
        bound[32] = (low >> 3) | (high << 61);
        bound[33] = high >> 3;
        const Limbs magnitude = getMagnitude();
        for (size_t i = LIMB_COUNT; i != 0; --i)
        {
            if (magnitude[i - 1] < bound[i - 1])
                return;
            if (magnitude[i - 1] > bound[i - 1])
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid sumPromQL state finite magnitude");
        }
    }
};

static_assert(std::numeric_limits<Float64>::is_iec559 && sizeof(Float64) == sizeof(UInt64));

class AggregateFunctionSumPromQL final : public IAggregateFunctionDataHelper<AggregateFunctionSumPromQLData, AggregateFunctionSumPromQL>
{
public:
    explicit AggregateFunctionSumPromQL(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper(argument_types_, {}, std::make_shared<DataTypeFloat64>())
    {
    }

    String getName() const override { return "sumPromQL"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).add(assert_cast<const ColumnFloat64 &>(*columns[0]).getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(data(place).get());
    }

};

}
