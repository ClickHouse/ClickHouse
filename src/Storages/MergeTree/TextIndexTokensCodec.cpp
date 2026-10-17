#include <Storages/MergeTree/TextIndexTokensCodec.h>

#include <Columns/ColumnString.h>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/transformEndianness.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <base/arithmeticOverflow.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace TextIndexTokensCodec
{

namespace
{

/// Token lengths never exceed `SerializationString::MAX_STRING_SIZE`, so they fit into 32 bits.
constexpr UInt64 MAX_BIT_WIDTH_FRONT_CODING_V2 = 32;

void readBitPacked(ReadBuffer & istr, size_t num_values, UInt8 bit_width, PODArray<UInt32> & values)
{
    size_t num_bits = 0;
    if (common::mulOverflow<size_t>(num_values, bit_width, num_bits))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: too many tokens ({}) in a block", num_values);

    PODArray<UInt64> words;
    words.resize_fill((num_bits + 63) / 64 + 1);
    istr.readStrict(reinterpret_cast<char *>(words.data()), (num_bits + 7) / 8);

    for (auto & word : words)
        transformEndianness<std::endian::little>(word);

    values.resize(num_values);
    for (size_t i = 0; i < num_values; ++i)
        values[i] = static_cast<UInt32>(readBitsPacked64(words.data(), i * bit_width, bit_width));
}

UInt8 readBitWidth(ReadBuffer & istr)
{
    UInt64 bit_width = 0;
    readVarUInt(bit_width, istr);
    if (bit_width > MAX_BIT_WIDTH_FRONT_CODING_V2)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: bit width ({}) exceeds the maximum ({})", bit_width, MAX_BIT_WIDTH_FRONT_CODING_V2);
    return static_cast<UInt8>(bit_width);
}

}

void writeBitPacked(std::span<const UInt32> values, UInt8 bit_width, WriteBuffer & ostr)
{
    size_t num_bits = values.size() * bit_width;
    PODArray<UInt64> words;
    words.resize_fill((num_bits + 63) / 64 + 1);

    for (size_t i = 0; i < values.size(); ++i)
        writeBitsPacked64(words.data(), i * bit_width, values[i]);

    for (auto & word : words)
        transformEndianness<std::endian::little>(word);

    ostr.write(reinterpret_cast<const char *>(words.data()), (num_bits + 7) / 8);
}

ColumnPtr deserializeRaw(ReadBuffer & istr, size_t num_tokens)
{
    auto tokens_column = ColumnString::create();
    tokens_column->reserve(num_tokens);

    auto serialization_string = SerializationString::create();
    serialization_string->deserializeBinaryBulk(*tokens_column, istr, num_tokens, 0.0);

    return tokens_column;
}

ColumnPtr deserializeFrontCodingV1(ReadBuffer & istr, size_t num_tokens)
{
    auto tokens_column = ColumnString::create();

    if (num_tokens != 0)
    {
        tokens_column->reserve(num_tokens);
        ColumnString::Chars & data = tokens_column->getChars();
        ColumnString::Offsets & offsets = tokens_column->getOffsets();

        /// Avoiding calling resize in a loop improves the performance.
        /// The average length of words in English language is 4.7 characters, rounded up to the next power of 2.
        data.resize(roundUpToPowerOfTwoOrZero(num_tokens * 8));

        size_t offset = 0;

        /// Read the first token
        {
            UInt64 first_token_size = 0;
            readVarUInt(first_token_size, istr);
            /// Prevent a corrupt or malicious .dct file from allocating huge amounts of memory
            if (first_token_size > SerializationString::MAX_STRING_SIZE)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: first token size ({}) exceeds the maximum ({})", first_token_size, SerializationString::MAX_STRING_SIZE);
            offset += first_token_size;
            if (offset > data.size())
                data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));
            istr.readStrict(reinterpret_cast<char *>(data.data()), first_token_size);
            offsets.push_back(offset);
        }

        size_t previous_token_offset = 0;
        for (size_t i = 1; i < num_tokens; ++i)
        {
            const UInt64 data_offset = offset;

            UInt64 lcp = 0;
            readVarUInt(lcp, istr);
            UInt64 data_size = 0;
            readVarUInt(data_size, istr);

            /// Reject a corrupted or malicious `.dct`: an out-of-range `lcp` or an overflowing `lcp + data_size` would wrap `offset`, skip the resize, and cause an out-of-bounds write below.
            const UInt64 previous_token_size = data_offset - previous_token_offset;
            if (lcp > previous_token_size)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Corrupted text index dictionary: front-coding longest common prefix ({}) exceeds the previous token size ({})",
                    lcp, previous_token_size);

            UInt64 token_size = 0;
            UInt64 next_offset = 0;
            if (common::addOverflow(lcp, data_size, token_size) || common::addOverflow<UInt64>(offset, token_size, next_offset))
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Corrupted text index dictionary: front-coding token size overflows (lcp = {}, data_size = {})",
                    lcp, data_size);

            if (token_size > SerializationString::MAX_STRING_SIZE)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: front-coding token size ({}) exceeds the maximum ({})", token_size, SerializationString::MAX_STRING_SIZE);

            offset = next_offset;

            if (offset > data.size())
                data.resize_exact(roundUpToPowerOfTwoOrZero(std::max(offset, data.size() * 2)));

            std::memcpy(&data[data_offset], &data[previous_token_offset], lcp);
            istr.readStrict(reinterpret_cast<char *>(&data[data_offset + lcp]), data_size);

            offsets.push_back(offset);
            previous_token_offset = data_offset;
        }

        data.resize_exact(offset);
    }

    return tokens_column;
}

ColumnPtr deserializeFrontCodingV2(ReadBuffer & istr, size_t num_tokens)
{
    auto tokens_column = ColumnString::create();
    if (num_tokens == 0)
        return tokens_column;

    ColumnString::Chars & data = tokens_column->getChars();
    ColumnString::Offsets & offsets = tokens_column->getOffsets();

    UInt64 first_token_size = 0;
    readVarUInt(first_token_size, istr);
    if (first_token_size > SerializationString::MAX_STRING_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: first token size ({}) exceeds the maximum ({})", first_token_size, SerializationString::MAX_STRING_SIZE);
    data.resize(first_token_size);
    istr.readStrict(reinterpret_cast<char *>(data.data()), first_token_size);

    const UInt8 lcp_bit_width = readBitWidth(istr);
    const UInt8 suffix_size_bit_width = readBitWidth(istr);

    PODArray<UInt32> lcps;
    PODArray<UInt32> suffix_sizes;
    readBitPacked(istr, num_tokens - 1, lcp_bit_width, lcps);
    readBitPacked(istr, num_tokens - 1, suffix_size_bit_width, suffix_sizes);

    offsets.resize(num_tokens);
    offsets[0] = first_token_size;
    UInt64 total_suffix_size = 0;

    for (size_t i = 1; i < num_tokens; ++i)
    {
        const UInt64 previous_token_size = offsets[i - 1] - offsets[i - 2];
        const UInt64 lcp = lcps[i - 1];
        const UInt64 token_size = lcp + suffix_sizes[i - 1];

        if (lcp > previous_token_size)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Corrupted text index dictionary: front-coding longest common prefix ({}) exceeds the previous token size ({})",
                lcp, previous_token_size);

        if (token_size > SerializationString::MAX_STRING_SIZE)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: front-coding token size ({}) exceeds the maximum ({})", token_size, SerializationString::MAX_STRING_SIZE);

        if (common::addOverflow<UInt64>(offsets[i - 1], token_size, offsets[i]))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted text index dictionary: front-coding total size overflows");

        total_suffix_size += suffix_sizes[i - 1];
    }

    PODArray<char> suffixes(total_suffix_size);
    istr.readStrict(suffixes.data(), total_suffix_size);

    data.resize_exact(offsets.back());
    const char * suffix = suffixes.data();

    for (size_t i = 1; i < num_tokens; ++i)
    {
        const UInt64 lcp = lcps[i - 1];
        const UInt64 suffix_size = suffix_sizes[i - 1];
        std::memcpy(&data[offsets[i - 1]], &data[offsets[i - 2]], lcp);
        std::memcpy(&data[offsets[i - 1] + lcp], suffix, suffix_size);
        suffix += suffix_size;
    }

    return tokens_column;
}

}

}
