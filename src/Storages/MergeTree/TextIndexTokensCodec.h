#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Common/PODArray.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <bit>
#include <span>
#include <string_view>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Token layouts of a text index dictionary block, without the block header (format and number of tokens).
namespace TextIndexTokensCodec
{

void writeBitPacked(std::span<const UInt32> values, UInt8 bit_width, WriteBuffer & ostr);

ColumnPtr deserializeRaw(ReadBuffer & istr, size_t num_tokens);
ColumnPtr deserializeFrontCodingV1(ReadBuffer & istr, size_t num_tokens);
ColumnPtr deserializeFrontCodingV2(ReadBuffer & istr, size_t num_tokens);

inline size_t computeCommonPrefixLength(const std::string_view lhs, const std::string_view rhs)
{
    size_t common_prefix_length = 0;
    size_t max_length = std::min(lhs.size(), rhs.size());

    while (common_prefix_length < max_length && lhs[common_prefix_length] == rhs[common_prefix_length])
        ++common_prefix_length;

    return common_prefix_length;
}

template <typename TokenGetter>
void serializeRaw(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    size_t block_begin,
    size_t block_end)
{
    /// Write tokens the same as in SerializationString::serializeBinaryBulk
    /// to be able to read them later with SerializationString::deserializeBinaryBulk.

    for (size_t i = block_begin; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        TextIndexSerialization::checkTokenSize(current_token.size());
        writeVarUInt(current_token.size(), ostr);
        ostr.write(current_token.data(), current_token.size());
    }
}

/*
 * The front coding implementation is based on the idea from following papers.
 * 1. https://doi.org/10.1109/Innovate-Data.2017.9
 * 2. https://doi.org/10.1145/3448016.345279
 */
template <typename TokenGetter>
void serializeFrontCodingV1(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    size_t block_begin,
    size_t block_end)
{
    const auto & first_token = token_getter(block_begin);
    TextIndexSerialization::checkTokenSize(first_token.size());
    writeVarUInt(first_token.size(), ostr);
    ostr.write(first_token.data(), first_token.size());

    std::string_view previous_token = first_token;
    for (size_t i = block_begin + 1; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        TextIndexSerialization::checkTokenSize(current_token.size());
        auto lcp = computeCommonPrefixLength(previous_token, current_token);
        writeVarUInt(lcp, ostr);
        writeVarUInt(current_token.size() - lcp, ostr);
        ostr.write(current_token.data() + lcp, current_token.size() - lcp);
        previous_token = current_token;
    }
}

/// Same data as front coding, but the per-token lengths are stored as two bit-packed columns followed by all suffixes.
template <typename TokenGetter>
void serializeFrontCodingV2(
    const TokenGetter & token_getter,
    WriteBuffer & ostr,
    size_t block_begin,
    size_t block_end)
{
    const auto & first_token = token_getter(block_begin);
    TextIndexSerialization::checkTokenSize(first_token.size());
    writeVarUInt(first_token.size(), ostr);
    ostr.write(first_token.data(), first_token.size());

    PODArray<UInt32> lcps;
    PODArray<UInt32> suffix_sizes;
    lcps.reserve(block_end - block_begin - 1);
    suffix_sizes.reserve(block_end - block_begin - 1);

    std::string_view previous_token = first_token;
    for (size_t i = block_begin + 1; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        TextIndexSerialization::checkTokenSize(current_token.size());
        auto lcp = computeCommonPrefixLength(previous_token, current_token);
        lcps.push_back(static_cast<UInt32>(lcp));
        suffix_sizes.push_back(static_cast<UInt32>(current_token.size() - lcp));
        previous_token = current_token;
    }

    const auto lcp_bit_width = static_cast<UInt8>(std::bit_width(lcps.empty() ? 0 : *std::ranges::max_element(lcps)));
    const auto suffix_size_bit_width = static_cast<UInt8>(std::bit_width(suffix_sizes.empty() ? 0 : *std::ranges::max_element(suffix_sizes)));

    writeVarUInt(lcp_bit_width, ostr);
    writeVarUInt(suffix_size_bit_width, ostr);
    writeBitPacked(lcps, lcp_bit_width, ostr);
    writeBitPacked(suffix_sizes, suffix_size_bit_width, ostr);

    for (size_t i = block_begin + 1; i < block_end; ++i)
    {
        auto current_token = token_getter(i);
        size_t lcp = lcps[i - block_begin - 1];
        ostr.write(current_token.data() + lcp, current_token.size() - lcp);
    }
}

}

}
