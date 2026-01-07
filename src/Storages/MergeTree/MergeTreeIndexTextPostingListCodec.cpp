#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

#include <roaring/roaring.hh>

#include <config.h>

#if USE_SIMDCOMP
extern "C"
{
#include <simdcomp.h>
}
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

static constexpr size_t BLOCK_SIZE = 128;

namespace impl
{

#if USE_SIMDCOMP
struct BlockCodec
{

    /// Returns {compressed_bytes, bits} where bits is the max bit-width required
    /// to represent all values in [0..n).
    static std::pair<size_t, size_t> calculateNeededBytesAndMaxBits(std::span<uint32_t> & data) noexcept
    {
        size_t n = data.size();
        auto bits = maxbits_length(data.data(), n);
        auto bytes = simdpack_compressedbytes(n, bits);
        return {bytes, bits};
    }

    static uint32_t encode(std::span<uint32_t> & in, uint32_t max_bits, std::span<char> & out) noexcept
    {
        /// simdcomp expects __m128i* output pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128_out = reinterpret_cast<__m128i *>(out.data());
        auto * m128_out_end = simdpack_length(in.data(), in.size(), m128_out, max_bits);
        auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
        out = out.subspan(used);
        return used;
    }

    static std::size_t decode(std::span<const std::byte> & in, std::size_t n, uint32_t max_bits, std::span<uint32_t> & out) noexcept
    {
        /// simdcomp expects __m128i* input pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128i_in = reinterpret_cast<const __m128i *>(in.data());
        auto * m128i_in_end = simdunpack_length(m128i_in, n, out.data(), max_bits);
        auto used = static_cast<size_t>(m128i_in_end - m128i_in) * sizeof(__m128);
        in = in.subspan(used);
        return used;
    }
};
#else
struct BlockCodec
{
    /// Returns {compressed_bytes, bits} where bits is the max bit-width required
    /// to represent all values in [0..n).
    static inline std::pair<size_t, size_t> calculateNeededBytesAndMaxBits(std::span<uint32_t> & data) noexcept
    {
        size_t n = data.size();
        /// In the posting list case, rowids are in ascending order, so the last rowid is always the maximum.
        uint32_t bits = n == 0 ? 0 : std::bit_width(data.back());
        size_t bytes = (n * bits + 7) / 8;
        return {bytes, bits};
    }

    static uint32_t encode(std::span<uint32_t> & in, uint32_t max_bits, std::span<char> & out) noexcept
    {
        const size_t n = in.size();
        if (n == 0 || max_bits == 0)
            return 0;

        size_t used = (n * max_bits + 7) / 8;
        char * dst = out.data();

        if (max_bits == 32)
        {
            std::memcpy(dst, in.data(), n * sizeof(uint32_t));
            out = out.subspan(used);
            return static_cast<uint32_t>(used);
        }

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        size_t dst_offset = 0;

        for (uint32_t v : in)
        {
            acc |= (static_cast<uint64_t>(v) << acc_bits);
            acc_bits += max_bits;

            while (acc_bits >= 32)
            {
                if (dst_offset + 4 <= used)
                {
                    uint32_t word = static_cast<uint32_t>(acc);
                    std::memcpy(dst + dst_offset, &word, sizeof(word));
                }
                dst_offset += 4;

                acc >>= 32;
                acc_bits -= 32;
            }
        }

        if (acc_bits > 0 && dst_offset < used)
        {
            uint32_t word = static_cast<uint32_t>(acc);
            std::memcpy(dst + dst_offset, &word, sizeof(word));
        }

        out = out.subspan(used);
        return static_cast<uint32_t>(used);
    }

    static std::size_t decode(std::span<const std::byte> & in, std::size_t n, uint32_t max_bits, std::span<uint32_t> & out) noexcept
    {
        if (n == 0)
            return 0;

        if (max_bits == 0)
            return 0;

        size_t used = (n * max_bits + 7) / 8;
        const char * src = reinterpret_cast<const char *>(in.data());

        if (max_bits == 32)
        {
            std::memcpy(out.data(), src, n * sizeof(uint32_t));
            in = in.subspan(used);
            return used;
        }

        uint32_t mask = (1u << max_bits) - 1u;

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        size_t dst_offset = 0;

        for (size_t i = 0; i < n; ++i)
        {
            while (acc_bits < max_bits)
            {
                uint32_t word = 0;
                if (dst_offset < used)
                {
                    size_t rem = used - dst_offset;
                    std::memcpy(&word, src + dst_offset, rem >= 4 ? 4 : rem);
                }
                dst_offset += 4;
                acc |= (static_cast<uint64_t>(word) << acc_bits);
                acc_bits += 32;
            }

            out[i] = static_cast<uint32_t>(acc) & mask;
            acc >>= max_bits;
            acc_bits -= max_bits;
        }

        in = in.subspan(used);
        return used;
    }
};
#endif

}

/// Normalize the requested block size to a multiple of BLOCK_SIZE.
/// We encode/decode posting lists in fixed-size blocks, and the SIMD bit-packing
/// implementation expects block-aligned sizes for efficient processing.
PostingListCodecImpl::PostingListCodecImpl(size_t postings_list_block_size)
    : posting_list_block_size((postings_list_block_size + BLOCK_SIZE - 1) & ~(BLOCK_SIZE - 1))
{
    compressed_data.reserve(BLOCK_SIZE);
    current_segment.reserve(BLOCK_SIZE);
}

void PostingListCodecImpl::insert(uint32_t row)
{
    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = row;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = row;
        current_segment.emplace_back(row - prev_row);
        ++rows_in_current_segment;
        ++total_rows;
        return;
    }

    current_segment.emplace_back(row - prev_row);
    prev_row = row;
    ++rows_in_current_segment;
    ++total_rows;

    if (current_segment.size() == BLOCK_SIZE)
        compressBlock(current_segment);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::insert(std::span<uint32_t> rows)
{
    chassert(rows.size() == BLOCK_SIZE && rows_in_current_segment % BLOCK_SIZE == 0);

    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = rows.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = rows.front();
        rows_in_current_segment += BLOCK_SIZE;
        total_rows += BLOCK_SIZE;
    }

    auto last_row = rows.back();
    std::adjacent_difference(rows.begin(), rows.end(), rows.begin());
    rows[0] -= prev_row;
    prev_row = last_row;

    compressBlock(rows);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::decode(ReadBuffer & in, PostingList & postings)
{
    Header header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_row = header.first_row_id;

    uint32_t tail_block_size = header.cardinality % BLOCK_SIZE;
    uint32_t full_block_count = header.cardinality / BLOCK_SIZE;

    current_segment.reserve(BLOCK_SIZE);
    if (header.payload_bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.payload_bytes);
    compressed_data.resize(header.payload_bytes);
    in.readStrict(compressed_data.data(), header.payload_bytes);

    //auto * p = reinterpret_cast<unsigned char *> (compressed_data.data());
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    for (uint32_t i = 0; i < full_block_count; i++)
    {
        decodeOneBlock(compressed_data_span, BLOCK_SIZE, prev_row, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
    if (tail_block_size)
    {
        decodeOneBlock(compressed_data_span, tail_block_size, prev_row, current_segment);
        postings.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_begin, descriptor.row_end);
        Header header(static_cast<uint8_t>(codec_type), descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_begin);
        header.write(out);
        out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

namespace
{
void encodeU8(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

uint8_t decodeU8(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}
}

void PostingListCodecImpl::compressBlock(std::span<uint32_t> segment)
{
    auto & last_segment = segment_descriptors.back();
    last_segment.cardinality += segment.size();
    last_segment.row_end = prev_row;

    auto [needed_bytes, max_bits] = impl::BlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t memory_gap = compressed_data.capacity() - compressed_data.size();
    size_t need_bytes = needed_bytes + 1;
    if (memory_gap < need_bytes)
    {
        auto min_need = need_bytes - memory_gap;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }
    /// Block Layout: [1byte(max_bits)][payload]
    size_t offset = compressed_data.size();
    compressed_data.resize(compressed_data.size() + need_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + offset, need_bytes);
    encodeU8(max_bits, compressed_data_span);
    auto used = impl::BlockCodec::encode(segment, max_bits, compressed_data_span);
    chassert(used == needed_bytes && compressed_data_span.empty());

    last_segment.compressed_data_size = compressed_data.size() - last_segment.compressed_data_offset;
    current_segment.clear();
}

void PostingListCodecImpl::decodeOneBlock(
        std::span<const std::byte> & in, size_t count, uint32_t & prev_row, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    uint8_t bits = decodeU8(in);
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    impl::BlockCodec::decode(in, count, bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, prev_row);
    prev_row = current_segment.empty() ? prev_row : current_segment.back();
}

void PostingListCodecSIMDComp::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecImpl impl;
    impl.decode(in, postings);
}

void PostingListCodecSIMDComp::encode(
        const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecImpl impl(posting_list_block_size);

    if (postings.isSmall())
    {
        const auto & small_postings = postings.getSmall();
        size_t num_postings = postings.size();
        for (size_t i = 0; i < num_postings; ++i)
            impl.insert(small_postings[i]);
    }
    else
    {
        std::vector<uint32_t> rowids;
        rowids.resize(postings.size());
        const auto & large_postings = postings.getLarge();
        large_postings.toUint32Array(rowids.data());
        std::span<uint32_t> rowids_view(rowids.data(), rowids.size());

        while (rowids_view.size() >= BLOCK_SIZE)
        {
            auto front = rowids_view.first(BLOCK_SIZE);
            impl.insert(front);
            rowids_view = rowids_view.subspan(BLOCK_SIZE);
        }

        if (!rowids_view.empty())
        {
            for (auto rowid : rowids_view)
                impl.insert(rowid);
        }
    }

    impl.encode(out, info);
}
}

