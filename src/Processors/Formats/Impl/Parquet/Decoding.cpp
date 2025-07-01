#include <Processors/Formats/Impl/Parquet/Decoding.h>

#include <arrow/util/bit_stream_utils.h>

#include <IO/VarInt.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_NUMBER;
}

namespace DB::Parquet
{

/// Used for dictionary indices and repetition/definition levels.
/// Throws if any decoded value is >= `limit`.
template <typename T>
struct BitPackedRLEDecoder : public PageDecoder
{
    size_t limit = 0;
    size_t bit_width = 0;
    size_t run_length = 0;
    size_t run_bytes = 0; // if bit-packed run
    size_t bit_idx = 0; // if bit-packed run
    T val = 0; // if RLE run
    bool run_is_rle = false; // otherwise bit-packed

    BitPackedRLEDecoder(std::span<const char> data_, size_t limit_, bool has_header_byte)
        : PageDecoder(data_), limit(limit_)
    {
        static_assert(sizeof(T) <= 4);
        chassert(limit <= std::numeric_limits<T>::max());

        if (has_header_byte)
        {
            requireRemainingBytes(1);
            bit_width = size_t(UInt8(*data));
            data += 1;
            if (bit_width < 1 || bit_width > 8 * sizeof(T))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid dict indices bit width: {}", bit_width);
        }
        else
        {
            chassert(limit > 0);
            bit_width = 32 - __builtin_clz(UInt32(limit - 1));
        }

        chassert(bit_width > 0 && bit_width <= 32);
    }

    void skip(size_t num_values) override
    {
        skipOrDecode<true>(num_values, nullptr);
    }
    void decode(size_t num_values, IColumn & col) override
    {
        auto & out = assert_cast<ColumnVector<T> &>(col).getData();
        decodeArray(num_values, out);
    }
    void decodeArray(size_t num_values, PaddedPODArray<T> & out)
    {
        size_t start = out.size();
        out.resize(start + num_values);
        skipOrDecode<false>(num_values, &out[start]);
    }

    void startRun()
    {
        UInt64 len;
        data = readVarUInt(len, data, end - data);
        if (len & 1)
        {
            /// Bit-packed run.
            size_t groups = len >> 1;
            run_bytes = groups * bit_width;
            requireRemainingBytes(run_bytes > size_t(end - data));
            run_is_rle = false;
            run_length = groups << 3;
            bit_idx = 0;
        }
        else
        {
            const size_t byte_width = (bit_width + 7) / 8;
            chassert(byte_width <= sizeof(T));
            const T value_mask = T((1ul << bit_width) - 1);

            run_length = len >> 1;
            requireRemainingBytes(byte_width);

            memcpy(&val, data, sizeof(T));
            val &= value_mask;

            if (val >= limit)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (rle)");
            run_is_rle = true;
            data += byte_width;
        }
    }

    template <bool SKIP>
    void skipOrDecode(size_t num_values, T * out)
    {
        const T value_mask = T((1ul << bit_width) - 1);
        /// TODO [parquet]: May make sense to have specialized version of this loop for bit_width=1,
        ///                 which is very common as def levels for nullables.

        /// (Some stats from hits.parquet, in case it helps with optimization:
        ///  bit-packed runs: 64879089, total 2548822304 values (~39 values/run),
        ///  RLE runs: 81177527, total 7373423915 values (~91 values/run).)
        while (num_values)
        {
            if (run_length == 0)
                startRun();

            size_t n = std::min(run_length, num_values);
            run_length -= n;
            num_values -= n;

            if (run_is_rle)
            {
                if constexpr (!SKIP)
                {
                    const T v = val; // without this std::fill reloads it from memory on each iteration
                    std::fill(out, out + n, v);
                    out += n;
                }
            }
            else
            {
                if constexpr (!SKIP)
                {
                    for (size_t i = 0; i < n; ++i)
                    {
                        size_t x;
                        memcpy(&x, data + (bit_idx >> 3), 8);
                        x = (x >> (bit_idx & 7)) & value_mask;

                        if (x >= limit)
                            throw Exception(ErrorCodes::INCORRECT_DATA, "Dict index or rep/def level out of bounds (bp)");
                        *out = x;
                        ++out;
                        bit_idx += bit_width;
                    }
                }

                if (!run_length)
                    data += run_bytes;
            }
        }
    }
};

struct PlainFixedSizeDecoder : public PageDecoder
{
    std::shared_ptr<FixedSizeConverter> converter;

    PlainFixedSizeDecoder(std::span<const char> data_, std::shared_ptr<FixedSizeConverter> converter_) : PageDecoder(data_), converter(std::move(converter_)) {}

    void skip(size_t num_values) override
    {
        size_t bytes = num_values * converter->input_size;
        requireRemainingBytes(bytes);
        data += bytes;
    }

    void decode(size_t num_values, IColumn & col) override
    {
        const char * from = data;
        skip(num_values);
        converter->convertColumn(std::span(from, num_values * converter->input_size), num_values, col);
    }
};

struct PlainStringDecoder : public PageDecoder
{
    std::shared_ptr<StringConverter> converter;
    IColumn::Offsets offsets;

    PlainStringDecoder(std::span<const char> data_, std::shared_ptr<StringConverter> converter_) : PageDecoder(data_), converter(std::move(converter_)) {}

    void skip(size_t num_values) override
    {
        for (size_t i = 0; i < num_values; ++i)
        {
            UInt32 x;
            memcpy(&x, data, 4); /// omitting range check because input is padded
            size_t len = 4 + size_t(x);
            requireRemainingBytes(len);
            data += len;
        }
    }

    void decode(size_t num_values, IColumn & col) override
    {
        if (converter->isTrivial())
        {
            /// Fast path for directly appending to ColumnString.
            auto & col_str = assert_cast<ColumnString &>(col);
            col_str.reserve(col_str.size() + num_values);
            for (size_t i = 0; i < num_values; ++i)
            {
                UInt32 x;
                memcpy(&x, data, 4); /// omitting range check because input is padded
                size_t len = 4 + size_t(x);
                requireRemainingBytes(len);
                col_str.insertData(data + 4, size_t(x));
                data += len;
            }
        }
        else
        {
            offsets.clear();
            offsets.reserve(num_values);
            /// We have extra 4 bytes *before* each string, but StringConverter expects
            /// separator_bytes *after* each string (for compatibility with ColumnString, which has
            /// extra '\0' byte after each string). So we offset the `data` start pointer to skip the
            /// first 4 bytes.
            const char * chars_start = data + 4;
            size_t offset = 0;
            for (size_t i = 0; i < num_values; ++i)
            {
                UInt32 x;
                memcpy(&x, data, 4); /// omitting range check because input is padded
                size_t len = 4 + size_t(x);
                requireRemainingBytes(len);
                offset += len;
                offsets.push_back(offset);
                data += len;
            }

            converter->convertColumn(std::span(chars_start, offset), offsets.data(), /*separator_bytes*/ 4, num_values, col);
        }
    }
};

struct DeltaBinaryPackedDecoder : public PageDecoder
{
    std::shared_ptr<FixedSizeConverter> converter;

    size_t values_per_block = 0;
    size_t miniblocks_per_block = 0;
    size_t total_values_remaining = 0;
    /// Do all arithmetic as unsigned to silently wrap on overflow (as DELTA_BINARY_PACKED wants).
    /// (Note: signed and unsigned integer addition are exactly the same operation, the only
    ///  difference is whether overflow is UB or not.)
    UInt64 current_value = 0;

    UInt64 min_delta = 0;
    const UInt8 * miniblock_bit_widths = nullptr;
    size_t miniblock_idx = 0; // within block
    size_t miniblock_values_remaining = 0;
    arrow::bit_util::BitReader bit_reader;

    PODArray<UInt64> temp_values;

    DeltaBinaryPackedDecoder(std::span<const char> data_, std::shared_ptr<FixedSizeConverter> converter_) : PageDecoder(data_), converter(std::move(converter_))
    {
        /// From https://parquet.apache.org/docs/file-format/data-pages/encodings/ :
        ///
        /// Delta encoding consists of a header followed by blocks of delta encoded values binary
        /// packed. Each block is made of miniblocks, each of them binary packed with its own bit width.
        ///
        /// The header is defined as follows:
        /// <block size in values> <number of miniblocks in a block> <total value count> <first value>
        ///  * the block size is a multiple of 128; it is stored as a ULEB128 int
        ///  * the miniblock count per block is a divisor of the block size such that their
        ///    quotient, the number of values in a miniblock, is a multiple of 32; it is stored as a
        ///    ULEB128 int
        ///  * the total value count is stored as a ULEB128 int
        ///  * the first value is stored as a zigzag ULEB128 int

        data = readVarUInt(values_per_block, data, end - data);
        data = readVarUInt(miniblocks_per_block, data, end - data);
        data = readVarUInt(total_values_remaining, data, end - data);
        data = readVarUInt(current_value, data, end - data);
        current_value = UInt64(decodeZigZag(current_value));

        if (values_per_block == 0 || values_per_block % 128 != 0 || miniblocks_per_block == 0 || values_per_block % miniblocks_per_block != 0 || values_per_block / miniblocks_per_block % 32 != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid DELTA_BINARY_PACKED header");

        /// Sanity-check total_values_remaining: each value takes at least one bit.
        /// This is useful to avoid allocating lots of memory if the input is corrupted.
        requireRemainingBytes((total_values_remaining + 7)/8);
    }

    void nextBlock()
    {
        /// From https://parquet.apache.org/docs/file-format/data-pages/encodings/ :
        ///
        /// Each block contains
        /// <min delta> <list of bitwidths of miniblocks> <miniblocks>
        ///  * the min delta is a zigzag ULEB128 int (we compute a minimum as we need positive
        ///    integers for bit packing)
        ///  * the bitwidth of each block is stored as a byte
        ///  * each miniblock is a list of bit packed ints according to the bit width stored at the
        ///    beginning of the block

        data = readVarUInt(min_delta, data, end - data);
        min_delta = UInt64(decodeZigZag(min_delta));
        requireRemainingBytes(miniblocks_per_block);
        miniblock_bit_widths = reinterpret_cast<const UInt8 *>(data);
        data += miniblocks_per_block;
        miniblock_idx = 0;
    }

    void nextMiniblock()
    {
        ++miniblock_idx;
        if (miniblock_idx >= miniblocks_per_block || miniblock_bit_widths == nullptr)
            nextBlock();
        chassert(miniblock_idx < miniblocks_per_block);

        miniblock_values_remaining = values_per_block / miniblocks_per_block;
        size_t bytes = (miniblock_values_remaining * miniblock_bit_widths[miniblock_idx] + 7) / 8;
        requireRemainingBytes(bytes);
        bit_reader.Reset(reinterpret_cast<const uint8_t *>(data), int(bytes));
        data += bytes;
    }

    void skip(size_t num_values) override
    {
        switch (converter->input_size)
        {
            case 32: decodeImpl<UInt32, true>(num_values, nullptr, [](UInt32 x) { return x; }); break;
            case 64: decodeImpl<UInt64, true>(num_values, nullptr, [](UInt64 x) { return x; }); break;
            default: chassert(false);
        }
    }

    void decode(size_t num_values, IColumn & col) override
    {
        bool direct = converter->isTrivial();
        char * to;
        if (direct)
        {
            auto to_span = col.insertRawUninitialized(num_values);
            chassert(to_span.size() == num_values * converter->input_size);
            to = to_span.data();
        }
        else
        {
            size_t num_u64s = converter->input_size == 32 ? (num_values + 1) / 2 : num_values;
            temp_values.resize(num_u64s);
            to = reinterpret_cast<char *>(temp_values.data());
        }

        switch (converter->input_size)
        {
            case 32: decodeImpl<UInt32, false>(num_values, to, [](UInt32 x) { return x; }); break;
            case 64: decodeImpl<UInt64, false>(num_values, to, [](UInt64 x) { return x; }); break;
            default: chassert(false);
        }

        if (!direct)
            converter->convertColumn(std::span(to, num_values * converter->input_size), num_values, col);
    }

    template <typename T, bool SKIP, typename F>
    void decodeImpl(size_t num_values, char * out_bytes, F func)
    {
        if (total_values_remaining < num_values)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Trying to read past total number of values in DELTA_BINARY_PACKED encoding");
        total_values_remaining -= num_values;

        T * out_values = reinterpret_cast<T *>(out_bytes);
        while (num_values)
        {
            if (!miniblock_values_remaining)
                nextMiniblock();

            size_t n = std::min(num_values, miniblock_values_remaining);
            int bits_per_delta = int(miniblock_bit_widths[miniblock_idx]);

            if constexpr (SKIP)
            {
                bool ok = bit_reader.Advance(bits_per_delta * n);
                chassert(ok);
            }
            else
            {
                /// Unpack deltas.
                int read_count = bit_reader.GetBatch(bits_per_delta, out_values, n);
                chassert(read_count == int(n));

                for (size_t i = 0; i < n; ++i)
                {
                    current_value += min_delta + UInt64(out_values[i]);
                    out_values[i] = func(T(current_value));
                }

                miniblock_values_remaining -= n;
                out_values += n;
            }
        }
    }
};

struct DeltaLengthByteArrayDecoder : public PageDecoder
{
    std::shared_ptr<StringConverter> converter;

    PaddedPODArray<UInt64> offsets;
    size_t idx = 0;

    DeltaLengthByteArrayDecoder(std::span<const char> data_, std::shared_ptr<StringConverter> converter_) : PageDecoder(data_), converter(std::move(converter_))
    {
        /// Decode all lengths in advance because otherwise there's no way to tell where chars start.
        DeltaBinaryPackedDecoder lengths_decoder(data_, nullptr);
        offsets.resize(lengths_decoder.total_values_remaining);
        size_t last_offset = 0;
        lengths_decoder.decodeImpl<UInt64, false>(
            lengths_decoder.total_values_remaining, reinterpret_cast<char *>(offsets.data()),
            [&](UInt64 len)
            {
                if (common::addOverflow(last_offset, len, last_offset))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Overflow in lengths in DELTA_LENGTH_BYTE_ARRAY data");
                return last_offset;
            });
        chassert(lengths_decoder.end == end);
        data = lengths_decoder.data;
        requireRemainingBytes(last_offset);
    }

    void skip(size_t num_values) override
    {
        if (num_values > offsets.size() - idx)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Too few values in page");
        idx += num_values;
    }

    void decode(size_t num_values, IColumn & col) override
    {
        if (num_values > offsets.size() - idx)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Too few values in page");
        converter->convertColumn(std::span(data, end - data), offsets.data() + idx, /*separator_bytes*/ 0, num_values, col);
        idx += num_values;
    }
};

struct DeltaByteArrayDecoder : public PageDecoder
{
    /// This encoding is applicable for both BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY.
    std::shared_ptr<StringConverter> string_converter;
    std::shared_ptr<FixedSizeConverter> fixed_size_converter;

    PaddedPODArray<UInt64> prefixes;
    PaddedPODArray<UInt64> suffixes;

    size_t idx = 0;
    String current_value;

    MutableColumnPtr temp_column;
    PaddedPODArray<char> temp_buffer;

    DeltaByteArrayDecoder(std::span<const char> data_, std::shared_ptr<StringConverter> string_converter_, std::shared_ptr<FixedSizeConverter> fixed_size_converter_) : PageDecoder(data_), string_converter(std::move(string_converter_)), fixed_size_converter(fixed_size_converter_)
    {
        for (auto * lengths : {&prefixes, &suffixes})
        {
            DeltaBinaryPackedDecoder decoder(std::span(data, end - data), nullptr);
            lengths->resize(decoder.total_values_remaining);
            decoder.decodeImpl<UInt64, false>(
                decoder.total_values_remaining, reinterpret_cast<char *>(lengths->data()),
                [&](UInt64 x) { return x; });
            data = decoder.data;
        }

        if (prefixes.size() != suffixes.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Value count mismatch in DELTA_BYTE_ARRAY headers");
    }

    void skip(size_t num_values) override
    {
        if (fixed_size_converter)
            decodeImpl<true, true>(num_values, nullptr, nullptr);
        else
            decodeImpl<true, false>(num_values, nullptr, nullptr);
    }

    void decode(size_t num_values, IColumn & col) override
    {
        if (fixed_size_converter)
        {
            bool direct = fixed_size_converter->isTrivial();
            std::span<char> to;
            if (direct)
            {
                to = col.insertRawUninitialized(num_values);
                chassert(to.size() == num_values * fixed_size_converter->input_size);
            }
            else
            {
                temp_buffer.resize(num_values * fixed_size_converter->input_size);
                to = std::span(temp_buffer.data(), temp_buffer.size());
            }

            decodeImpl<false, true>(num_values, nullptr, to.data());

            if (!direct)
                fixed_size_converter->convertColumn(to, num_values, col);
        }
        else
        {
            bool direct = string_converter->isTrivial();
            ColumnString * col_str;
            if (direct)
            {
                col_str = assert_cast<ColumnString *>(&col);
            }
            else
            {
                if (!temp_column)
                    temp_column = ColumnString::create();
                col_str = assert_cast<ColumnString *>(temp_column.get());
                col_str->getOffsets().clear();
                col_str->getChars().clear();
            }
            col_str->reserve(col_str->size() + num_values);

            decodeImpl<false, false>(num_values, col_str, nullptr);
            chassert(col_str->size() == num_values);

            if (!direct)
                string_converter->convertColumn(std::span(reinterpret_cast<char *>(col_str->getChars().data()), col_str->getChars().size()), col_str->getOffsets().data(), /*separator_bytes*/ 1, num_values, col);
        }
    }

    template <bool SKIP, bool FIXED_SIZE>
    void decodeImpl(size_t num_values, ColumnString * out_str, char * out_fixed_size)
    {
        if (num_values > prefixes.size() - idx)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Too few values in page");
        size_t fixed_size = FIXED_SIZE ? fixed_size_converter->input_size : 0;

        for (size_t i = 0; i < num_values; ++i)
        {
            if (prefixes[idx] > current_value.size())
                throw Exception(ErrorCodes::INCORRECT_DATA, "DELTA_BYTE_ARRAY too long");
            current_value.resize(prefixes[idx]);
            requireRemainingBytes(suffixes[idx]);
            current_value.append(data, suffixes[idx]);
            data += suffixes[idx];
            ++idx;

            if constexpr (FIXED_SIZE)
            {
                if (current_value.size() != fixed_size)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected fixed string size in DELTA_BYTE_ARRAY");

                if constexpr(!SKIP)
                {
                    memcpy(out_fixed_size, current_value.data(), fixed_size);
                    out_fixed_size += fixed_size;
                }
            }
            else if constexpr (!SKIP)
            {
                out_str->insertData(current_value.data(), current_value.size());
            }
        }
    }
};

struct ByteStreamSplitDecoder : public PageDecoder
{
    std::shared_ptr<FixedSizeConverter> converter;
    size_t stream_size = 0;

    PaddedPODArray<char> temp_buffer;

    ByteStreamSplitDecoder(std::span<const char> data_, std::shared_ptr<FixedSizeConverter> converter_) : PageDecoder(data_), converter(std::move(converter_))
    {
        if (data_.size() % converter->input_size != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BYTE_STREAM_SPLIT data size not divisible by element size");
        stream_size = data_.size() / converter->input_size;
        /// Point [data, end) to the first stream.
        end = data + stream_size;
    }

    void skip(size_t num_values) override
    {
        requireRemainingBytes(num_values);
        data += num_values;
    }

    void decode(size_t num_values, IColumn & col) override
    {
        size_t num_streams = converter->input_size;

        bool direct = converter->isTrivial();
        char * to = nullptr;
        if (direct)
        {
            auto span = col.insertRawUninitialized(num_values);
            chassert(span.size() == num_values * num_streams);
            to = span.data();
        }
        else
        {
            temp_buffer.resize(num_values * num_streams);
            to = temp_buffer.data();
        }

        requireRemainingBytes(num_values);

        size_t i = 0;
        while (i < num_values)
        {
            if (num_values - i >= 8)
            {
                /// Slightly faster code path that reads 8 bytes at once.
                /// Arrow has ByteStreamSplitDecode with various fancy simd implementations, maybe
                /// we should reuse that instead.
                for (size_t stream = 0; stream < num_streams; ++stream)
                {
                    UInt64 x = unalignedLoad<UInt64>(&data[i + stream * stream_size]);
                    to[(i + 0) * num_streams + stream] = char(UInt8(x >> 0));
                    to[(i + 1) * num_streams + stream] = char(UInt8(x >>  8));
                    to[(i + 2) * num_streams + stream] = char(UInt8(x >> 16));
                    to[(i + 3) * num_streams + stream] = char(UInt8(x >> 24));
                    to[(i + 4) * num_streams + stream] = char(UInt8(x >> 32));
                    to[(i + 5) * num_streams + stream] = char(UInt8(x >> 40));
                    to[(i + 6) * num_streams + stream] = char(UInt8(x >> 48));
                    to[(i + 7) * num_streams + stream] = char(UInt8(x >> 56));
                }
                i += 8;
            }
            else
            {
                for (size_t stream = 0; stream < num_streams; ++stream)
                    to[i * num_streams + stream] = data[i + stream * stream_size];
                i += 1;
            }
        }
        data += num_values;

        if (!direct)
            converter->convertColumn(std::span(to, num_values * num_streams), num_values, col);
    }
};


bool PageDecoderInfo::canReadDirectlyIntoColumn(parq::Encoding::type encoding, size_t num_values, IColumn & col, std::span<char> & out) const
{
    if (encoding == parq::Encoding::PLAIN && fixed_size_converter && fixed_size_converter->isTrivial())
    {
        chassert(col.sizeOfValueIfFixed() == fixed_size_converter->input_size);
        out = col.insertRawUninitialized(num_values);
        return true;
    }
    return false;
}

void PageDecoderInfo::decodeField(std::span<const char> data, bool is_max, Field & out) const
{
    if (!allow_stats)
        return;

    if (fixed_size_converter)
        fixed_size_converter->convertField(data, is_max, out);
    else if (string_converter)
        string_converter->convertField(data, is_max, out);
    else
        chassert(false);
}

std::unique_ptr<PageDecoder> PageDecoderInfo::makeDecoder(
    parq::Encoding::type encoding, std::span<const char> data) const
{
    switch (encoding)
    {
        case parq::Encoding::PLAIN:
            switch (physical_type)
            {
                case parq::Type::INT32:
                case parq::Type::INT64:
                case parq::Type::INT96:
                case parq::Type::FLOAT:
                case parq::Type::DOUBLE:
                case parq::Type::FIXED_LEN_BYTE_ARRAY:
                    return std::make_unique<PlainFixedSizeDecoder>(data, fixed_size_converter);
                case parq::Type::BYTE_ARRAY:
                    return std::make_unique<PlainStringDecoder>(data, string_converter);
                case parq::Type::BOOLEAN:
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BOOLEAN is not implemented");
            }
        /// TODO [parquet]: RLE for BOOLEAN
        case parq::Encoding::RLE: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RLE encoding is not implemented");
        case parq::Encoding::BIT_PACKED: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected BIT_PACKED encoding for values");
        case parq::Encoding::DELTA_BINARY_PACKED:
            switch (physical_type)
            {
                case parq::Type::INT32:
                case parq::Type::INT64:
                    return std::make_unique<DeltaBinaryPackedDecoder>(data, fixed_size_converter);
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected encoding DELTA_BINARY_PACKED for type {}", thriftToString(physical_type));
            }
        case parq::Encoding::DELTA_LENGTH_BYTE_ARRAY:
            switch (physical_type)
            {
                case parq::Type::BYTE_ARRAY:
                    return std::make_unique<DeltaLengthByteArrayDecoder>(data, string_converter);
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected encoding DELTA_LENGTH_BYTE_ARRAY for type {}", thriftToString(physical_type));
            }
        case parq::Encoding::DELTA_BYTE_ARRAY:
            switch (physical_type)
            {
                case parq::Type::BYTE_ARRAY:
                case parq::Type::FIXED_LEN_BYTE_ARRAY:
                    return std::make_unique<DeltaByteArrayDecoder>(data, string_converter, fixed_size_converter);
                default:
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected encoding DELTA_BYTE_ARRAY for type {}", thriftToString(physical_type));
            }
        case parq::Encoding::BYTE_STREAM_SPLIT:
            if (!fixed_size_converter)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected encoding BYTE_STREAM_SPLIT for type {}", thriftToString(physical_type));
            return std::make_unique<ByteStreamSplitDecoder>(data, fixed_size_converter);
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected page encoding: {}", thriftToString(encoding));
    }
}

void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out)
{
    if (max == 0)
        return;
    out.resize(num_values);
    switch (encoding)
    {
        case parq::Encoding::RLE:
            BitPackedRLEDecoder<UInt8>(data, size_t(max) + 1, /*has_header_byte=*/ false).decodeArray(num_values, out);
            break;
        case parq::Encoding::BIT_PACKED:
            /// TODO [parquet]: BIT_PACKED levels
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BIT_PACKED levels not implemented");
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected repetition/definition levels encoding: {}", thriftToString(encoding));
    }
}

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data)
{
    switch (encoding)
    {
        case parq::Encoding::RLE_DICTIONARY: return std::make_unique<BitPackedRLEDecoder<UInt32>>(data, dictionary_size, /*has_header_byte=*/ true);
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected dictionary indices encoding: {}", thriftToString(encoding));
    }
}


void Dictionary::reset()
{
    mode = Mode::Uninitialized;
    data = {};
    col.reset();
    offsets.clear();
    offsets.shrink_to_fit();
    decompressed_buf.clear();
    decompressed_buf.shrink_to_fit();
}

bool Dictionary::isInitialized() const
{
    return mode != Mode::Uninitialized;
}

double Dictionary::getAverageValueSize() const
{
    switch (mode)
    {
        case Mode::FixedSize: return value_size;
        case Mode::StringPlain: return std::max(0., double(data.size()) / std::max(offsets.size(), 1ul) - 4);
        case Mode::Column: return double(col->byteSize()) / std::max(col->size(), 1ul);
        case Mode::Uninitialized: break;
    }
    chassert(false);
    return 0;
}

void Dictionary::decode(parq::Encoding::type encoding, const PageDecoderInfo & info, size_t num_values, std::span<const char> data_, const IDataType & raw_decoded_type)
{
    chassert(mode == Mode::Uninitialized);
    chassert(info.fixed_size_converter || info.string_converter);
    if (encoding == parq::Encoding::PLAIN_DICTIONARY)
        encoding = parq::Encoding::PLAIN;
    count = num_values;
    bool decode_generic = false;
    if (encoding != parq::Encoding::PLAIN)
    {
        /// Parquet supports only PLAIN encoding for dictionaries, but we support any encoding
        /// because it's easy (we need decode_generic code path anyway for ShortInt and Boolean).
        decode_generic = true;
    }
    else if (info.fixed_size_converter && info.fixed_size_converter->isTrivial())
    {
        /// No decoding needed, we'll be just copying bytes from dictionary page directly.
        mode = Mode::FixedSize;
        value_size = info.fixed_size_converter->input_size;
        data = data_;
    }
    else if (info.string_converter && info.string_converter->isTrivial())
    {
        mode = Mode::StringPlain;
        data = data_;

        offsets.resize(num_values);
        const char * ptr = data.data();
        const char * end = data.data() + data.size();
        for (size_t i = 0; i < num_values; ++i)
        {
            UInt32 x;
            memcpy(&x, ptr, 4); /// omitting range check because input is padded
            size_t len = 4 + size_t(x);
            if (len > size_t(end - ptr))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Encoded string is out of bounds");
            ptr += len;
            offsets[i] = ptr - data.data();
        }
    }
    else
    {
        /// Values need to be converted, e.g. Decimal encoded as BYTE_ARRAY, or Int8 encoded as INT32.
        decode_generic = true;
    }

    if (decode_generic)
    {
        auto decoder = info.makeDecoder(encoding, data_);
        auto c = raw_decoded_type.createColumn();
        c->reserve(num_values);
        decoder->decode(num_values, *c);
        col = std::move(c);

        if (col->isFixedAndContiguous())
        {
            mode = Mode::FixedSize;
            value_size = col->sizeOfValueIfFixed();
            std::string_view s = col->getRawData();
            data = std::span(s.data(), s.size());
            chassert(data.size() == col->size() * value_size);
        }
        else
        {
            mode = Mode::Column;
        }
    }

    chassert(mode != Mode::Uninitialized);

    if (mode == Mode::FixedSize && data.size() != count * value_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect dictionary page size: {} != {} * {}", data.size(), count, value_size);
}

template<size_t S>
static void indexImpl(const PaddedPODArray<UInt32> & indexes, std::span<const char> data, std::span<char> to)
{
    size_t size = indexes.size();
    for (size_t i = 0; i < size; ++i)
        memcpy(to.data() + i * S, data.data() + indexes[i] * S, S);
}

void Dictionary::index(const ColumnUInt32 & indexes_col, IColumn & out)
{
    const PaddedPODArray<UInt32> & indexes = indexes_col.getData();
    switch (mode)
    {
        case Mode::FixedSize:
        {
            auto to = out.insertRawUninitialized(indexes.size());
            chassert(to.size() == value_size * indexes.size());
            /// Short variable-length memcpy is very slow compared to a simple mov, so we dispatch
            /// to specialized loops covering basic int types.
            switch (value_size)
            {
                case 1: indexImpl<1>(indexes, data, to); break;
                case 2: indexImpl<2>(indexes, data, to); break;
                case 3: indexImpl<3>(indexes, data, to); break;
                case 4: indexImpl<4>(indexes, data, to); break;
                case 8: indexImpl<8>(indexes, data, to); break;
                case 16: indexImpl<16>(indexes, data, to); break;
                default:
                    for (size_t i = 0; i < indexes.size(); ++i)
                        memcpy(to.data() + i * value_size, data.data() + indexes[i] * value_size, value_size);
            }
            break;
        }
        case Mode::StringPlain:
        {
            auto & c = assert_cast<ColumnString &>(out);
            c.reserve(c.size() + indexes.size());
            for (UInt32 idx : indexes)
            {
                size_t start = offsets[size_t(idx) - 1] + 4; // offsets[-1] is ok because of padding
                size_t len = offsets[idx] - start;
                /// TODO [parquet]: Try optimizing short memcpy by taking advantage of padding (maybe memcpySmall.h helps). Also in PlainStringDecoder.
                c.insertData(data.data() + start, len);
            }
            break;
        }
        case Mode::Column:
        {
            ColumnPtr temp = col->index(indexes_col, /*limit*/ 0);
            out.insertRangeFrom(*temp, 0, indexes.size());
            break;
        }
        case Mode::Uninitialized: chassert(false);
    }
}

void memcpyIntoColumn(const char * data, size_t num_values, size_t value_size, IColumn & col)
{
    auto to = col.insertRawUninitialized(num_values);
    chassert(to.size() == num_values * value_size);
    memcpy(to.data(), data, to.size());
}

template <typename From, typename To>
static void convertIntColumnImpl(const char * from_bytes, char * to_bytes, size_t num_values)
{
    To * to = reinterpret_cast<To *>(to_bytes);
    for (size_t i = 0; i < num_values; ++i)
    {
        /// (Can't reinterpret_cast<const From *>(from_bytes) because pointer may be unaligned).
        From x;
        memcpy(&x, from_bytes + i * sizeof(From), sizeof(From));
        to[i] = static_cast<To>(x);
    }
}

void IntConverter::convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const
{
    if (truncate_output.has_value())
    {
        chassert(input_size == 4);
        auto to = col.insertRawUninitialized(num_values);
        chassert(to.size() == num_values * truncate_output.value());
        /// Signedness doesn't matter here, we just need to copy the first 1 or 2 bytes of each
        /// group of 4 bytes.
        if (*truncate_output == 1)
            convertIntColumnImpl<UInt32, UInt8>(data.data(), to.data(), num_values);
        else if (*truncate_output == 2)
            convertIntColumnImpl<UInt32, UInt16>(data.data(), to.data(), num_values);
        else
            chassert(false);
    }
    else
    {
        memcpyIntoColumn(data.data(), num_values, input_size, col);
    }
}

void IntConverter::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    if (data.size() != input_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected value size in int statistics: {} != {}", data.size(), input_size);

    UInt64 val = 0;
    switch (input_size)
    {
        case 4: val = unalignedLoad<UInt32>(data.data()); break;
        case 8: val = unalignedLoad<UInt64>(data.data()); break;
        default: chassert(false);
    }

    /// Sign-extend.
    if (input_signed && input_size < 8 && (val >> (input_size * 8 - 1)) != 0)
        val |= 0 - (1ul << (input_size * 8));

    /// Check for overflow in signed <-> unsigned conversion.
    if (input_signed && !field_signed && Int64(val) < 0)
        return;
    if (!input_signed && field_signed && val > UInt64(INT64_MAX))
        return;

    if (field_ipv4)
    {
        if (val <= UInt64(UINT32_MAX))
            out = Field(IPv4(UInt32(val)));
    }
    else if (field_timestamp_from_millis)
    {
        /// Convert milliseconds to seconds, with the same rounding as when casting from
        /// DateTime64(3) to DateTime.
        /// (Shouldn't we round max towards positive infinity and min towards negative infinity?
        ///  No, that's not required because the values in the column will also be converted to
        ///  seconds by castColumn, with the same rounding. So the rounded min/max stats
        ///  accurately represent min/max among the rounded values.)
        val /= 1000;
        if (val <= UInt64(UINT32_MAX))
            out = Field(val);
    }
    else if (field_decimal_scale.has_value())
    {
        switch (input_size)
        {
            case 4: out = DecimalField<Decimal32>(Int32(val), *field_decimal_scale); break;
            case 8: out = DecimalField<Decimal64>(val, *field_decimal_scale); break;
            default: chassert(false);
        }
    }
    else if (field_signed)
        out = Field(Int64(val));
    else
        out = Field(val);
}

template<typename T>
void FloatConverter<T>::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    if (data.size() != input_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected value size in float statistics: {} != {}", data.size(), input_size);

    T x;
    memcpy(&x, data.data(), sizeof(x));

    /// parquet.thrift says:
    /// (*) Because the sorting order is not specified properly for floating
    ///     point values (relations vs. total ordering) the following
    ///     compatibility rules should be applied when reading statistics:
    ///     - If the min is a NaN, it should be ignored.
    ///     - If the max is a NaN, it should be ignored.
    ///     - If the min is +0, the row group may contain -0 values as well.
    ///     - If the max is -0, the row group may contain +0 values as well.
    ///     - When looking for NaN values, min and max should be ignored.
    ///
    /// We reject NaNs, but don't do anything about +-0 because normal Field comparisons should
    /// already treat them as equal.
    if (!std::isnan(x))
        out = Field(x);
}

template struct FloatConverter<float>;
template struct FloatConverter<double>;

void FixedStringConverter::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    if (data.size() != input_size)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected size of fixed string in statistics: {} != {}", data.size(), input_size);

    out = Field(String(data.data(), data.size()));
}

void TrivialStringConverter::convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const
{
    auto & col_str = assert_cast<ColumnString &>(col);
    col_str.reserve(col_str.size() + num_values);
    chassert(chars.size() >= offsets[num_values - 1]);
    col_str.getChars().reserve(col_str.getChars().size() + (offsets[num_values - 1] - offsets[-1]) - separator_bytes * num_values + num_values);
    for (size_t i = 0; i < num_values; ++i)
        col_str.insertData(chars.data() + offsets[i - 1], offsets[i] - offsets[i - 1] - separator_bytes);
}

void TrivialStringConverter::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    out = Field(String(data.data(), data.size()));
}

/// Reverse bytes. Like std::byteswap, but works for Int128 and Int256 too.
template <typename T>
T byteswap(T x)
{
    if constexpr (sizeof(T) <= 8)
        x = std::byteswap(x);
    else
    {
        x.items[0] = std::byteswap(x.items[0]);
        x.items[1] = std::byteswap(x.items[1]);
        if constexpr (sizeof(T) == 16)
        {
            std::swap(x.items[0], x.items[1]);
        }
        else
        {
            static_assert(sizeof(T) == 32);
            x.items[2] = std::byteswap(x.items[2]);
            x.items[3] = std::byteswap(x.items[3]);
            std::swap(x.items[0], x.items[3]);
            std::swap(x.items[1], x.items[2]);
        }
    }
    return x;
}

template <typename T>
BigEndianHelper<T>::BigEndianHelper(size_t input_size)
{
    chassert(sizeof(T) >= input_size);
    value_offset = sizeof(T) - input_size;
    value_mask = (~T(0)) << (8 * value_offset);

    if (value_offset != 0)
    {
        sign_mask = T(1) << (8 * input_size - 1);
        sign_extension_mask = (~T(0)) << (8 * input_size);
    }
}

template <typename T>
void BigEndianHelper<T>::fixupValue(T & x) const
{
    x &= value_mask; // mask off the garbage bytes that we've read out of bounds

    /// Convert to little-endian.
    x = byteswap(x);

    /// Sign-extend.
    if (x & sign_mask)
        x |= sign_extension_mask;
}

template <typename T>
T BigEndianHelper<T>::convertPaddedValue(const char * data) const
{
    /// We take advantage of input padding and do fixed-size memcpy of size sizeof(T) instead
    /// of variable-size memcpy of size input_size. Variable-size memcpy is slow.
    T x;
    memcpy(&x, data - value_offset, sizeof(T));
    fixupValue(x);
    return x;
}

template <typename T>
T BigEndianHelper<T>::convertUnpaddedValue(std::span<const char> data) const
{
    chassert(data.size() <= sizeof(T));
    T x = 0;
    memcpy(reinterpret_cast<char *>(&x) + value_offset, data.data(), data.size());
    fixupValue(x);
    return x;
}

template struct BigEndianHelper<Int32>;
template struct BigEndianHelper<Int64>;
template struct BigEndianHelper<Int128>;
template struct BigEndianHelper<Int256>;

template <typename T>
void BigEndianDecimalFixedSizeConverter<T>::convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const
{
    const char * from_bytes = data.data();
    auto to_bytes = col.insertRawUninitialized(num_values);
    chassert(to_bytes.size() == num_values * sizeof(T));
    T * to = reinterpret_cast<T *>(to_bytes.data());
    for (size_t i = 0; i < num_values; ++i)
    {
        to[i] = helper.convertPaddedValue(from_bytes);
        from_bytes += input_size;
    }
}

template <typename T>
void BigEndianDecimalFixedSizeConverter<T>::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    if (data.size() != input_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected value size in Decimal statistics: {} != {}", data.size(), input_size);

    T x = helper.convertUnpaddedValue(data);
    out = DecimalField(Decimal<T>(x), scale);
}

template struct BigEndianDecimalFixedSizeConverter<Int32>;
template struct BigEndianDecimalFixedSizeConverter<Int64>;
template struct BigEndianDecimalFixedSizeConverter<Int128>;
template struct BigEndianDecimalFixedSizeConverter<Int256>;

template <typename T>
void BigEndianDecimalStringConverter<T>::convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const
{
    auto to_bytes = col.insertRawUninitialized(num_values);
    chassert(to_bytes.size() == num_values * sizeof(T));
    T * to = reinterpret_cast<T *>(to_bytes.data());

    for (size_t i = 0; i < num_values; ++i)
    {
        const char * data = chars.data() + offsets[i - 1];
        size_t size = offsets[i] - offsets[i - 1] - separator_bytes;
        if (size > sizeof(T))
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpectedly wide Decimal value: {} > {} bytes", size, sizeof(T));

        to[i] = BigEndianHelper<T>(size).convertPaddedValue(data);
    }
}

template <typename T>
void BigEndianDecimalStringConverter<T>::convertField(std::span<const char> data, bool /*is_max*/, Field & out) const
{
    if (data.size() > sizeof(T))
        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpectedly wide value in Decimal statistics: {} > {} bytes", data.size(), sizeof(T));

    T x = BigEndianHelper<T>(data.size()).convertUnpaddedValue(data);
    out = DecimalField(Decimal<T>(x), scale);
}

template struct BigEndianDecimalStringConverter<Int32>;
template struct BigEndianDecimalStringConverter<Int64>;
template struct BigEndianDecimalStringConverter<Int128>;
template struct BigEndianDecimalStringConverter<Int256>;

}
