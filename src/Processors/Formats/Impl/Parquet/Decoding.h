#pragma once

#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

#include <IO/VarInt.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_DECOMPRESS;
    extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
}

namespace DB::Parquet
{

//TODO: move implementations to .cpp, including templates

struct PageDecoderInfo;

struct Dictionary
{
    enum class Mode
    {
        Uninitialized,

        FixedSize,
        /// `data` strings with 4-byte length prefixes, `offsets` points to the end of each string.
        StringPlain,
    };

    Mode mode = Mode::Uninitialized;
    size_t value_size = 0; // if fixed_size
    PaddedPODArray<UInt32> offsets; // if !fixed_size
    size_t count = 0;

    /// Points into `col`, or `decompressed_buf`, or into Prefetcher's memory (kept alive by dictionary_page_prefetch).
    std::span<const char> data;

    PaddedPODArray<char> decompressed_buf;
    ColumnPtr col;

    void reset()
    {
        mode = Mode::Uninitialized;
        data = {};
        col.reset();
        offsets.clear();
        offsets.shrink_to_fit();
        decompressed_buf.clear();
        decompressed_buf.shrink_to_fit();
    }

    bool isInitialized() const
    {
        return mode != Mode::Uninitialized;
    }

    double getAverageValueSize() const
    {
        switch (mode)
        {
            case Mode::FixedSize: return value_size;
            case Mode::StringPlain: return std::max(0., double(data.size()) / std::max(offsets.size(), 1ul) - 4);
            case Mode::Uninitialized: break;
        }
        chassert(false);
        return 0;
    }

    void index(const PaddedPODArray<UInt32> & indexes, IColumn & out);

    void decode(parq::Encoding::type encoding, const PageDecoderInfo & info, size_t num_values, std::span<const char> data_, const IDataType & raw_decoded_type);
};

struct PageDecoder
{
    virtual void skip(size_t num_values) = 0;
    virtual void decode(size_t num_values, IColumn &) = 0;

    explicit PageDecoder(std::span<const char> data_) : data(data_.data()), end(data_.data() + data_.size()) {}
    virtual ~PageDecoder() = default;

    const char * data = nullptr;
    const char * end = nullptr;

    void requireRemainingBytes(size_t n)
    {
        if (size_t(end - data) < n)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpectd end of page data");
    }
};

/// We choose PageDecoder implementation in two steps:
///  1. during schema conversion we create PageDecoderInfo (this should be in schema conversion because
///    that's where column type and data type are decided, and they should match the decoder type);
///  2. after reading page header, the Encoding becomes known, and we create a PageDecoder.
struct PageDecoderInfo
{
    enum class Kind
    {
        FixedSize,
        String,
        ShortInt, // convert Int32 to UInt8 or UInt16
        Boolean,
    };

    Kind kind = Kind::FixedSize;
    size_t value_size = 0; // if FixedSize or ShortInt

    /// True if we can decompress the whole page directly into IColumn's memory.
    bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, std::span<char> & out) const;

    /// [data, end) must be padded, i.e. have at least PADDING_FOR_SIMD bytes of readable memory
    /// before `data` and after `end`.
    std::unique_ptr<PageDecoder> makeDecoder(parq::Encoding::type, std::span<const char> data) const;
};

void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out);

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data);


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
        static_assert(sizeof(T) <= 4, "");
        chassert(limit <= std::numeric_limits<T>::max());

        if (has_header_byte)
        {
            requireRemainingBytes(1);
            bit_width = *data;
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
        /// TODO: May make sense to have specialized version of this loop for bit_width=1, which is
        ///       very common as def levels for nullables.
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
                    std::fill(out, out + n, val);
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
    size_t value_size;

    explicit PlainFixedSizeDecoder(std::span<const char> data_, size_t value_size_) : PageDecoder(data_), value_size(value_size_) {}

    void skip(size_t num_values) override
    {
        size_t bytes = num_values * value_size;
        requireRemainingBytes(bytes);
        data += bytes;
    }

    void decode(size_t num_values, IColumn & col) override
    {
        const char * from = data;
        skip(num_values);
        auto to = col.insertRawUninitialized(num_values);
        chassert(to.size() == size_t(data - from));
        memcpy(to.data(), from, to.size());
    }
};

template <typename From, typename To>
struct PlainCastDecoder : public PageDecoder
{
    using PageDecoder::PageDecoder;

    void skip(size_t num_values) override
    {
        size_t bytes = num_values * sizeof(From);
        requireRemainingBytes(bytes);
        data += bytes;
    }

    void decode(size_t num_values, IColumn & col) override
    {
        const char * from_bytes = data;
        skip(num_values);
        /// Note that `col` element type may be different from To, e.g. different signedness, so
        /// we can't assert_cast to ColumnVector<To>, and need to be careful about aliasing rules.
        /// (We use To = UInt16 for Int16 columns to reduce the number of template instantiations,
        ///  since a To = Int16 instantiation should compile to identical machine code anyway.)
        auto to_bytes = col.insertRawUninitialized(num_values);
        chassert(to_bytes.size() == num_values * sizeof(To));
        To * to = reinterpret_cast<To *>(to_bytes.data());
        for (size_t i = 0; i < num_values; ++i)
        {
            From x;
            memcpy(&x, from_bytes + i * sizeof(From), sizeof(From));
            to[i] = static_cast<To>(x);
        }
    }
};

struct PlainStringDecoder : public PageDecoder
{
    using PageDecoder::PageDecoder;

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
};

}
