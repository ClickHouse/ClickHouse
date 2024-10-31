#include "Processors/Formats/Impl/Parquet/Write.h"
#include "Processors/Formats/Impl/Parquet/ThriftUtil.h"
#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <arrow/util/rle_encoding.h>
#include <lz4.h>
#include <Columns/MaskOperations.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <IO/WriteHelpers.h>
#include <Common/config_version.h>
#include <Common/formatReadable.h>

#if USE_SNAPPY
#include <snappy.h>
#endif

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

namespace parq = parquet::format;

namespace
{

template <typename T, typename SourceType>
struct StatisticsNumeric
{
    T min = std::numeric_limits<T>::has_infinity
        ? std::numeric_limits<T>::infinity() : std::numeric_limits<T>::max();
    T max = std::numeric_limits<T>::has_infinity
        ? -std::numeric_limits<T>::infinity() : std::numeric_limits<T>::lowest();

    void add(SourceType x)
    {
        min = std::min(min, static_cast<T>(x));
        max = std::max(max, static_cast<T>(x));
    }

    void merge(const StatisticsNumeric & s)
    {
        min = std::min(min, s.min);
        max = std::max(max, s.max);
    }

    void clear() { *this = {}; }

    parq::Statistics get(const WriteOptions &)
    {
        parq::Statistics s;
        s.__isset.min_value = s.__isset.max_value = true;
        s.min_value.resize(sizeof(T));
        s.max_value.resize(sizeof(T));
        memcpy(s.min_value.data(), &min, sizeof(T));
        memcpy(s.max_value.data(), &max, sizeof(T));

        if constexpr (std::is_signed_v<T>)
        {
            s.__set_min(s.min_value);
            s.__set_max(s.max_value);
        }
        return s;
    }
};

struct StatisticsFixedStringRef
{
    size_t fixed_string_size = UINT64_MAX;
    const uint8_t * min = nullptr;
    const uint8_t * max = nullptr;

    void add(parquet::FixedLenByteArray a)
    {
        chassert(fixed_string_size != UINT64_MAX);
        addMin(a.ptr);
        addMax(a.ptr);
    }

    void merge(const StatisticsFixedStringRef & s)
    {
        chassert(fixed_string_size == UINT64_MAX || fixed_string_size == s.fixed_string_size);
        fixed_string_size = s.fixed_string_size;
        if (s.min == nullptr)
            return;
        addMin(s.min);
        addMax(s.max);
    }

    void clear() { min = max = nullptr; }

    parq::Statistics get(const WriteOptions & options) const
    {
        parq::Statistics s;
        if (min == nullptr || fixed_string_size > options.max_statistics_size)
            return s;
        s.__set_min_value(std::string(reinterpret_cast<const char *>(min), fixed_string_size));
        s.__set_max_value(std::string(reinterpret_cast<const char *>(max), fixed_string_size));
        return s;
    }

    void addMin(const uint8_t * p)
    {
        if (min == nullptr || memcmp(p, min, fixed_string_size) < 0)
            min = p;
    }
    void addMax(const uint8_t * p)
    {
        if (max == nullptr || memcmp(p, max, fixed_string_size) > 0)
            max = p;
    }
};

template<size_t S>
struct StatisticsFixedStringCopy
{
    bool empty = true;
    std::array<uint8_t, S> min {};
    std::array<uint8_t, S> max {};

    void add(parquet::FixedLenByteArray a)
    {
        addMin(a.ptr);
        addMax(a.ptr);
        empty = false;
    }

    void merge(const StatisticsFixedStringCopy<S> & s)
    {
        if (s.empty)
            return;
        addMin(s.min.data());
        addMax(s.max.data());
        empty = false;
    }

    void clear() { empty = true; }

    parq::Statistics get(const WriteOptions &) const
    {
        parq::Statistics s;
        if (empty)
            return s;
        s.__set_min_value(std::string(reinterpret_cast<const char *>(min.data()), S));
        s.__set_max_value(std::string(reinterpret_cast<const char *>(max.data()), S));
        return s;
    }

    void addMin(const uint8_t * p)
    {
        if (empty || memcmp(p, min.data(), S) < 0)
            memcpy(min.data(), p, S);
    }
    void addMax(const uint8_t * p)
    {
        if (empty || memcmp(p, max.data(), S) > 0)
            memcpy(max.data(), p, S);
    }
};

struct StatisticsStringRef
{
    parquet::ByteArray min;
    parquet::ByteArray max;

    void add(parquet::ByteArray x)
    {
        addMin(x);
        addMax(x);
    }

    void merge(const StatisticsStringRef & s)
    {
        if (s.min.ptr == nullptr)
            return;
        addMin(s.min);
        addMax(s.max);
    }

    void clear() { *this = {}; }

    parq::Statistics get(const WriteOptions & options) const
    {
        parq::Statistics s;
        if (min.ptr == nullptr)
            return s;
        if (static_cast<size_t>(min.len) <= options.max_statistics_size)
            s.__set_min_value(std::string(reinterpret_cast<const char *>(min.ptr), static_cast<size_t>(min.len)));
        if (static_cast<size_t>(max.len) <= options.max_statistics_size)
            s.__set_max_value(std::string(reinterpret_cast<const char *>(max.ptr), static_cast<size_t>(max.len)));
        return s;
    }

    void addMin(parquet::ByteArray x)
    {
        if (min.ptr == nullptr || compare(x, min) < 0)
            min = x;
    }

    void addMax(parquet::ByteArray x)
    {
        if (max.ptr == nullptr || compare(x, max) > 0)
            max = x;
    }

    static int compare(parquet::ByteArray a, parquet::ByteArray b)
    {
        int t = memcmp(a.ptr, b.ptr, std::min(a.len, b.len));
        if (t != 0)
            return t;
        return a.len - b.len;
    }
};

/// The column usually needs to be converted to one of Parquet physical types, e.g. UInt16 -> Int32
/// or [element of ColumnString] -> std::string_view.
/// We do this conversion in small batches rather than all at once, just before encoding the batch,
/// in hopes of getting better performance through cache locality.
/// The Converter* structs below are responsible for that.
/// When conversion is not needed, getBatch() will just return pointer into original data.

template <typename Col, typename To, typename MinMaxType = typename std::conditional_t<
        std::is_signed_v<typename Col::Container::value_type>,
        To,
        typename std::make_unsigned_t<To>>>
struct ConverterNumeric
{
    using Statistics = StatisticsNumeric<MinMaxType, To>;

    const Col & column;
    PODArray<To> buf;

    explicit ConverterNumeric(const ColumnPtr & c) : column(assert_cast<const Col &>(*c)) {}

    const To * getBatch(size_t offset, size_t count)
    {
        if constexpr (sizeof(*column.getData().data()) == sizeof(To))
            return reinterpret_cast<const To *>(column.getData().data() + offset);
        else
        {
            buf.resize(count);
            for (size_t i = 0; i < count; ++i)
                buf[i] = static_cast<To>(column.getData()[offset + i]); // NOLINT
            return buf.data();
        }
    }
};

struct ConverterDateTime64WithMultiplier
{
    using Statistics = StatisticsNumeric<Int64, Int64>;

    using Col = ColumnDecimal<DateTime64>;
    const Col & column;
    Int64 multiplier;
    PODArray<Int64> buf;

    ConverterDateTime64WithMultiplier(const ColumnPtr & c, Int64 multiplier_) : column(assert_cast<const Col &>(*c)), multiplier(multiplier_) {}

    const Int64 * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            /// Not checking overflow because DateTime64 values should already be in the range where
            /// they fit in Int64 at any allowed scale (i.e. up to nanoseconds).
            buf[i] = column.getData()[offset + i].value * multiplier;
        return buf.data();
    }
};

struct ConverterString
{
    using Statistics = StatisticsStringRef;

    const ColumnString & column;
    PODArray<parquet::ByteArray> buf;

    explicit ConverterString(const ColumnPtr & c) : column(assert_cast<const ColumnString &>(*c)) {}

    const parquet::ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
        {
            StringRef s = column.getDataAt(offset + i);
            buf[i] = parquet::ByteArray(static_cast<UInt32>(s.size), reinterpret_cast<const uint8_t *>(s.data));
        }
        return buf.data();
    }
};

struct ConverterFixedString
{
    using Statistics = StatisticsFixedStringRef;

    const ColumnFixedString & column;
    PODArray<parquet::FixedLenByteArray> buf;

    explicit ConverterFixedString(const ColumnPtr & c) : column(assert_cast<const ColumnFixedString &>(*c)) {}

    const parquet::FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i].ptr = reinterpret_cast<const uint8_t *>(column.getChars().data() + (offset + i) * column.getN());
        return buf.data();
    }

    size_t fixedStringSize() { return column.getN(); }
};

struct ConverterFixedStringAsString
{
    using Statistics = StatisticsStringRef;

    const ColumnFixedString & column;
    PODArray<parquet::ByteArray> buf;

    explicit ConverterFixedStringAsString(const ColumnPtr & c) : column(assert_cast<const ColumnFixedString &>(*c)) {}

    const parquet::ByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i] = parquet::ByteArray(static_cast<UInt32>(column.getN()), reinterpret_cast<const uint8_t *>(column.getChars().data() + (offset + i) * column.getN()));
        return buf.data();
    }
};

template <typename T>
struct ConverterNumberAsFixedString
{
    /// Calculate min/max statistics for little-endian fixed strings, not numbers, because parquet
    /// doesn't know it's numbers.
    using Statistics = StatisticsFixedStringCopy<sizeof(T)>;

    const ColumnVector<T> & column;
    PODArray<parquet::FixedLenByteArray> buf;

    explicit ConverterNumberAsFixedString(const ColumnPtr & c) : column(assert_cast<const ColumnVector<T> &>(*c)) {}

    const parquet::FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        buf.resize(count);
        for (size_t i = 0; i < count; ++i)
            buf[i].ptr = reinterpret_cast<const uint8_t *>(column.getData().data() + offset + i);
        return buf.data();
    }

    size_t fixedStringSize() { return sizeof(T); }
};

/// Like ConverterNumberAsFixedString, but converts to big-endian. Because that's the byte order
/// Parquet uses for decimal types and literally nothing else, for some reason.
template <typename T>
struct ConverterDecimal
{
    using Statistics = StatisticsFixedStringCopy<sizeof(T)>;

    const ColumnDecimal<T> & column;
    PODArray<uint8_t> data_buf;
    PODArray<parquet::FixedLenByteArray> ptr_buf;

    explicit ConverterDecimal(const ColumnPtr & c) : column(assert_cast<const ColumnDecimal<T> &>(*c)) {}

    const parquet::FixedLenByteArray * getBatch(size_t offset, size_t count)
    {
        data_buf.resize(count * sizeof(T));
        ptr_buf.resize(count);
        memcpy(data_buf.data(), reinterpret_cast<const char *>(column.getData().data() + offset), count * sizeof(T));
        for (size_t i = 0; i < count; ++i)
        {
            std::reverse(data_buf.data() + i * sizeof(T), data_buf.data() + (i + 1) * sizeof(T));
            ptr_buf[i].ptr = data_buf.data() + i * sizeof(T);
        }
        return ptr_buf.data();
    }

    size_t fixedStringSize() { return sizeof(T); }
};

/// Returns either `source` or `scratch`.
PODArray<char> & compress(PODArray<char> & source, PODArray<char> & scratch, CompressionMethod method)
{
    /// We could use wrapWriteBufferWithCompressionMethod() for everything, but I worry about the
    /// overhead of creating a bunch of WriteBuffers on each page (thousands of values).
    switch (method)
    {
        case CompressionMethod::None:
            return source;

        case CompressionMethod::Lz4:
        {
            #pragma clang diagnostic push
            #pragma clang diagnostic ignored "-Wold-style-cast"

            size_t max_dest_size = LZ4_COMPRESSBOUND(source.size());

            #pragma clang diagnostic pop

            if (max_dest_size > std::numeric_limits<int>::max())
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", ReadableSize(source.size()));

            scratch.resize(max_dest_size);

            int compressed_size = LZ4_compress_default(
                source.data(),
                scratch.data(),
                static_cast<int>(source.size()),
                static_cast<int>(max_dest_size));

            scratch.resize(static_cast<size_t>(compressed_size));
            return scratch;
        }

#if USE_SNAPPY
        case CompressionMethod::Snappy:
        {
            size_t max_dest_size = snappy::MaxCompressedLength(source.size());

            if (max_dest_size > std::numeric_limits<int>::max())
                throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress column of size {}", formatReadableSizeWithBinarySuffix(source.size()));

            scratch.resize(max_dest_size);

            size_t compressed_size;
            snappy::RawCompress(source.data(), source.size(), scratch.data(), &compressed_size);

            scratch.resize(compressed_size);
            return scratch;
        }
#endif

        default:
        {
            auto dest_buf = std::make_unique<WriteBufferFromVector<PODArray<char>>>(scratch);
            auto compressed_buf = wrapWriteBufferWithCompressionMethod(
                std::move(dest_buf),
                method,
                /*level*/ 3,
                /*zstd_window_log*/ 0,
                source.size(),
                /*existing_memory*/ source.data());
            chassert(compressed_buf->position() == source.data());
            chassert(compressed_buf->available() == source.size());
            compressed_buf->position() += source.size();
            compressed_buf->finalize();
            return scratch;
        }
    }
}

void encodeRepDefLevelsRLE(const UInt8 * data, size_t size, UInt8 max_level, PODArray<char> & out)
{
    using arrow::util::RleEncoder;

    chassert(max_level > 0);
    size_t offset = out.size();
    size_t prefix_size = sizeof(Int32);

    int bit_width = bitScanReverse(max_level) + 1;
    int max_rle_size = RleEncoder::MaxBufferSize(bit_width, static_cast<int>(size)) +
                       RleEncoder::MinBufferSize(bit_width);

    out.resize(offset + prefix_size + max_rle_size);

    RleEncoder encoder(reinterpret_cast<uint8_t *>(out.data() + offset + prefix_size), max_rle_size, bit_width);
    for (size_t i = 0; i < size; ++i)
        encoder.Put(data[i]);
    encoder.Flush();
    Int32 len = encoder.len();

    memcpy(out.data() + offset, &len, prefix_size);
    out.resize(offset + prefix_size + len);
}

void addToEncodingsUsed(ColumnChunkWriteState & s, parq::Encoding::type e)
{
    if (!std::count(s.column_chunk.meta_data.encodings.begin(), s.column_chunk.meta_data.encodings.end(), e))
        s.column_chunk.meta_data.encodings.push_back(e);
}

void writePage(const parq::PageHeader & header, const PODArray<char> & compressed, ColumnChunkWriteState & s, WriteBuffer & out)
{
    size_t header_size = serializeThriftStruct(header, out);
    out.write(compressed.data(), compressed.size());

    /// Remember first data page and first dictionary page.
    if (header.__isset.data_page_header && s.column_chunk.meta_data.data_page_offset == -1)
        s.column_chunk.meta_data.__set_data_page_offset(s.column_chunk.meta_data.total_compressed_size);
    if (header.__isset.dictionary_page_header && !s.column_chunk.meta_data.__isset.dictionary_page_offset)
        s.column_chunk.meta_data.__set_dictionary_page_offset(s.column_chunk.meta_data.total_compressed_size);

    s.column_chunk.meta_data.total_uncompressed_size += header.uncompressed_page_size + header_size;
    s.column_chunk.meta_data.total_compressed_size += header.compressed_page_size + header_size;
}

template <typename ParquetDType, typename Converter>
void writeColumnImpl(
    ColumnChunkWriteState & s, const WriteOptions & options, WriteBuffer & out, Converter && converter)
{
    size_t num_values = s.max_def > 0 ? s.def.size() : s.primitive_column->size();
    auto encoding = options.encoding;

    typename Converter::Statistics page_statistics;
    typename Converter::Statistics total_statistics;

    bool use_dictionary = options.use_dictionary_encoding && !s.is_bool;

    std::optional<parquet::ColumnDescriptor> fixed_string_descr;
    if constexpr (std::is_same_v<ParquetDType, parquet::FLBAType>)
    {
        /// This just communicates one number to MakeTypedEncoder(): the fixed string length.
        fixed_string_descr.emplace(parquet::schema::PrimitiveNode::Make(
            "", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            parquet::ConvertedType::NONE, static_cast<int>(converter.fixedStringSize())), 0, 0);

        if constexpr (std::is_same_v<typename Converter::Statistics, StatisticsFixedStringRef>)
            page_statistics.fixed_string_size = converter.fixedStringSize();
    }

    /// Could use an arena here (by passing a custom MemoryPool), to reuse memory across pages.
    /// Alternatively, we could avoid using arrow's dictionary encoding code and leverage
    /// ColumnLowCardinality instead. It would work basically the same way as what this function
    /// currently does: add values to the ColumnRowCardinality (instead of `encoder`) in batches,
    /// checking dictionary size after each batch. That might be faster.
    auto encoder = parquet::MakeTypedEncoder<ParquetDType>(
        // ignored if using dictionary
        static_cast<parquet::Encoding::type>(encoding),
        use_dictionary, fixed_string_descr ? &*fixed_string_descr : nullptr);

    struct PageData
    {
        parq::PageHeader header;
        PODArray<char> data;
        size_t first_row_index = 0;
    };
    std::vector<PageData> dict_encoded_pages; // can't write them out until we have full dictionary

    /// Reused across pages to reduce number of allocations and improve locality.
    PODArray<char> encoded;
    PODArray<char> compressed_maybe;

    /// Start of current page.
    size_t def_offset = 0; // index in def and rep
    size_t data_offset = 0; // index in primitive_column

    auto flush_page = [&](size_t def_count, size_t data_count)
    {
        encoded.clear();

        /// Concatenate encoded rep, def, and data.

        if (s.max_rep > 0)
            encodeRepDefLevelsRLE(s.rep.data() + def_offset, def_count, s.max_rep, encoded);
        if (s.max_def > 0)
            encodeRepDefLevelsRLE(s.def.data() + def_offset, def_count, s.max_def, encoded);

        std::shared_ptr<parquet::Buffer> values = encoder->FlushValues(); // resets it for next page

        encoded.resize(encoded.size() + values->size());
        memcpy(encoded.data() + encoded.size() - values->size(), values->data(), values->size());
        values.reset();

        if (encoded.size() > INT32_MAX)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Uncompressed page is too big: {}", encoded.size());

        size_t uncompressed_size = encoded.size();
        auto & compressed = compress(encoded, compressed_maybe, s.compression);

        if (compressed.size() > INT32_MAX)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Compressed page is too big: {}", compressed.size());

        parq::PageHeader header;
        header.__set_type(parq::PageType::DATA_PAGE);
        header.__set_uncompressed_page_size(static_cast<int>(uncompressed_size));
        header.__set_compressed_page_size(static_cast<int>(compressed.size()));
        header.__isset.data_page_header = true;
        auto & d = header.data_page_header;
        d.__set_num_values(static_cast<Int32>(def_count));
        d.__set_encoding(use_dictionary ? parq::Encoding::RLE_DICTIONARY : encoding);
        d.__set_definition_level_encoding(parq::Encoding::RLE);
        d.__set_repetition_level_encoding(parq::Encoding::RLE);
        /// We could also put checksum in `header.crc`, but apparently no one uses it:
        /// https://issues.apache.org/jira/browse/PARQUET-594

        if (options.write_page_statistics)
        {
            d.__set_statistics(page_statistics.get(options));
            bool all_null_page = data_count == 0;
            if (all_null_page)
            {
                s.column_index.min_values.push_back("");
                s.column_index.max_values.push_back("");
            }
            else
            {
                s.column_index.min_values.push_back(d.statistics.min_value);
                s.column_index.max_values.push_back(d.statistics.max_value);
            }
            bool has_null_count = s.max_def == 1 && s.max_rep == 0;
            if (has_null_count)
                d.statistics.__set_null_count(static_cast<Int64>(def_count - data_count));
            s.column_index.__isset.null_counts = has_null_count;
            if (has_null_count)
            {
                s.column_index.__isset.null_counts = true;
                s.column_index.null_counts.emplace_back(d.statistics.null_count);
            }
            else
            {
                if (s.column_index.__isset.null_counts)
                    s.column_index.null_counts.emplace_back(0);
            }
            s.column_index.null_pages.push_back(all_null_page);
        }

        total_statistics.merge(page_statistics);
        page_statistics.clear();

        if (use_dictionary)
        {
            dict_encoded_pages.push_back({.header = std::move(header), .data = {}, .first_row_index = def_offset});
            std::swap(dict_encoded_pages.back().data, compressed);
        }
        else
        {
            parquet::format::PageLocation location;
            location.offset = out.count();
            writePage(header, compressed, s, out);
            location.compressed_page_size = static_cast<int32_t>(out.count() - location.offset);
            location.first_row_index = def_offset;
            s.offset_index.page_locations.emplace_back(location);
        }
        def_offset += def_count;
        data_offset += data_count;
    };

    auto flush_dict = [&] -> bool
    {
        auto * dict_encoder = dynamic_cast<parquet::DictEncoder<ParquetDType> *>(encoder.get());
        int dict_size = dict_encoder->dict_encoded_size();

        encoded.resize(static_cast<size_t>(dict_size));
        dict_encoder->WriteDict(reinterpret_cast<uint8_t *>(encoded.data()));

        auto & compressed = compress(encoded, compressed_maybe, s.compression);

        if (compressed.size() > INT32_MAX)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Compressed dictionary page is too big: {}", compressed.size());

        parq::PageHeader header;
        header.__set_type(parq::PageType::DICTIONARY_PAGE);
        header.__set_uncompressed_page_size(dict_size);
        header.__set_compressed_page_size(static_cast<int>(compressed.size()));
        header.__isset.dictionary_page_header = true;
        header.dictionary_page_header.__set_num_values(dict_encoder->num_entries());
        header.dictionary_page_header.__set_encoding(parq::Encoding::PLAIN);

        writePage(header, compressed, s, out);

        for (auto & p : dict_encoded_pages)
        {
            parquet::format::PageLocation location;
            location.offset = out.count();
            writePage(p.header, p.data, s, out);
            location.compressed_page_size = static_cast<int32_t>(out.count() - location.offset);
            location.first_row_index = p.first_row_index;
            s.offset_index.page_locations.emplace_back(location);
        }

        dict_encoded_pages.clear();
        encoder.reset();

        return true;
    };

    auto is_dict_too_big = [&] {
        auto * dict_encoder = dynamic_cast<parquet::DictEncoder<ParquetDType> *>(encoder.get());
        int dict_size = dict_encoder->dict_encoded_size();
        return static_cast<size_t>(dict_size) >= options.dictionary_size_limit;
    };

    while (def_offset < num_values)
    {
        /// Pick enough data for a page.
        size_t next_def_offset = def_offset;
        size_t next_data_offset = data_offset;
        while (true)
        {
            /// Bite off a batch of defs and corresponding data values.
            size_t def_count = std::min(options.write_batch_size, num_values - next_def_offset);
            size_t data_count = 0;
            if (s.max_def == 0)
                data_count = def_count;
            else
                for (size_t i = 0; i < def_count; ++i)
                    data_count += s.def[next_def_offset + i] == s.max_def;

            /// Encode the data (but not the levels yet), so that we can estimate its encoded size.
            const typename ParquetDType::c_type * converted = converter.getBatch(next_data_offset, data_count);

            if (options.write_page_statistics || options.write_column_chunk_statistics)
/// Workaround for clang bug: https://github.com/llvm/llvm-project/issues/63630
#ifdef MEMORY_SANITIZER
#pragma clang loop vectorize(disable)
#endif
                for (size_t i = 0; i < data_count; ++i)
                    page_statistics.add(converted[i]);

            encoder->Put(converted, static_cast<int>(data_count));

            next_def_offset += def_count;
            next_data_offset += data_count;

            if (use_dictionary && is_dict_too_big())
            {
                /// Fallback to non-dictionary encoding.
                ///
                /// Discard encoded data and start over.
                /// This is different from what arrow does: arrow writes out the dictionary-encoded
                /// data, then uses non-dictionary encoding for later pages.
                /// Starting over seems better: it produces slightly smaller files (I saw 1-4%) in
                /// exchange for slight decrease in speed (I saw < 5%). This seems like a good
                /// trade because encoding speed is much less important than decoding (as evidenced
                /// by arrow not supporting parallel encoding, even though it's easy to support).

                def_offset = 0;
                data_offset = 0;
                dict_encoded_pages.clear();

                //clear column_index
                s.column_index = parquet::format::ColumnIndex();
                use_dictionary = false;

#ifndef NDEBUG
                /// Arrow's DictEncoderImpl destructor asserts that FlushValues() was called, so we
                /// call it even though we don't need its output.
                encoder->FlushValues();
#endif

                encoder = parquet::MakeTypedEncoder<ParquetDType>(
                    static_cast<parquet::Encoding::type>(encoding), /* use_dictionary */ false,
                    fixed_string_descr ? &*fixed_string_descr : nullptr);
                break;
            }

            if (next_def_offset == num_values ||
                static_cast<size_t>(encoder->EstimatedDataEncodedSize()) >= options.data_page_size)
            {
                flush_page(next_def_offset - def_offset, next_data_offset - data_offset);
                break;
            }
        }
    }

    if (use_dictionary)
        flush_dict();

    chassert(data_offset == s.primitive_column->size());

    if (options.write_column_chunk_statistics)
    {
        s.column_chunk.meta_data.__set_statistics(total_statistics.get(options));

        if (s.max_def == 1 && s.max_rep == 0)
            s.column_chunk.meta_data.statistics.__set_null_count(static_cast<Int64>(def_offset - data_offset));
    }

    /// Report which encodings we've used.
    if (s.max_rep > 0 || s.max_def > 0)
        addToEncodingsUsed(s, parq::Encoding::RLE); // levels
    if (use_dictionary)
    {
        addToEncodingsUsed(s, parq::Encoding::PLAIN); // dictionary itself
        addToEncodingsUsed(s, parq::Encoding::RLE_DICTIONARY); // ids
    }
    else
    {
        addToEncodingsUsed(s, encoding);
    }
}

}

void writeColumnChunkBody(ColumnChunkWriteState & s, const WriteOptions & options, WriteBuffer & out)
{
    s.column_chunk.meta_data.__set_num_values(s.max_def > 0 ? s.def.size() : s.primitive_column->size());

    /// We'll be updating these as we go.
    s.column_chunk.meta_data.__set_encodings({});
    s.column_chunk.meta_data.__set_total_compressed_size(0);
    s.column_chunk.meta_data.__set_total_uncompressed_size(0);
    s.column_chunk.meta_data.__set_data_page_offset(-1);

    s.primitive_column = s.primitive_column->convertToFullColumnIfLowCardinality();

    switch (s.primitive_column->getDataType())
    {
        /// Numeric conversion to Int32 or Int64.
        #define N(source_type, parquet_dtype) \
            writeColumnImpl<parquet::parquet_dtype>(s, options, out, \
                ConverterNumeric<ColumnVector<source_type>, parquet::parquet_dtype::c_type>( \
                    s.primitive_column))

        case TypeIndex::UInt8:
            if (s.is_bool)
                writeColumnImpl<parquet::BooleanType>(s, options, out,
                    ConverterNumeric<ColumnVector<UInt8>, bool, bool>(s.primitive_column));
            else
                N(UInt8, Int32Type);
         break;
        case TypeIndex::UInt16 : N(UInt16, Int32Type); break;
        case TypeIndex::UInt32 : N(UInt32, Int32Type); break;
        case TypeIndex::UInt64 : N(UInt64, Int64Type); break;
        case TypeIndex::Int8   : N(Int8,   Int32Type); break;
        case TypeIndex::Int16  : N(Int16,  Int32Type); break;
        case TypeIndex::Int32  : N(Int32,  Int32Type); break;
        case TypeIndex::Int64  : N(Int64,  Int64Type); break;

        case TypeIndex::Enum8:      N(Int8,   Int32Type); break;
        case TypeIndex::Enum16:     N(Int16,  Int32Type); break;
        case TypeIndex::Date:       N(UInt16, Int32Type); break;
        case TypeIndex::Date32:     N(Int32,  Int32Type); break;
        case TypeIndex::DateTime:   N(UInt32, Int32Type); break;

        #undef N

        case TypeIndex::Float32:
            writeColumnImpl<parquet::FloatType>(
                s, options, out, ConverterNumeric<ColumnVector<Float32>, Float32, Float32>(
                    s.primitive_column));
            break;

        case TypeIndex::Float64:
            writeColumnImpl<parquet::DoubleType>(
                s, options, out, ConverterNumeric<ColumnVector<Float64>, Float64, Float64>(
                    s.primitive_column));
            break;

        case TypeIndex::DateTime64:
            if (s.datetime64_multiplier == 1)
                writeColumnImpl<parquet::Int64Type>(
                    s, options, out, ConverterNumeric<ColumnDecimal<DateTime64>, Int64, Int64>(
                        s.primitive_column));
            else
                writeColumnImpl<parquet::Int64Type>(
                    s, options, out, ConverterDateTime64WithMultiplier(
                        s.primitive_column, s.datetime64_multiplier));
            break;

        case TypeIndex::IPv4:
            writeColumnImpl<parquet::Int32Type>(
                s, options, out, ConverterNumeric<ColumnVector<IPv4>, Int32, UInt32>(
                    s.primitive_column));
            break;

        case TypeIndex::String:
            writeColumnImpl<parquet::ByteArrayType>(
                s, options, out, ConverterString(s.primitive_column));
            break;

        case TypeIndex::FixedString:
            if (options.output_fixed_string_as_fixed_byte_array)
                writeColumnImpl<parquet::FLBAType>(
                s, options, out, ConverterFixedString(s.primitive_column));
            else
                writeColumnImpl<parquet::ByteArrayType>(
                s, options, out, ConverterFixedStringAsString(s.primitive_column));
            break;

        #define F(source_type) \
            writeColumnImpl<parquet::FLBAType>( \
                s, options, out, ConverterNumberAsFixedString<source_type>(s.primitive_column))
        case TypeIndex::UInt128: F(UInt128); break;
        case TypeIndex::UInt256: F(UInt256); break;
        case TypeIndex::Int128:  F(Int128); break;
        case TypeIndex::Int256:  F(Int256); break;
        case TypeIndex::IPv6:    F(IPv6); break;
        #undef F

        #define D(source_type) \
            writeColumnImpl<parquet::FLBAType>( \
                s, options, out, ConverterDecimal<source_type>(s.primitive_column))
        case TypeIndex::Decimal32:  D(Decimal32); break;
        case TypeIndex::Decimal64:  D(Decimal64); break;
        case TypeIndex::Decimal128: D(Decimal128); break;
        case TypeIndex::Decimal256: D(Decimal256); break;
        #undef D

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column type: {}", s.primitive_column->getFamilyName());
    }

    /// Free some memory.
    s.primitive_column = {};
    s.def = {};
    s.rep = {};
}

void writeFileHeader(WriteBuffer & out)
{
    /// Write the magic bytes. We're a wizard now.
    out.write("PAR1", 4);
}

parq::ColumnChunk finalizeColumnChunkAndWriteFooter(
    size_t offset_in_file, ColumnChunkWriteState s, const WriteOptions &, WriteBuffer & out)
{
    if (s.column_chunk.meta_data.data_page_offset != -1)
        s.column_chunk.meta_data.data_page_offset += offset_in_file;
    if (s.column_chunk.meta_data.__isset.dictionary_page_offset)
        s.column_chunk.meta_data.dictionary_page_offset += offset_in_file;
    s.column_chunk.file_offset = offset_in_file + s.column_chunk.meta_data.total_compressed_size;

    serializeThriftStruct(s.column_chunk, out);

    return s.column_chunk;
}

parq::RowGroup makeRowGroup(std::vector<parq::ColumnChunk> column_chunks, size_t num_rows)
{
    parq::RowGroup r;
    r.__set_num_rows(num_rows);
    r.__set_columns(column_chunks);
    r.__set_total_compressed_size(0);
    for (auto & c : r.columns)
    {
        r.total_byte_size += c.meta_data.total_uncompressed_size;
        r.total_compressed_size += c.meta_data.total_compressed_size;
    }
    if (!r.columns.empty())
    {
        auto & m = r.columns[0].meta_data;
        r.__set_file_offset(m.__isset.dictionary_page_offset ? m.dictionary_page_offset : m.data_page_offset);
    }
    return r;
}

void writePageIndex(
    const std::vector<std::vector<parquet::format::ColumnIndex>> & column_indexes,
    const std::vector<std::vector<parquet::format::OffsetIndex>> & offset_indexes,
    std::vector<parq::RowGroup> & row_groups,
    WriteBuffer & out,
    size_t base_offset)
{
    chassert(row_groups.size() == column_indexes.size() && row_groups.size() == offset_indexes.size());
    auto num_row_groups = row_groups.size();
    // write column index
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        const auto & current_group_column_index = column_indexes.at(i);
        chassert(row_groups.at(i).columns.size() == current_group_column_index.size());
        auto & row_group = row_groups.at(i);
        for (size_t j = 0; j < row_groups.at(i).columns.size(); ++j)
        {
            auto & column = row_group.columns.at(j);
            int64_t column_index_offset = static_cast<int64_t>(out.count() - base_offset);
            int32_t column_index_length = static_cast<int32_t>(serializeThriftStruct(current_group_column_index.at(j), out));
            column.__isset.column_index_offset = true;
            column.column_index_offset = column_index_offset;
            column.__isset.column_index_length = true;
            column.column_index_length = column_index_length;
        }
    }

    // write offset index
    for (size_t i = 0; i < num_row_groups; ++i)
    {
        const auto & current_group_offset_index = offset_indexes.at(i);
        chassert(row_groups.at(i).columns.size() == current_group_offset_index.size());
        for (size_t j = 0; j < row_groups.at(i).columns.size(); ++j)
        {
            int64_t offset_index_offset = out.count() - base_offset;
            int32_t offset_index_length = static_cast<int32_t>(serializeThriftStruct(current_group_offset_index.at(j), out));
            row_groups.at(i).columns.at(j).__isset.offset_index_offset = true;
            row_groups.at(i).columns.at(j).offset_index_offset = offset_index_offset;
            row_groups.at(i).columns.at(j).__isset.offset_index_length = true;
            row_groups.at(i).columns.at(j).offset_index_length = offset_index_length;
        }
    }
}

void writeFileFooter(std::vector<parq::RowGroup> row_groups, SchemaElements schema, const WriteOptions & options, WriteBuffer & out)
{
    parq::FileMetaData meta;
    meta.version = 2;
    meta.schema = std::move(schema);
    meta.row_groups = std::move(row_groups);
    for (auto & r : meta.row_groups)
        meta.num_rows += r.num_rows;
    meta.__set_created_by(std::string(VERSION_NAME) + " " + VERSION_DESCRIBE);

    if (options.write_page_statistics || options.write_column_chunk_statistics)
    {
        meta.__set_column_orders({});
        for (auto & s : meta.schema)
            if (!s.__isset.num_children)
                meta.column_orders.emplace_back();
        for (auto & c : meta.column_orders)
            c.__set_TYPE_ORDER({});
    }

    size_t footer_size = serializeThriftStruct(meta, out);

    if (footer_size > INT32_MAX)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Parquet file metadata too big: {}", footer_size);

    writeIntBinary(static_cast<int>(footer_size), out);
    out.write("PAR1", 4);
}

}
