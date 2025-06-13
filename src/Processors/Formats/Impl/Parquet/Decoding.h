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

    void reset();

    bool isInitialized() const;

    double getAverageValueSize() const;

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
        /// Representation in IColumn is the same as in parquet PLAIN encoding. Can just memcpy.
        /// E.g. INT64 -> Int64/UInt64/Decimal64, FIXED_LEN_BYTE_ARRAY -> FixedString.
        FixedSize,
        String,
        /// Convert Int32 to UInt8 or UInt16.
        ShortInt,
        /// Signed big-endian integer. Input values have a fixed size <= 32 bytes (not necessarily
        /// power of two). Decoder reverses bytes and pads+sign-extends to value_size.
        BigEndian,
        Boolean,
    };

    Kind kind = Kind::FixedSize;
    size_t value_size = 0; // if FixedSize, ShortInt, or BigEndian

    /// If BigEndian. input_value_size <= value_size.
    /// Read values of size input_value_size and pad them with zeroes to size value_size.
    size_t input_value_size = 0;

    /// True if we can decompress the whole page directly into IColumn's memory.
    bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, std::span<char> & out) const;

    /// [data, end) must be padded, i.e. have at least PADDING_FOR_SIMD bytes of readable memory
    /// before `data` and after `end`.
    std::unique_ptr<PageDecoder> makeDecoder(parq::Encoding::type, std::span<const char> data) const;
};

/// Decodes values min/max values from parquet statistics into Fields suitable for
/// KeyCondition::checkInHyperrectangle.
///
/// We should be extra careful about type conversions to make sure the comparator used by parquet
/// writer when producing min/max is exactly equivalent to the comparator that KeyCondition will use
/// when comparing our decoded+converted Field values.
/// E.g. if the parquet file has column `x String`, but we read it as `file(..., 'x Int64')`, we
/// silently auto-cast data from String to Int64 (by parsing number as text); but we can't do the
/// same for min/max values because String min/max is not the same as Int64 min/max (e.g. "10" < "9").
/// So we have a small allowlist of type conversions (dispatched in SchemaConverter), and the
/// conversion is done together with decoding, by StatsDecoder.
/// In particular we don't call something like convertFieldToType, working through all the cases
/// would be a nightmare.
struct StatsDecoder
{
    /// Decodes min/max value from parquet Statistics or ColumnIndex.
    /// Called separately for min (with is_max=false) and max (is_max=true).
    /// The caller pre-fills `out` with corresponding +-infinity, so this function can just leave
    /// `out` unchanged if the value can't be decoded.
    virtual void decode(const String & in, bool is_max, Field & out) const = 0;

    virtual ~StatsDecoder() = default;
};

/// Input physical type: INT32 or INT64.
/// Output Field type: Int64, UInt64, or IPv4.
struct IntStatsDecoder : public StatsDecoder
{
    size_t input_value_size = 0;
    bool input_signed = false;

    bool output_signed = false;
    bool output_ipv4 = false;

    void decode(const String &, bool, Field &) const override;
};

/// Input physical type: INT32, INT64, BYTE_ARRAY, or FIXED_LEN_BYTE_ARRAY.
/// Output Field type: Decimal32, Decimal64, Decimal128, or Decimal256.
/// (This struct has information for converting between different scales and sizes, but conversions
///  are not implemented. `decode` will only produce result if input and output have the same size and scale.)
struct DecimalStatsDecoder : public StatsDecoder
{
    size_t input_value_size = 0;
    UInt32 input_scale = 0;
    bool input_big_endian = false;

    size_t output_value_size = 0;
    size_t output_scale = 0;
    bool output_int = 0;

    void decode(const String &, bool, Field &) const override;
};

/// FLOAT -> Float32, or DOUBLE -> Float64.
struct FloatStatsDecoder : public StatsDecoder
{
    size_t value_size = 0;

    void decode(const String &, bool, Field &) const override;
};

/// Input physical type: BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY.
/// Output Field type: String.
struct StringStatsDecoder : public StatsDecoder
{
    void decode(const String &, bool, Field &) const override;
};


void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out);

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data);

}
