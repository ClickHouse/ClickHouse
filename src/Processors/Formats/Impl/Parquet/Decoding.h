#pragma once

#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

struct PageDecoderInfo;

struct Dictionary
{
    enum class Mode
    {
        Uninitialized,

        /// `data` is just an array of values, value_size bytes each.
        FixedSize,
        /// `data` contains strings with 4-byte length prefixes, `offsets` points to the end of each string.
        StringPlain,
        /// `col` has the values. Use col->index to extract requested values.
        /// This is slow because IColumn::index creates a new column, but we want to append to an
        /// existing one, so we end up copying values twice.
        /// Maybe we should refactor IColumn::index to append to a column; then we can remove FixedSize mode.
        Column,
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
    void index(const ColumnUInt32 & indexes_col, IColumn & out);
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

    void requireRemainingBytes(size_t n) const
    {
        if (size_t(end - data) < n)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of page data");
    }
};

void memcpyIntoColumn(const char * data, size_t num_values, size_t value_size, IColumn & col);

struct FixedSizeConverter
{
    /// Encoded value size in bytes. E.g. 4 for INT32, or string length for FIXED_LEN_BYTE_ARRAY.
    size_t input_size = 0;

    /// If true, we can just memcpy into IColumn::insertRawUninitialized instead of calling
    /// convertColumn.
    virtual bool isTrivial() const { return false; }

    virtual void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const
    {
        chassert(isTrivial());
        memcpyIntoColumn(data.data(), num_values, input_size, col);
    }

    /// Decodes min/max value from parquet Statistics or ColumnIndex.
    /// Called separately for min (with is_max=false) and max (is_max=true).
    /// The caller pre-fills `out` with corresponding +-infinity, so this function can just leave
    /// `out` unchanged if the value can't be decoded.
    /// Called only if PageDecoderInfo::allow_stats is true, which SchemaConverter sets only after
    /// carefully checking that min/max stats are usable in this situation (e.g. no type conversion
    /// is needed).
    virtual void convertField(std::span<const char> /*data*/, bool /*is_max*/, Field &) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FixedSizeConverter subclass doesn't support decoding Field");
    }

    virtual ~FixedSizeConverter() = default;
};

struct StringConverter
{
    /// If true, the output is ColumnString, and no special conversion is needed.
    virtual bool isTrivial() const { return false; }

    /// i-th string is range [offsets[i-1], offsets[i]-separator_bytes) in `chars`.
    /// `offsets[-1]` must be valid and is not necessarily 0.
    /// Does no range checks, the caller must ensure that `offsets` are valid and `chars` are long enough.
    virtual void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn &) const = 0;
    virtual void convertField(std::span<const char> /*data*/, bool /*is_max*/, Field &) const
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StringConverter subclass doesn't support decoding Field");
    }

    virtual ~StringConverter() = default;
};

/// We choose PageDecoder implementation in two steps:
///  1. during schema conversion we create PageDecoderInfo (this should be in schema conversion
///     because that's where column type and data type are decided, and they should match the
///     decoder type);
///  2. after reading page header, the Encoding becomes known, and we create a PageDecoder.
struct PageDecoderInfo
{
    parq::Type::type physical_type;

    /// Postprocessing of decoded values. Exactly one of these is set, depending on physical_type.
    std::shared_ptr<FixedSizeConverter> fixed_size_converter;
    std::shared_ptr<StringConverter> string_converter;

    /// True if we can parse and use min/max from parquet Statistics.
    /// False if type hint requires a nontrivial cast that's either not monotonic or not supported.
    ///
    /// E.g. if the parquet file has column `x String`, but we read it as `file(..., 'x Int64')`, we
    /// silently auto-cast data from String to Int64 (by parsing number as text); but we can't do the
    /// same for min/max stats because String min/max is not the same as Int64 min/max (e.g. "10" < "9").
    /// So we have a small allowlist of type conversions (dispatched in SchemaConverter), and the
    /// conversion is done together with decoding, by FixedSizeConverter/StringConverter.
    /// In particular we don't call something like convertFieldToType because working through all
    /// the cases would be a nightmare.
    bool allow_stats = false;

    /// True if we can decompress the whole page directly into IColumn's memory.
    bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, std::span<char> & out) const;

    /// [data, end) must be padded, i.e. have at least PADDING_FOR_SIMD bytes of readable memory
    /// before `data` and after `end`.
    std::unique_ptr<PageDecoder> makeDecoder(parq::Encoding::type, std::span<const char> data) const;

    /// Decode a min/max value from Statistics.
    /// If not supported or allow_stats is false, leaves `out` unchanged.
    void decodeField(std::span<const char> data, bool is_max, Field & out) const;
};


/// Input physical type: BOOLEAN, INT32, or INT64.
/// input_size in {1, 4, 8}.
/// Output column type: [U]Int{8,16,32,64}.
/// Output Field type: [U]Int64, IPv4, or Decimal{32,64}.
struct IntConverter : public FixedSizeConverter
{
    bool input_signed = true;

    /// Cast to an integer of this size (in bytes). If nullopt, leave input_size.
    /// Only allowed if input_size is 4.
    std::optional<size_t> output_size;

    /// These determine the type of Field produced by convertField (when parsing min/max stats).
    /// No effect on convertColumn - it just copies bytes and doesn't care what they mean.
    std::optional<UInt32> field_decimal_scale; // Decimal{32,64}(scale)
    bool field_ipv4 = false; // IPv4
    bool field_timestamp_from_millis = false; // convert DateTime64(3) to DateTime
    bool field_signed = true; // Int64, otherwise UInt64
    /// If not Ignore, it's a Date32 column and we should range-check it.
    FormatSettings::DateTimeOverflowBehavior date_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Ignore;

    bool isTrivial() const override
    {
        return !output_size.has_value() && date_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Ignore;
    }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

/// Input physical type: FLOAT or DOUBLE.
/// Output column type: Float{32,64}.
/// Output Field type: Float{32,64}.
template <typename T>
struct FloatConverter : public FixedSizeConverter
{
    FloatConverter() { input_size = sizeof(T); }

    bool isTrivial() const override { return true; }

    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

extern template struct FloatConverter<float>;
extern template struct FloatConverter<double>;

/// FIXED_LEN_BYTE_ARRAY[2] as float16 (not to be confused with bfloat16) -> Float32.
struct Float16Converter : public FixedSizeConverter
{
    Float16Converter() { input_size = 2; }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
};

/// FIXED_LEN_BYTE_ARRAY -> any fixed-size type
struct FixedStringConverter : public FixedSizeConverter
{
    bool isTrivial() const override { return true; }

    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

struct TrivialStringConverter : public StringConverter
{
    bool isTrivial() const override { return true; }

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

/// A thing that byteswaps and sign-extends integers up to 32 bytes long.
/// It's a struct (instead of a function) to allow precalculating some values if input size is fixed.
/// (I didn't test whether this precalculation actually improves performance.)
template <typename T>
struct BigEndianHelper
{
    size_t value_offset = 0;
    T value_mask = 0;
    T sign_mask = 0;
    T sign_extension_mask = 0;

    explicit BigEndianHelper(size_t input_size);

    /// Mask off extra bytes, reverse bytes, sign-extend.
    void fixupValue(T & x) const;

    T convertPaddedValue(const char * data) const;
    T convertUnpaddedValue(std::span<const char> data) const;
};

extern template struct BigEndianHelper<Int32>;
extern template struct BigEndianHelper<Int64>;
extern template struct BigEndianHelper<Int128>;
extern template struct BigEndianHelper<Int256>;

/// Input physical type: FIXED_LEN_BYTE_ARRAY.
/// Output column type: Decimal<T>, where T = Int{32,64,128,256}.
/// Output Field type: Decimal<T>.
template <typename T>
struct BigEndianDecimalFixedSizeConverter : public FixedSizeConverter
{
    /// (Input and output scale must match, we don't do scale conversion here.)
    UInt32 scale = 0;

    BigEndianHelper<T> helper;

    /// If input_size < sizeof(T), it means only the *last* (least-significant) input_size bytes of
    /// each value are encoded. The remaining most-significant bytes should be filled by sign-extension.
    BigEndianDecimalFixedSizeConverter(size_t input_size_, UInt32 scale_) : scale(scale_), helper(input_size_)
    {
        input_size = input_size_;
    }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

extern template struct BigEndianDecimalFixedSizeConverter<Int32>;
extern template struct BigEndianDecimalFixedSizeConverter<Int64>;
extern template struct BigEndianDecimalFixedSizeConverter<Int128>;
extern template struct BigEndianDecimalFixedSizeConverter<Int256>;

/// Input physical type: BYTE_ARRAY.
/// Output column type: Decimal<T>, where T = Int{32,64,128,256}.
/// Output Field type: Decimal<T>.
template <typename T>
struct BigEndianDecimalStringConverter : public StringConverter
{
    UInt32 scale = 0;

    explicit BigEndianDecimalStringConverter(UInt32 scale_) : scale(scale_) {}

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
    void convertField(std::span<const char> data, bool /*is_max*/, Field & out) const override;
};

extern template struct BigEndianDecimalStringConverter<Int32>;
extern template struct BigEndianDecimalStringConverter<Int64>;
extern template struct BigEndianDecimalStringConverter<Int128>;
extern template struct BigEndianDecimalStringConverter<Int256>;

struct Int96Converter : public FixedSizeConverter
{
    Int96Converter();

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
};

struct GeoConverter : public StringConverter
{
    GeoColumnMetadata geo_metadata;

    explicit GeoConverter(const GeoColumnMetadata & geo_metadata_) : geo_metadata(geo_metadata_) {}

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
};


void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out);

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data);

}
