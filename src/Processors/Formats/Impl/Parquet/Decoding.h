#pragma once

#include <Processors/Formats/Impl/ArrowGeoTypes.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB
{
class IDataType;
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
    /// Memory owned by the decoded dictionary (the decompression buffer, string offsets, and the
    /// decoded `col`), excluding `data` which only points into one of those or into prefetcher memory.
    size_t allocatedBytes() const;
    void index(const ColumnUInt32 & indexes_col, IColumn & out);
    /// Append the values at the given dictionary indexes to `out`. Same as `index`, from a plain
    /// array; `index` delegates here. The indexes must be within bounds.
    void appendIndexes(const UInt32 * indexes, size_t n, IColumn & out);
    /// Append the value at dictionary index `idx` to `out`, `n` times. Used by the fused
    /// decode-and-index path (`PageDecoder::decodeAndIndex`) to turn an RLE run of a repeated
    /// index into a bulk fill, instead of expanding the run into explicit indexes and gathering
    /// them one by one.
    void appendRepeated(size_t idx, size_t n, IColumn & out);
    void decode(parq::Encoding::type encoding, const PageDecoderInfo & info, size_t num_values, std::span<const char> data_, const IDataType & raw_decoded_type);

    /// Upper bound on `allocatedBytes()` after `decode()` with the given arguments, computed from the
    /// page header *before* decoding anything. Lets a memory-bounded caller (the dictionary-filter
    /// pruning path in `Reader::decodeDictionaryPage`) reject an oversized dictionary before `decode()`
    /// transiently materializes it, so the pruning path never overshoots its budget even momentarily.
    /// `page_payload_size` is the size of the payload `decode()` will see, i.e. the size of the `data_`
    /// span: the *decompressed* page size for a compressed column chunk, the on-disk page size for an
    /// `UNCOMPRESSED` one. It must never be the compressed size of a compressed page: those bytes live
    /// in the prefetch buffer and are accounted separately by the caller, so charging them here would
    /// double-count them. `codec` is the column chunk's compression codec, which decides whether the
    /// payload is materialized in `decompressed_buf` at all (see `Reader::decodeDictionaryPageImpl`).
    /// Must be kept in sync with `decode()`.
    static size_t decodedFootprintUpperBound(
        parq::CompressionCodec::type codec, parq::Encoding::type encoding, const PageDecoderInfo & info,
        size_t num_values, size_t page_payload_size, const IDataType & raw_decoded_type);
};

struct PageDecoder
{
    virtual void skip(size_t num_values) = 0;
    virtual void decode(size_t num_values, IColumn & col, const UInt8 * filter, size_t filter_offset) = 0;

    /// Fused decode-and-gather for dictionary-encoded data pages: append the *dictionary values*
    /// at the decoded indexes directly to `out`, without materializing the indexes as a column.
    /// An RLE run of a repeated index becomes a single dictionary lookup and a bulk fill, and
    /// bit-packed indexes are gathered from a small stack buffer, saving a full write + read pass
    /// over an indexes column (most values in typical files sit in RLE runs). Returns false if
    /// this decoder (or this dictionary mode) does not support the fusion; the caller then falls
    /// back to `decode` into an indexes column + `Dictionary::index`.
    virtual bool decodeAndIndex(size_t /*num_values*/, Dictionary & /*dictionary*/, IColumn & /*out*/) { return false; }

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
    /// Returns std::nullopt if the value can't be decoded; the caller then keeps the corresponding
    /// range bound at +-infinity.
    /// Called only if PageDecoderInfo::allow_stats is true, which SchemaConverter sets only after
    /// carefully checking that min/max stats are usable in this situation (either no type
    /// conversion is needed, or the Field is converted afterwards -
    /// see PageDecoderInfo::cast_stats_to_output_type).
    virtual std::optional<Field> convertField(std::span<const char> /*data*/, bool /*is_max*/) const
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
    virtual std::optional<Field> convertField(std::span<const char> /*data*/, bool /*is_max*/) const
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
    parq::Type::type physical_type{};

    /// Postprocessing of decoded values. Exactly one of these is set, depending on physical_type.
    std::shared_ptr<FixedSizeConverter> fixed_size_converter;
    std::shared_ptr<StringConverter> string_converter;

    /// True if we can parse and use min/max from parquet Statistics.
    /// False if type hint requires a nontrivial cast that's either not monotonic or not supported.
    ///
    /// E.g. if the parquet file has column `x String`, but we read it as `file(..., 'x Int64')`, we
    /// silently auto-cast data from String to Int64 (by parsing number as text); but we can't do the
    /// same for min/max stats because String min/max is not the same as Int64 min/max (e.g. "10" < "9").
    /// So we have a small allowlist of type conversions (dispatched in SchemaConverter).
    bool allow_stats = false;

    /// If true, we need to call tryConvertFieldToType on the output of
    /// FixedSizeConverter/StringConverter's convertField.
    /// The conversion is from type PrimitiveColumnInfo::decoded_type to the column's type in the
    /// output block (the type that KeyCondition compares the Field against), as provided to
    /// decodeField.
    /// E.g. if the file has timestamps in ms, but the requested type is DateTime64(6), IntConverter
    /// will output Decimal64 Field with scale 3, then tryConvertFieldToType will rescale it to scale 6.
    ///
    /// SchemaConverter sets this only for type pairs where tryConvertFieldToType was audited to
    /// behave exactly like the castColumn that is later applied to the data: monotonic, with the
    /// same rounding (so the converted min/max still bound the converted values), and returning
    /// Null on overflow (then we leave the bound at infinity) rather than clamping or wrapping.
    /// E.g. this wouldn't be correct for Time64 output type: its cast wraps values around by day,
    /// non-monotonically, while tryConvertFieldToType just rescales.
    bool cast_stats_to_output_type = false;

    /// True if we can decompress the whole page directly into IColumn's memory.
    bool canReadDirectlyIntoColumn(parq::Encoding::type, size_t /*num_values*/, IColumn &, std::span<char> & out) const;

    /// [data, end) must be padded, i.e. have at least PADDING_FOR_SIMD bytes of readable memory
    /// before `data` and after `end`.
    std::unique_ptr<PageDecoder> makeDecoder(parq::Encoding::type, std::span<const char> data) const;

    /// Decode a min/max value from Statistics.
    /// If not supported, allow_stats is false, or the value doesn't survive the conversion to
    /// `final_output_type` (see cast_stats_to_output_type), leaves `out` unchanged.
    void decodeField(std::span<const char> data, bool is_max, const IDataType & decoded_type, const IDataType & final_output_type, Field & out) const;
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
    /// If not Ignore, it's a date column and we should range-check it.
    FormatSettings::DateTimeOverflowBehavior date_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Ignore;
    /// Only used when date_overflow_behavior is not Ignore: the requested output type is Date rather
    /// than Date32, so range-check against the narrower [0, DATE_LUT_MAX_DAY_NUM] window. The final
    /// cast of the decoded Int32 column to Date narrows to UInt16 without checks, so an unchecked
    /// extended Date32 value would wrap into an unrelated in-range Date.
    bool date_target_is_date = false;
    /// Same idea for a DateTime output type: the final context-less cast ignores
    /// `date_time_overflow_behavior` and wraps day numbers whose midnight does not fit into
    /// DateTime, so range-check against the [0, MAX_DATETIME_DAY_NUM] window of ToDateTimeImpl.
    bool date_target_is_datetime = false;
    /// Same idea for a DateTime64 output type, except that its window is scale-dependent: the cast clamps whole
    /// seconds the target scale cannot represent, and a scale-9 DateTime64 stops at 2262-04-11, far below the
    /// Date32 upper bound. Holds the day range of the requested DateTime64 type, when the output is one.
    std::optional<std::pair<Int32, Int32>> date_target_datetime64_day_range;

    /// The allowed day-number window of the requested output type, and its name for error messages.
    std::pair<Int32, Int32> dateTargetDayRange() const;
    String dateTargetTypeName() const;

    bool isTrivial() const override
    {
        return !output_size.has_value() && date_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Ignore;
    }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
};

/// Input physical type: FLOAT or DOUBLE.
/// Output column type: Float{32,64}.
/// Output Field type: Float{32,64}.
template <typename T>
struct FloatConverter : public FixedSizeConverter
{
    FloatConverter() { input_size = sizeof(T); }

    bool isTrivial() const override { return true; }

    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
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

    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
};

struct UUIDConverter : public FixedSizeConverter
{
    UUIDConverter() { input_size = 16; }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool is_max) const override;
};

struct TrivialStringConverter : public StringConverter
{
    bool isTrivial() const override { return true; }

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
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
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
};

extern template struct BigEndianDecimalFixedSizeConverter<Int32>;
extern template struct BigEndianDecimalFixedSizeConverter<Int64>;
extern template struct BigEndianDecimalFixedSizeConverter<Int128>;
extern template struct BigEndianDecimalFixedSizeConverter<Int256>;

/// Input physical type: a `DECIMAL`-annotated `FIXED_LEN_BYTE_ARRAY`.
/// Output column and `Field` type: `T`, where `T` is `Int128`, `UInt128`, `Int256`, or `UInt256`.
/// Values are range-checked instead of truncating leading sign-extension bytes.
template <typename T>
struct BigEndianDecimalWideIntegerConverter : public FixedSizeConverter
{
    explicit BigEndianDecimalWideIntegerConverter(size_t input_size_)
    {
        chassert(input_size_ > 0);
        input_size = input_size_;
    }

    void convertColumn(std::span<const char> data, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
};

extern template struct BigEndianDecimalWideIntegerConverter<Int128>;
extern template struct BigEndianDecimalWideIntegerConverter<UInt128>;
extern template struct BigEndianDecimalWideIntegerConverter<Int256>;
extern template struct BigEndianDecimalWideIntegerConverter<UInt256>;

/// Input physical type: a `DECIMAL`-annotated `BYTE_ARRAY`.
/// Output column and `Field` type: `T`, where `T` is `Int128`, `UInt128`, `Int256`, or `UInt256`.
/// Values are range-checked instead of truncating leading sign-extension bytes.
template <typename T>
struct BigEndianDecimalWideIntegerStringConverter : public StringConverter
{
    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
};

extern template struct BigEndianDecimalWideIntegerStringConverter<Int128>;
extern template struct BigEndianDecimalWideIntegerStringConverter<UInt128>;
extern template struct BigEndianDecimalWideIntegerStringConverter<Int256>;
extern template struct BigEndianDecimalWideIntegerStringConverter<UInt256>;

/// Input physical type: BYTE_ARRAY.
/// Output column type: Decimal<T>, where T = Int{32,64,128,256}.
/// Output Field type: Decimal<T>.
template <typename T>
struct BigEndianDecimalStringConverter : public StringConverter
{
    UInt32 scale = 0;

    explicit BigEndianDecimalStringConverter(UInt32 scale_) : scale(scale_) {}

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
    std::optional<Field> convertField(std::span<const char> data, bool /*is_max*/) const override;
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
    bool precise_float_parsing = true;

    GeoConverter(const GeoColumnMetadata & geo_metadata_, bool precise_float_parsing_)
        : geo_metadata(geo_metadata_), precise_float_parsing(precise_float_parsing_) {}

    void convertColumn(std::span<const char> chars, const UInt64 * offsets, size_t separator_bytes, size_t num_values, IColumn & col) const override;
};


/// If out_num_zeros is not null, the number of zero levels is added to it, counted as part of the
/// decoding.
void decodeRepOrDefLevels(parq::Encoding::type encoding, UInt8 max, size_t num_values, std::span<const char> data, PaddedPODArray<UInt8> & out, size_t * out_num_zeros = nullptr);

std::unique_ptr<PageDecoder> makeDictionaryIndicesDecoder(parq::Encoding::type encoding, size_t dictionary_size, std::span<const char> data);

}
