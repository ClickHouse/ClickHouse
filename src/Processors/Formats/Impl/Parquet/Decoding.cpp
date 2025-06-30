#include <Processors/Formats/Impl/Parquet/Decoding.h>

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
            bit_width = size_t(*data);
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
    size_t value_size;
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

            converter->convertColumn(std::span(chars_start, offset), offsets, /*separator_bytes*/ 4, num_values, col);
        }
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
        /// TODO [parquet]: DELTA_BINARY_PACKED for INT32 and INT64
        case parq::Encoding::DELTA_BINARY_PACKED: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BINARY_PACKED encoding is not implemented");
        /// TODO [parquet]: DELTA_LENGTH_BYTE_ARRAY for BYTE_ARRAY
        case parq::Encoding::DELTA_LENGTH_BYTE_ARRAY: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_LENGTH_BYTE_ARRAY encoding is not implemented");
        /// TODO [parquet]: DELTA_BYTE_ARRAY for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY
        case parq::Encoding::DELTA_BYTE_ARRAY: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BYTE_ARRAY encoding is not implemented");
        /// TODO [parquet]: BYTE_STREAM_SPLIT for FLOAT and DOUBLE
        case parq::Encoding::BYTE_STREAM_SPLIT: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BYTE_STREAM_SPLIT encoding is not implemented");
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
                /// TODO [parquet]: Try optimizing short memcpy by taking advantage of padding. Also in PlainStringDecoder.
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

void TrivialStringConverter::convertColumn(std::span<const char> chars, const IColumn::Offsets & offsets, size_t separator_bytes, size_t num_values, IColumn & col) const
{
    auto & col_str = assert_cast<ColumnString &>(col);
    col_str.reserve(col_str.size() + num_values);
    chassert(chars.size() == offsets.back());
    col_str.getChars().reserve(col_str.getChars().size() + offsets.back() - separator_bytes * num_values + num_values);
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
void BigEndianDecimalStringConverter<T>::convertColumn(std::span<const char> chars, const IColumn::Offsets & offsets, size_t separator_bytes, size_t num_values, IColumn & col) const
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
