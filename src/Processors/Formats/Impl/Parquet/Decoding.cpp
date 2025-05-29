#include <Processors/Formats/Impl/Parquet/Decoding.h>

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


bool PageDecoderInfo::canReadDirectlyIntoColumn(parq::Encoding::type encoding, size_t num_values, IColumn & col, std::span<char> & out) const
{
    if (encoding == parq::Encoding::PLAIN && kind == Kind::FixedSize)
    {
        chassert(col.sizeOfValueIfFixed() == value_size);
        out = col.insertRawUninitialized(num_values);
        return true;
    }
    return false;
}

std::unique_ptr<PageDecoder> PageDecoderInfo::makeDecoder(
    parq::Encoding::type encoding, std::span<const char> data) const
{
    switch (encoding)
    {
        case parq::Encoding::PLAIN:
            switch (kind)
            {
                case Kind::FixedSize: return std::make_unique<PlainFixedSizeDecoder>(data, value_size);
                case Kind::String: return std::make_unique<PlainStringDecoder>(data);
                case Kind::ShortInt:
                    switch (value_size)
                    {
                        case 1: return std::make_unique<PlainCastDecoder<UInt32, UInt8>>(data);
                        case 2: return std::make_unique<PlainCastDecoder<UInt32, UInt16>>(data);
                        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected value_size for ShortInt decoder");
                        }
                case Kind::Boolean: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BOOLEAN is not implemented");
            }
        /// TODO [parquet]:
        case parq::Encoding::RLE: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "RLE encoding is not implemented");
        case parq::Encoding::BIT_PACKED: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "BIT_PACKED encoding is not implemented");
        case parq::Encoding::DELTA_BINARY_PACKED: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BINARY_PACKED encoding is not implemented");
        case parq::Encoding::DELTA_LENGTH_BYTE_ARRAY: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_LENGTH_BYTE_ARRAY encoding is not implemented");
        case parq::Encoding::DELTA_BYTE_ARRAY: throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DELTA_BYTE_ARRAY encoding is not implemented");
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
            /// TODO [parquet]
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
        case Mode::Uninitialized: break;
    }
    chassert(false);
    return 0;
}

void Dictionary::decode(parq::Encoding::type encoding, const PageDecoderInfo & info, size_t num_values, std::span<const char> data_, const IDataType & raw_decoded_type)
{
    chassert(mode == Mode::Uninitialized);
    if (encoding == parq::Encoding::PLAIN_DICTIONARY)
        encoding = parq::Encoding::PLAIN;
    count = num_values;
    bool decode_generic = false;
    switch (info.kind)
    {
        case PageDecoderInfo::Kind::FixedSize:
            mode = Mode::FixedSize;
            value_size = info.value_size;
            if (encoding == parq::Encoding::PLAIN)
                data = data_;
            else
                /// Parquet supports only PLAIN encoding for dictionaries, but we support any encoding
                /// because it's easy (we need decode_generic code path anyway for ShortInt and Boolean).
                decode_generic = true;
            break;
        case PageDecoderInfo::Kind::String:
            if (encoding == parq::Encoding::PLAIN)
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
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected non-plain encoding in dictionary");
            }
            break;
        case PageDecoderInfo::Kind::ShortInt:
        case PageDecoderInfo::Kind::Boolean:
            mode = Mode::FixedSize;
            value_size = info.value_size;
            decode_generic = true;
            break;
    }

    if (decode_generic)
    {
        chassert(mode == Mode::FixedSize);
        auto decoder = info.makeDecoder(encoding, data_);
        auto c = raw_decoded_type.createColumn();
        c->reserve(num_values);
        decoder->decode(num_values, *c);
        col = std::move(c);

        std::string_view s = col->getRawData();
        data = std::span(s.data(), s.size());
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

void Dictionary::index(const PaddedPODArray<UInt32> & indexes, IColumn & out)
{
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
        case Mode::Uninitialized: chassert(false);
    }
}



}
