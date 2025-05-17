#include <Processors/Formats/Impl/Parquet/Decoding.h>

namespace DB::Parquet
{

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
        //TODO
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
            //TODO
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
    for (size_t i = 0; i < indexes.size(); ++i)
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
                /// TODO: Try optimizing short memcpy by taking advantage of padding. Also in PlainStringDecoder.
                c.insertData(data.data() + start, len);
            }
            break;
        }
        case Mode::Uninitialized: chassert(false);
    }
}

}
