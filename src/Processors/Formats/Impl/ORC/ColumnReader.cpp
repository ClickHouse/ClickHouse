#include <Processors/Formats/Impl/ORC/ColumnReader.h>

#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Common/DateLUT.h>

#include <bit>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::ORC
{

namespace
{

RleVersion getRleVersion(const Proto::ColumnEncoding & encoding)
{
    switch (encoding.kind)
    {
        case Proto::ColumnEncodingKind::DIRECT:
        case Proto::ColumnEncodingKind::DICTIONARY:
            return RleVersion::V1;
        case Proto::ColumnEncodingKind::DIRECT_V2:
        case Proto::ColumnEncodingKind::DICTIONARY_V2:
            return RleVersion::V2;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown ORC column encoding {}", static_cast<int>(encoding.kind));
}

std::unique_ptr<InputByteStream> getRequiredStream(StripeStreams & stripe, UInt64 column_id, Proto::StreamKind kind, const char * stream_name, const char * column_kind)
{
    auto stream = stripe.getStream(column_id, kind);
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "{} stream not found in ORC {} column {}", stream_name, column_kind, column_id);
    return stream;
}

/// Converts the bytes read into the beginning of a buffer of integers into the integers, in place.
template <typename T>
void expandBytesToIntegers(T * buffer, size_t num_values)
{
    const Int8 * bytes = reinterpret_cast<const Int8 *>(buffer);
    for (size_t i = num_values; i > 0; --i)
        buffer[i - 1] = bytes[i - 1];
}


class BooleanColumnReader final : public ColumnReader
{
public:
    BooleanColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
        , rle(getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "boolean"))
    {
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        rle.skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        auto * data = typeid_cast<LongVectorBatch &>(batch).data.data();
        char * bytes = reinterpret_cast<char *>(data);
        memset(bytes, 0, num_values);
        rle.next(bytes, num_values, batch.hasNulls ? batch.notNull.data() : nullptr);
        expandBytesToIntegers(data, num_values);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle.seek(positions.at(column_id));
    }

private:
    BooleanRleDecoder rle;
};

class ByteColumnReader final : public ColumnReader
{
public:
    ByteColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
        , rle(getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "byte"))
    {
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        rle.skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        auto * data = typeid_cast<LongVectorBatch &>(batch).data.data();
        char * bytes = reinterpret_cast<char *>(data);
        memset(bytes, 0, num_values);
        rle.next(bytes, num_values, batch.hasNulls ? batch.notNull.data() : nullptr);
        expandBytesToIntegers(data, num_values);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle.seek(positions.at(column_id));
    }

private:
    ByteRleDecoder rle;
};

class IntegerColumnReader final : public ColumnReader
{
public:
    IntegerColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        rle = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "integer"),
            true, getRleVersion(stripe.getEncoding(column_id)));
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        rle->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        rle->next(typeid_cast<LongVectorBatch &>(batch).data.data(), num_values, batch.hasNulls ? batch.notNull.data() : nullptr);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(column_id));
    }

private:
    std::unique_ptr<IntRleDecoder> rle;
};

template <typename ItemType>
class FloatingColumnReader final : public ColumnReader
{
public:
    FloatingColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
        , stream(getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "floating point"))
    {
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        stream->skipBytes(num_values * sizeof(ItemType));
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        double * out = typeid_cast<DoubleVectorBatch &>(batch).data.data();

        size_t i = 0;
        if (!not_null)
        {
            while (i < num_values)
            {
                if (stream->pos == stream->end && !stream->nextChunk())
                    stream->throwUnexpectedEnd();
                size_t available = (stream->end - stream->pos) / sizeof(ItemType);
                if (available == 0)
                {
                    out[i++] = read();
                    continue;
                }
                size_t count = std::min(available, num_values - i);
                for (size_t j = 0; j < count; ++j)
                {
                    ItemType value;
                    memcpy(&value, stream->pos + j * sizeof(ItemType), sizeof(ItemType));
                    if constexpr (std::endian::native == std::endian::big)
                        value = std::bit_cast<ItemType>(std::byteswap(std::bit_cast<std::conditional_t<sizeof(ItemType) == 8, UInt64, UInt32>>(value)));
                    out[i + j] = static_cast<double>(value);
                }
                stream->pos += count * sizeof(ItemType);
                i += count;
            }
        }
        else
        {
            for (; i < num_values; ++i)
                if (not_null[i])
                    out[i] = read();
        }
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        stream->seek(positions.at(column_id));
    }

private:
    double read()
    {
        using Bits = std::conditional_t<sizeof(ItemType) == 8, UInt64, UInt32>;
        Bits bits = 0;
        for (size_t b = 0; b < sizeof(ItemType); ++b)
            bits |= static_cast<Bits>(stream->readByte()) << (b * 8);
        return static_cast<double>(std::bit_cast<ItemType>(bits));
    }

    std::unique_ptr<InputByteStream> stream;
};

std::shared_ptr<StringDictionary> loadStringDictionary(UInt64 column_id, StripeStreams & stripe)
{
    const auto & encoding = stripe.getEncoding(column_id);
    UInt32 dictionary_size = encoding.dictionary_size;
    auto dictionary = std::make_shared<StringDictionary>();

    /// A dictionary cannot have more entries than the stripe has values.
    if (dictionary_size > stripe.getNumberOfRows())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "The dictionary of ORC column {} has {} entries, more than the {} rows of the stripe",
            column_id, dictionary_size, stripe.getNumberOfRows());

    dictionary->dictionaryOffset.resize(dictionary_size + 1);
    Int64 * offsets = dictionary->dictionaryOffset.data();
    offsets[0] = 0;
    if (dictionary_size > 0)
    {
        auto length_stream = getRequiredStream(stripe, column_id, Proto::StreamKind::LENGTH, "LENGTH", "string dictionary");
        auto length_decoder = createIntRleDecoder(std::move(length_stream), false, getRleVersion(encoding));
        length_decoder->next(offsets + 1, dictionary_size, nullptr);
    }
    for (size_t i = 1; i <= dictionary_size; ++i)
    {
        if (offsets[i] < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative dictionary entry length in ORC column {}", column_id);
        if (__builtin_add_overflow(offsets[i], offsets[i - 1], &offsets[i]))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Dictionary size overflow in ORC column {}", column_id);
    }

    Int64 blob_size = offsets[dictionary_size];
    if (blob_size > 0)
    {
        auto blob_stream = getRequiredStream(stripe, column_id, Proto::StreamKind::DICTIONARY_DATA, "DICTIONARY_DATA", "string dictionary");
        dictionary->dictionaryBlob.resize(blob_size);
        blob_stream->readBytes(dictionary->dictionaryBlob.data(), blob_size);
    }
    return dictionary;
}

class StringDictionaryColumnReader final : public ColumnReader
{
public:
    StringDictionaryColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        rle = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "string dictionary"),
            false, getRleVersion(stripe.getEncoding(column_id)));
        dictionary = loadStringDictionary(column_id, stripe);
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        rle->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & string_batch = dynamic_cast<StringVectorBatch &>(batch);
        char * blob = dictionary->dictionaryBlob.data();
        const Int64 * offsets = dictionary->dictionaryOffset.data();
        char ** starts = string_batch.data.data();
        Int64 * lengths = string_batch.length.data();
        rle->next(lengths, num_values, not_null);

        UInt64 dictionary_count = dictionary->dictionaryOffset.size() - 1;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            Int64 entry = lengths[i];
            if (entry < 0 || static_cast<UInt64>(entry) >= dictionary_count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Dictionary index {} is out of range [0, {}) in ORC column {}", entry, dictionary_count, column_id);
            starts[i] = blob + offsets[entry];
            lengths[i] = offsets[entry + 1] - offsets[entry];
        }
    }

    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        batch.isEncoded = true;
        auto & encoded_batch = dynamic_cast<EncodedStringVectorBatch &>(batch);
        encoded_batch.dictionary = dictionary;
        rle->next(encoded_batch.index.data(), num_values, not_null);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(column_id));
    }

private:
    std::shared_ptr<StringDictionary> dictionary;
    std::unique_ptr<IntRleDecoder> rle;
};

class StringDirectColumnReader final : public ColumnReader
{
public:
    StringDirectColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        length_rle = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::LENGTH, "LENGTH", "string"),
            false, getRleVersion(stripe.getEncoding(column_id)));
        blob_stream = getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "string");
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        static constexpr size_t BUFFER_SIZE = 1024;
        Int64 buffer[BUFFER_SIZE];
        size_t done = 0;
        size_t total_bytes = 0;
        while (done < num_values)
        {
            size_t step = std::min(BUFFER_SIZE, num_values - done);
            length_rle->next(buffer, step, nullptr);
            total_bytes += computeSize(buffer, nullptr, step);
            done += step;
        }
        blob_stream->skipBytes(total_bytes);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & string_batch = dynamic_cast<StringVectorBatch &>(batch);
        Int64 * lengths = string_batch.length.data();
        length_rle->next(lengths, num_values, not_null);

        size_t total_length = computeSize(lengths, not_null, num_values);
        string_batch.blob.resize(total_length);
        blob_stream->readBytes(string_batch.blob.data(), total_length);

        char ** starts = string_batch.data.data();
        char * ptr = string_batch.blob.data();
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            starts[i] = ptr;
            ptr += lengths[i];
        }
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        auto & provider = positions.at(column_id);
        blob_stream->seek(provider);
        length_rle->seek(provider);
    }

private:
    size_t computeSize(const Int64 * lengths, const char * not_null, size_t num_values) const
    {
        size_t total = 0;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            if (lengths[i] < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Negative string length in ORC column {}", column_id);
            if (__builtin_add_overflow(total, static_cast<size_t>(lengths[i]), &total))
                throw Exception(ErrorCodes::INCORRECT_DATA, "String length overflow in ORC column {}", column_id);
        }
        return total;
    }

    std::unique_ptr<IntRleDecoder> length_rle;
    std::unique_ptr<InputByteStream> blob_stream;
};

class TimestampColumnReader final : public ColumnReader
{
public:
    TimestampColumnReader(const Type & type, StripeStreams & stripe, bool is_instant)
        : ColumnReader(type, stripe)
        , writer_timezone(is_instant ? DateLUT::instance("UTC") : stripe.getWriterTimezone())
        , reader_timezone(is_instant ? DateLUT::instance("UTC") : stripe.getReaderTimezone())
        , same_timezone(&writer_timezone == &reader_timezone || writer_timezone.getTimeZone() == reader_timezone.getTimeZone())
    {
        /// The base of the seconds is 2015-01-01 00:00:00 in the time zone of the writer.
        static constexpr Int64 UTC_2015_01_01 = 1420070400;
        epoch_offset = UTC_2015_01_01 - writer_timezone.timezoneOffset(UTC_2015_01_01);

        RleVersion version = getRleVersion(stripe.getEncoding(column_id));
        seconds_rle = createIntRleDecoder(getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "timestamp"), true, version);
        nanos_rle = createIntRleDecoder(getRequiredStream(stripe, column_id, Proto::StreamKind::SECONDARY, "SECONDARY", "timestamp"), false, version);
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        seconds_rle->skip(num_values);
        nanos_rle->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & timestamp_batch = typeid_cast<TimestampVectorBatch &>(batch);
        Int64 * seconds = timestamp_batch.data.data();
        Int64 * nanos = timestamp_batch.nanoseconds.data();
        seconds_rle->next(seconds, num_values, not_null);
        nanos_rle->next(nanos, num_values, not_null);

        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;

            /// The lowest 3 bits are the number of the trailing zeros (minus one) that are removed.
            UInt64 zeros = nanos[i] & 0x7;
            /// An arithmetic shift, as in the reference implementation: some writers (the Arrow
            /// adapter) store negative nanoseconds for the timestamps before the epoch.
            nanos[i] >>= 3;
            if (zeros != 0)
                for (UInt64 j = 0; j <= zeros; ++j)
                    nanos[i] *= 10;

            Int64 writer_time = static_cast<Int64>(static_cast<UInt64>(seconds[i]) + static_cast<UInt64>(epoch_offset));
            if (writer_time < 0 && nanos[i] > 999999)
                writer_time -= 1;

            if (!same_timezone)
            {
                /// Keep the same wall clock time when the writer and the reader time zones differ.
                Int64 writer_offset = writer_timezone.timezoneOffset(writer_time);
                Int64 reader_offset = reader_timezone.timezoneOffset(writer_time);
                if (writer_offset != reader_offset)
                {
                    /// If the adjustment crosses a DST boundary, the offset has to be reevaluated.
                    Int64 adjusted_time = writer_time + writer_offset - reader_offset;
                    Int64 adjusted_reader_offset = reader_timezone.timezoneOffset(adjusted_time);
                    writer_time = writer_time + writer_offset - adjusted_reader_offset;
                }
            }
            seconds[i] = writer_time;
        }
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        auto & provider = positions.at(column_id);
        seconds_rle->seek(provider);
        nanos_rle->seek(provider);
    }

private:
    const DateLUTImpl & writer_timezone;
    const DateLUTImpl & reader_timezone;
    const bool same_timezone;
    Int64 epoch_offset = 0;
    std::unique_ptr<IntRleDecoder> seconds_rle;
    std::unique_ptr<IntRleDecoder> nanos_rle;
};

constexpr UInt32 MAX_PRECISION_64 = 18;

constexpr Int64 POWERS_OF_TEN[MAX_PRECISION_64 + 1] = {
    1LL,
    10LL,
    100LL,
    1000LL,
    10000LL,
    100000LL,
    1000000LL,
    10000000LL,
    100000000LL,
    1000000000LL,
    10000000000LL,
    100000000000LL,
    1000000000000LL,
    10000000000000LL,
    100000000000000LL,
    1000000000000000LL,
    10000000000000000LL,
    100000000000000000LL,
    1000000000000000000LL};

/// The DECIMAL columns: an unbounded signed varint per value in the DATA stream and the scale of
/// each value in the SECONDARY stream. The values are rescaled to the scale of the type.
class DecimalColumnReader : public ColumnReader
{
public:
    DecimalColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
        , precision(static_cast<Int32>(type.getPrecision()))
        , scale(static_cast<Int32>(type.getScale()))
    {
        value_stream = getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "decimal");
        scale_decoder = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::SECONDARY, "SECONDARY", "decimal"),
            true, getRleVersion(stripe.getEncoding(column_id)));
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        size_t skipped = 0;
        while (skipped < num_values)
            if (!(value_stream->readByte() & 0x80))
                ++skipped;
        scale_decoder->skip(num_values);
        return num_values;
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        auto & provider = positions.at(column_id);
        value_stream->seek(provider);
        scale_decoder->seek(provider);
    }

protected:
    /// Reads the zigzag-encoded varint of up to 128 bits.
    Int128 readInt128()
    {
        UInt128 value = 0;
        UInt32 shift = 0;
        while (true)
        {
            UInt8 byte = value_stream->readByte();
            if (shift < 128)
                value |= static_cast<UInt128>(byte & 0x7F) << shift;
            shift += 7;
            if (!(byte & 0x80))
                break;
        }
        return static_cast<Int128>((value >> 1) ^ (UInt128(0) - (value & 1)));
    }

    static void scaleInt128(Int128 & value, Int32 target_scale, Int32 current_scale)
    {
        while (target_scale > current_scale)
        {
            Int32 adjust = std::min<Int32>(MAX_PRECISION_64, target_scale - current_scale);
            value = static_cast<Int128>(static_cast<UInt128>(value) * static_cast<UInt128>(POWERS_OF_TEN[adjust]));
            current_scale += adjust;
        }
        while (current_scale > target_scale)
        {
            Int32 adjust = std::min<Int32>(MAX_PRECISION_64, current_scale - target_scale);
            value /= POWERS_OF_TEN[adjust];
            current_scale -= adjust;
        }
    }

    const Int32 precision;
    Int32 scale;
    std::unique_ptr<InputByteStream> value_stream;
    std::unique_ptr<IntRleDecoder> scale_decoder;
    PaddedPODArray<Int64> scales;
};

class Decimal64ColumnReader final : public DecimalColumnReader
{
public:
    using DecimalColumnReader::DecimalColumnReader;

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & decimal_batch = typeid_cast<Decimal64VectorBatch &>(batch);
        scales.resize(num_values);
        scale_decoder->next(scales.data(), num_values, not_null);
        decimal_batch.precision = precision;
        decimal_batch.scale = scale;
        Int64 * values = decimal_batch.values.data();
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            values[i] = readInt64(static_cast<Int32>(scales[i]));
        }
    }

private:
    Int64 readInt64(Int32 current_scale)
    {
        UInt64 encoded = readVarint(*value_stream);
        Int64 value = static_cast<Int64>((encoded >> 1) ^ (~(encoded & 1) + 1));
        if (scale > current_scale && static_cast<UInt64>(scale - current_scale) <= MAX_PRECISION_64)
            value = static_cast<Int64>(static_cast<UInt64>(value) * static_cast<UInt64>(POWERS_OF_TEN[scale - current_scale]));
        else if (scale < current_scale && static_cast<UInt64>(current_scale - scale) <= MAX_PRECISION_64)
            value /= POWERS_OF_TEN[current_scale - scale];
        else if (scale != current_scale)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Decimal scale {} is out of range in ORC column {}", current_scale, column_id);
        return value;
    }
};

class Decimal128ColumnReader final : public DecimalColumnReader
{
public:
    using DecimalColumnReader::DecimalColumnReader;

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & decimal_batch = typeid_cast<Decimal128VectorBatch &>(batch);
        scales.resize(num_values);
        scale_decoder->next(scales.data(), num_values, not_null);
        decimal_batch.precision = precision;
        decimal_batch.scale = scale;
        Int128 * values = decimal_batch.values.data();
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            values[i] = readInt128();
            scaleInt128(values[i], scale, static_cast<Int32>(scales[i]));
        }
    }
};

/// The DECIMAL columns written by Hive 0.11, without the precision and the scale in the type.
/// As in the reference implementation, the values are read with the scale 6 and must have at most 38 digits.
class DecimalHive11ColumnReader final : public DecimalColumnReader
{
public:
    DecimalHive11ColumnReader(const Type & type, StripeStreams & stripe)
        : DecimalColumnReader(type, stripe)
    {
        scale = 6;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & decimal_batch = typeid_cast<Decimal128VectorBatch &>(batch);
        scales.resize(num_values);
        scale_decoder->next(scales.data(), num_values, not_null);
        decimal_batch.precision = precision;
        decimal_batch.scale = scale;
        Int128 * values = decimal_batch.values.data();

        static const Int128 max_value = Int128(10000000000000000000ULL) * Int128(10000000000000000000ULL) - 1;
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            bool overflow = false;
            UInt128 value = 0;
            UInt32 shift = 0;
            while (true)
            {
                UInt8 byte = value_stream->readByte();
                UInt32 bits = byte & 0x7F;
                if (shift > 128 || (shift == 126 && bits > 3))
                    overflow = true;
                if (shift < 128)
                    value |= static_cast<UInt128>(bits) << shift;
                shift += 7;
                if (!(byte & 0x80))
                    break;
            }
            Int128 decoded = static_cast<Int128>((value >> 1) ^ (UInt128(0) - (value & 1)));
            if (!overflow)
            {
                scaleInt128(decoded, scale, static_cast<Int32>(scales[i]));
                overflow = decoded > max_value || decoded < -max_value;
            }
            if (overflow)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Hive 0.11 decimal was more than 38 digits in ORC column {}", column_id);
            values[i] = decoded;
        }
    }
};

class StructColumnReader final : public ColumnReader
{
public:
    StructColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        const auto & encoding = stripe.getEncoding(column_id);
        if (encoding.kind != Proto::ColumnEncodingKind::DIRECT)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown encoding {} of ORC struct column {}", static_cast<int>(encoding.kind), column_id);
        const auto & selected = stripe.getSelectedColumns();
        for (UInt64 i = 0; i < type.getSubtypeCount(); ++i)
        {
            const Type & child = *type.getSubtype(i);
            if (selected[child.getColumnId()])
                children.push_back(buildColumnReader(child, stripe));
        }
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        for (auto & child : children)
            child->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<false>(batch, num_values, incoming_mask);
    }

    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<true>(batch, num_values, incoming_mask);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        for (auto & child : children)
            child->seekToRowGroup(positions);
    }

private:
    template <bool encoded>
    void nextImpl(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        const char * not_null = batch.hasNulls ? batch.notNull.data() : nullptr;
        auto & struct_batch = typeid_cast<StructVectorBatch &>(batch);
        for (size_t i = 0; i < children.size(); ++i)
        {
            if constexpr (encoded)
                children[i]->nextEncoded(*struct_batch.fields[i], num_values, not_null);
            else
                children[i]->next(*struct_batch.fields[i], num_values, not_null);
        }
    }

    std::vector<std::unique_ptr<ColumnReader>> children;
};

/// Converts the lengths into the offsets, returns the total number of the children.
UInt64 lengthsToOffsets(Int64 * offsets, size_t num_values, const char * not_null, UInt64 column_id)
{
    UInt64 total = 0;
    for (size_t i = 0; i < num_values; ++i)
    {
        if (not_null && !not_null[i])
        {
            offsets[i] = static_cast<Int64>(total);
            continue;
        }
        Int64 length = offsets[i];
        if (length < 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Negative length {} in ORC column {}", length, column_id);
        offsets[i] = static_cast<Int64>(total);
        if (__builtin_add_overflow(total, static_cast<UInt64>(length), &total) || total > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Length overflow in ORC column {}", column_id);
    }
    offsets[num_values] = static_cast<Int64>(total);
    return total;
}

UInt64 sumLengths(IntRleDecoder & rle, size_t num_values, UInt64 column_id)
{
    static constexpr size_t BUFFER_SIZE = 1024;
    Int64 buffer[BUFFER_SIZE];
    UInt64 total = 0;
    size_t read = 0;
    while (read < num_values)
    {
        size_t chunk = std::min(num_values - read, BUFFER_SIZE);
        rle.next(buffer, chunk, nullptr);
        for (size_t i = 0; i < chunk; ++i)
        {
            if (buffer[i] < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Negative length {} in ORC column {}", buffer[i], column_id);
            total += static_cast<UInt64>(buffer[i]);
        }
        read += chunk;
    }
    return total;
}

class ListColumnReader final : public ColumnReader
{
public:
    ListColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        rle = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::LENGTH, "LENGTH", "list"),
            false, getRleVersion(stripe.getEncoding(column_id)));
        const Type & child_type = *type.getSubtype(0);
        if (stripe.getSelectedColumns()[child_type.getColumnId()])
            child = buildColumnReader(child_type, stripe);
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        if (child)
            child->skip(sumLengths(*rle, num_values, column_id));
        else
            rle->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<false>(batch, num_values, incoming_mask);
    }

    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<true>(batch, num_values, incoming_mask);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(column_id));
        if (child)
            child->seekToRowGroup(positions);
    }

private:
    template <bool encoded>
    void nextImpl(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        auto & list_batch = typeid_cast<ListVectorBatch &>(batch);
        const char * not_null = list_batch.hasNulls ? list_batch.notNull.data() : nullptr;
        Int64 * offsets = list_batch.offsets.data();
        rle->next(offsets, num_values, not_null);
        UInt64 total = lengthsToOffsets(offsets, num_values, not_null, column_id);
        if (child)
        {
            if constexpr (encoded)
                child->nextEncoded(*list_batch.elements, total, nullptr);
            else
                child->next(*list_batch.elements, total, nullptr);
        }
    }

    std::unique_ptr<IntRleDecoder> rle;
    std::unique_ptr<ColumnReader> child;
};

class MapColumnReader final : public ColumnReader
{
public:
    MapColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
    {
        rle = createIntRleDecoder(
            getRequiredStream(stripe, column_id, Proto::StreamKind::LENGTH, "LENGTH", "map"),
            false, getRleVersion(stripe.getEncoding(column_id)));
        const auto & selected = stripe.getSelectedColumns();
        const Type & key_type = *type.getSubtype(0);
        if (selected[key_type.getColumnId()])
            key_reader = buildColumnReader(key_type, stripe);
        const Type & value_type = *type.getSubtype(1);
        if (selected[value_type.getColumnId()])
            value_reader = buildColumnReader(value_type, stripe);
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        if (key_reader || value_reader)
        {
            UInt64 total = sumLengths(*rle, num_values, column_id);
            if (key_reader)
                key_reader->skip(total);
            if (value_reader)
                value_reader->skip(total);
        }
        else
            rle->skip(num_values);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<false>(batch, num_values, incoming_mask);
    }

    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<true>(batch, num_values, incoming_mask);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle->seek(positions.at(column_id));
        if (key_reader)
            key_reader->seekToRowGroup(positions);
        if (value_reader)
            value_reader->seekToRowGroup(positions);
    }

private:
    template <bool encoded>
    void nextImpl(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        auto & map_batch = typeid_cast<MapVectorBatch &>(batch);
        const char * not_null = map_batch.hasNulls ? map_batch.notNull.data() : nullptr;
        Int64 * offsets = map_batch.offsets.data();
        rle->next(offsets, num_values, not_null);
        UInt64 total = lengthsToOffsets(offsets, num_values, not_null, column_id);
        if (key_reader)
        {
            if constexpr (encoded)
                key_reader->nextEncoded(*map_batch.keys, total, nullptr);
            else
                key_reader->next(*map_batch.keys, total, nullptr);
        }
        if (value_reader)
        {
            if constexpr (encoded)
                value_reader->nextEncoded(*map_batch.elements, total, nullptr);
            else
                value_reader->next(*map_batch.elements, total, nullptr);
        }
    }

    std::unique_ptr<IntRleDecoder> rle;
    std::unique_ptr<ColumnReader> key_reader;
    std::unique_ptr<ColumnReader> value_reader;
};

class UnionColumnReader final : public ColumnReader
{
public:
    UnionColumnReader(const Type & type, StripeStreams & stripe)
        : ColumnReader(type, stripe)
        , rle(getRequiredStream(stripe, column_id, Proto::StreamKind::DATA, "DATA", "union"))
        , num_children(type.getSubtypeCount())
    {
        children.resize(num_children);
        counts.resize(num_children);
        const auto & selected = stripe.getSelectedColumns();
        for (size_t i = 0; i < num_children; ++i)
        {
            const Type & child = *type.getSubtype(i);
            if (selected[child.getColumnId()])
                children[i] = buildColumnReader(child, stripe);
        }
    }

    size_t skip(size_t num_values) override
    {
        num_values = ColumnReader::skip(num_values);
        std::fill(counts.begin(), counts.end(), 0);
        static constexpr size_t BUFFER_SIZE = 1024;
        char buffer[BUFFER_SIZE];
        size_t read = 0;
        while (read < num_values)
        {
            size_t chunk = std::min(num_values - read, BUFFER_SIZE);
            rle.next(buffer, chunk, nullptr);
            for (size_t i = 0; i < chunk; ++i)
                ++counts[checkedTag(static_cast<UInt8>(buffer[i]))];
            read += chunk;
        }
        for (size_t i = 0; i < num_children; ++i)
            if (counts[i] && children[i])
                children[i]->skip(counts[i]);
        return num_values;
    }

    void next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<false>(batch, num_values, incoming_mask);
    }

    void nextEncoded(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask) override
    {
        nextImpl<true>(batch, num_values, incoming_mask);
    }

    void seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions) override
    {
        ColumnReader::seekToRowGroup(positions);
        rle.seek(positions.at(column_id));
        for (auto & child : children)
            if (child)
                child->seekToRowGroup(positions);
    }

private:
    size_t checkedTag(UInt8 tag) const
    {
        if (tag >= num_children)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid union tag {} for union with {} children in ORC column {}", static_cast<UInt32>(tag), num_children, column_id);
        return tag;
    }

    template <bool encoded>
    void nextImpl(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
    {
        ColumnReader::next(batch, num_values, incoming_mask);
        auto & union_batch = typeid_cast<UnionVectorBatch &>(batch);
        const char * not_null = union_batch.hasNulls ? union_batch.notNull.data() : nullptr;
        UInt8 * tags = union_batch.tags.data();
        UInt64 * offsets = union_batch.offsets.data();
        memset(tags, 0, num_values);
        rle.next(reinterpret_cast<char *>(tags), num_values, not_null);
        std::fill(counts.begin(), counts.end(), 0);
        for (size_t i = 0; i < num_values; ++i)
        {
            if (not_null && !not_null[i])
                continue;
            offsets[i] = counts[checkedTag(tags[i])]++;
        }
        for (size_t i = 0; i < num_children; ++i)
        {
            if (!children[i])
                continue;
            if constexpr (encoded)
                children[i]->nextEncoded(*union_batch.children[i], counts[i], nullptr);
            else
                children[i]->next(*union_batch.children[i], counts[i], nullptr);
        }
    }

    ByteRleDecoder rle;
    const size_t num_children;
    std::vector<std::unique_ptr<ColumnReader>> children;
    std::vector<UInt64> counts;
};

}


ColumnReader::ColumnReader(const Type & type, StripeStreams & stripe)
    : column_id(type.getColumnId())
{
    if (auto stream = stripe.getStream(column_id, Proto::StreamKind::PRESENT))
        not_null_decoder = std::make_unique<BooleanRleDecoder>(std::move(stream));
}

void ColumnReader::next(ColumnVectorBatch & batch, size_t num_values, const char * incoming_mask)
{
    if (num_values > batch.capacity)
        batch.resize(num_values);
    batch.numElements = num_values;

    if (not_null_decoder)
    {
        char * not_null = batch.notNull.data();
        not_null_decoder->next(not_null, num_values, incoming_mask);
        batch.hasNulls = memchr(not_null, 0, num_values) != nullptr;
        return;
    }
    if (incoming_mask)
    {
        batch.hasNulls = true;
        memcpy(batch.notNull.data(), incoming_mask, num_values);
        return;
    }
    batch.hasNulls = false;
}

size_t ColumnReader::skip(size_t num_values)
{
    if (!not_null_decoder)
        return num_values;

    static constexpr size_t BUFFER_SIZE = 32768;
    char buffer[BUFFER_SIZE];
    size_t remaining = num_values;
    while (remaining)
    {
        size_t chunk = std::min(remaining, BUFFER_SIZE);
        not_null_decoder->next(buffer, chunk, nullptr);
        for (size_t i = 0; i < chunk; ++i)
            if (!buffer[i])
                --num_values;
        remaining -= chunk;
    }
    return num_values;
}

void ColumnReader::seekToRowGroup(std::unordered_map<UInt64, PositionProvider> & positions)
{
    if (not_null_decoder)
    {
        auto it = positions.find(column_id);
        if (it == positions.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "No row index for ORC column {}", column_id);
        not_null_decoder->seek(it->second);
    }
}

std::unique_ptr<ColumnReader> buildColumnReader(const Type & type, StripeStreams & stripe)
{
    checkStackSize();

    switch (type.getKind())
    {
        case BOOLEAN:
            return std::make_unique<BooleanColumnReader>(type, stripe);
        case BYTE:
            return std::make_unique<ByteColumnReader>(type, stripe);
        case SHORT:
        case INT:
        case LONG:
        case DATE:
            return std::make_unique<IntegerColumnReader>(type, stripe);
        case FLOAT:
            return std::make_unique<FloatingColumnReader<float>>(type, stripe);
        case DOUBLE:
            return std::make_unique<FloatingColumnReader<double>>(type, stripe);
        case STRING:
        case BINARY:
        case CHAR:
        case VARCHAR:
        case GEOMETRY:
        case GEOGRAPHY:
            switch (stripe.getEncoding(type.getColumnId()).kind)
            {
                case Proto::ColumnEncodingKind::DICTIONARY:
                case Proto::ColumnEncodingKind::DICTIONARY_V2:
                    return std::make_unique<StringDictionaryColumnReader>(type, stripe);
                case Proto::ColumnEncodingKind::DIRECT:
                case Proto::ColumnEncodingKind::DIRECT_V2:
                    return std::make_unique<StringDirectColumnReader>(type, stripe);
            }
            break;
        case TIMESTAMP:
            return std::make_unique<TimestampColumnReader>(type, stripe, false);
        case TIMESTAMP_INSTANT:
            return std::make_unique<TimestampColumnReader>(type, stripe, true);
        case DECIMAL:
            if (type.getPrecision() == 0)
                return std::make_unique<DecimalHive11ColumnReader>(type, stripe);
            if (type.getPrecision() <= MAX_PRECISION_64)
                return std::make_unique<Decimal64ColumnReader>(type, stripe);
            return std::make_unique<Decimal128ColumnReader>(type, stripe);
        case STRUCT:
            return std::make_unique<StructColumnReader>(type, stripe);
        case LIST:
            return std::make_unique<ListColumnReader>(type, stripe);
        case MAP:
            return std::make_unique<MapColumnReader>(type, stripe);
        case UNION:
            return std::make_unique<UnionColumnReader>(type, stripe);
    }
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading ORC type {} is not supported", type.toString());
}

}
