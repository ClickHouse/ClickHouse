#include <Processors/Formats/Impl/Avro/AvroOcfReader.h>

#if USE_AVRO_RUST

#include <Core/Block.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/Avro/AvroRustPlan.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>

#include <avro_rust.h>

#include <lzma.h>
#include <zlib.h>
#include <zstd.h>

#if USE_BZIP2
#    include <bzlib.h>
#endif

#if USE_SNAPPY
#    include <snappy.h>
#endif

#include <algorithm>
#include <limits>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int AVRO_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

namespace AvroRust
{

namespace
{

constexpr size_t ERROR_CAPACITY = 512;
constexpr size_t SYNC_SIZE = 16;
/// Upper bound of the first allocation for a decompressed block; the buffer then grows only when the decoder fills it.
constexpr size_t MAX_INITIAL_PLAIN_CAPACITY = 64 * 1024;
/// Input is handed to the zlib family and bzip2 in pieces that fit their 32 bit counters.
constexpr size_t MAX_CODEC_CHUNK = 1ULL << 30;
/// Bytes of the next piece of the input that are copied after the bytes of a value that the end of a piece cut, at least.
constexpr size_t LOOKAHEAD_BYTES = 8 * 1024;
/// The decoder is given a piece of the input as it is only when the piece has at least this many bytes. A smaller piece is
/// not worth a call of the decoder, and is copied to a window of `PLAIN_WINDOW_BYTES` with the pieces that follow it.
constexpr size_t MIN_LENT_PIECE_BYTES = 64 * 1024;
/// The cap on the file header (magic, metadata, sync marker) that is parsed: the limit the Rust library applies for
/// `max_header_bytes == 0`, passed explicitly so that the window below can never outgrow it.
constexpr size_t MAX_HEADER_BYTES = 16ULL << 20;
/// Bytes that are already in the input buffer are taken into the header window up to this size even when the
/// parser did not ask for them yet, so that a header that fits one buffer is parsed once.
constexpr size_t HEADER_READ_AHEAD = 64 * 1024;
/// The same memory limit as `LZMAInflatingReadBuffer`.
constexpr uint64_t XZ_MEMORY_LIMIT = 500ULL * 1024 * 1024;

[[noreturn]] void throwAvro(const std::string & message)
{
    throw Exception(ErrorCodes::AVRO_EXCEPTION, "avro::Exception: {}", message);
}

void checkStatus(int32_t status, const char * err)
{
    if (status < 0)
        throwAvro(err);
}

[[noreturn]] void throwUnexpectedStatus(const char * what)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected status from the Avro library in {}", what);
}

struct HeaderDeleter
{
    void operator()(AvroOcfHeader * header) const { avro_ocf_header_free(header); }
};

}

namespace detail
{

/// The compressed payload of the current block: the bytes that follow its header, in the pieces in which the input
/// has them. First the bytes that were read together with the header (they are in the window of the reader), then the
/// buffer of `in`, which is refilled only when a piece was used up. Pieces are handed out without a copy and are
/// valid until `in` is asked for more bytes, which happens in `peek` after all of the pieces were taken, and only then.
class RawPayload
{
public:
    explicit RawPayload(OcfReader & reader_)
        : reader(reader_)
    {
    }

    /// Bytes of the payload that were not handed out.
    UInt64 left() const { return reader.raw_left; }

    /// The next contiguous piece of the payload, not empty unless `left` is 0. A payload that the input cuts short
    /// is an error.
    std::span<const UInt8> peek() const
    {
        if (!reader.raw_left)
            return {};
        if (reader.windowSize())
            return {reader.windowData(), static_cast<size_t>(std::min<UInt64>(reader.windowSize(), reader.raw_left))};
        if (reader.in.eof())
            throwAvro("EOF reached");
        return {
            reinterpret_cast<const UInt8 *>(reader.in.position()),
            static_cast<size_t>(std::min<UInt64>(reader.in.available(), reader.raw_left))};
    }

    /// Takes the first `n` bytes of what `peek` returned.
    void advance(size_t n)
    {
        if (n > reader.raw_left)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot take {} bytes of an Avro payload with {} left", n, reader.raw_left);
        if (reader.windowSize())
            reader.consumeWindow(n);
        else
            reader.in.position() += n;
        reader.raw_left -= n;
    }

private:
    OcfReader & reader;
};

/// The plain bytes of the current block, produced incrementally.
class PlainStream
{
public:
    virtual ~PlainStream() = default;

    /// Appends up to `limit` (not 0) plain bytes to `out`: fewer only when the payload has ended, which `finished`
    /// tells. A payload that is corrupt, or that ends before its compressed stream does, is an error.
    virtual void produce(PODArray<UInt8> & out, size_t limit) = 0;

    /// All the plain bytes were produced.
    bool finished() const { return done; }

protected:
    bool done = false;
};

}

namespace
{

/// Output of a decompressor into the tail of a buffer: grows by doubling only when the decoder has filled it, up to the
/// `limit` of bytes that it may add, so its size is at most twice the bytes actually produced.
class GrowingOutput
{
public:
    GrowingOutput(PODArray<UInt8> & buf_, size_t limit)
        : buf(buf_)
        , used(buf_.size())
        , end(used + limit)
    {
        buf.resize(used + std::min(limit, MAX_INITIAL_PLAIN_CAPACITY));
    }

    bool full() const { return used >= end; }

    /// Makes sure there is room for at least one more byte, unless `full`.
    void ensureSpace()
    {
        if (used == buf.size())
            buf.resize(std::min(end, std::max(used * 2, used + MAX_INITIAL_PLAIN_CAPACITY)));
    }

    UInt8 * tail() { return buf.data() + used; }
    size_t space() const { return buf.size() - used; }
    void produced(size_t n) { used += n; }
    void finish() { buf.resize(used); }

private:
    PODArray<UInt8> & buf;
    size_t used;
    const size_t end;
};

/// Common part of the decoders that read a compressed stream.
class CodecStream : public detail::PlainStream
{
protected:
    explicit CodecStream(detail::RawPayload raw_)
        : raw(raw_)
    {
    }

    /// Makes `pending` the next piece of the payload when the last one was used up. The piece is taken from the
    /// payload at once and stays valid until it is used up, because `in` is not asked for bytes before that.
    void feed()
    {
        if (!pending.empty())
            return;
        pending = raw.peek();
        /// Input is handed to the zlib family and bzip2 in pieces that fit their 32 bit counters.
        if (pending.size() > MAX_CODEC_CHUNK)
            pending = pending.first(MAX_CODEC_CHUNK);
        raw.advance(pending.size());
    }

    /// Every byte of the payload was handed to the decoder and used by it.
    bool inputExhausted() const { return pending.empty() && raw.left() == 0; }

    detail::RawPayload raw;
    std::span<const UInt8> pending;
};

class DeflateStream final : public CodecStream
{
public:
    explicit DeflateStream(detail::RawPayload raw_)
        : CodecStream(raw_)
    {
        if (inflateInit2(&stream, -MAX_WBITS) != Z_OK)
            throwAvro("Cannot initialize the deflate decoder");
    }

    ~DeflateStream() override { inflateEnd(&stream); }

    void produce(PODArray<UInt8> & out, size_t limit) override
    {
        GrowingOutput output(out, limit);
        while (!done && !output.full())
        {
            CurrentThread::checkIfNotCancelled();
            feed();
            output.ensureSpace();

            const size_t out_space = std::min<size_t>(output.space(), std::numeric_limits<uInt>::max());
            const size_t in_size = pending.size();
            stream.next_in = const_cast<Bytef *>(reinterpret_cast<const Bytef *>(pending.data()));
            stream.avail_in = static_cast<uInt>(in_size);
            stream.next_out = reinterpret_cast<Bytef *>(output.tail());
            stream.avail_out = static_cast<uInt>(out_space);

            const int ret = inflate(&stream, Z_NO_FLUSH);
            const size_t produced = out_space - stream.avail_out;
            const size_t consumed = in_size - stream.avail_in;
            output.produced(produced);
            pending = pending.subspan(consumed);

            if (ret == Z_STREAM_END)
            {
                /// Writers like fastavro and pyiceberg produce a block as `zlib.compress(data)[2:-1]`, which leaves the last
                /// 3 bytes of the adler32 trailer after the raw deflate stream, and `contrib/avro` reads such files: it stops at
                /// the end of the stream and never looks at what follows. The rest of the payload was already taken from the
                /// input (`pending`) or is skipped by `finishBlock`, which goes on to the sync marker.
                pending = {};
                done = true;
            }
            else if (ret != Z_OK && ret != Z_BUF_ERROR)
                throwAvro(fmt::format("Cannot read stream: {}", stream.msg ? stream.msg : "corrupted deflate data"));
            else if (produced == 0 && consumed == 0 && inputExhausted())
                throwAvro("Cannot read stream: unexpected end of deflate data");
        }
        output.finish();
    }

private:
    z_stream stream{};
};

class ZstdStream final : public CodecStream
{
public:
    explicit ZstdStream(detail::RawPayload raw_)
        : CodecStream(raw_)
        , dctx(ZSTD_createDCtx(), &ZSTD_freeDCtx)
    {
        if (!dctx)
            throwAvro("Cannot initialize the zstandard decoder");
    }

    void produce(PODArray<UInt8> & out, size_t limit) override
    {
        GrowingOutput output(out, limit);
        while (!done && !output.full())
        {
            CurrentThread::checkIfNotCancelled();
            feed();
            output.ensureSpace();

            ZSTD_inBuffer input{pending.data(), pending.size(), 0};
            ZSTD_outBuffer sink{output.tail(), output.space(), 0};
            const size_t ret = ZSTD_decompressStream(dctx.get(), &sink, &input);
            if (ZSTD_isError(ret))
                throwAvro(fmt::format("Cannot read stream: {}", ZSTD_getErrorName(ret)));
            output.produced(sink.pos);
            pending = pending.subspan(input.pos);

            /// Concatenated frames: a frame ended; stop only when all the input is consumed.
            if (ret == 0 && inputExhausted())
                done = true;
            else if (sink.pos == 0 && input.pos == 0 && inputExhausted())
                throwAvro("Cannot read stream: unexpected end of zstandard data");
        }
        output.finish();
    }

private:
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx;
};

#if USE_BZIP2
class Bzip2Stream final : public CodecStream
{
public:
    explicit Bzip2Stream(detail::RawPayload raw_)
        : CodecStream(raw_)
    {
        init();
    }

    ~Bzip2Stream() override { BZ2_bzDecompressEnd(&stream); }

    void produce(PODArray<UInt8> & out, size_t limit) override
    {
        GrowingOutput output(out, limit);
        while (!done && !output.full())
        {
            CurrentThread::checkIfNotCancelled();
            feed();
            output.ensureSpace();

            const size_t out_space = std::min<size_t>(output.space(), std::numeric_limits<unsigned>::max());
            const size_t in_size = pending.size();
            stream.next_in = const_cast<char *>(reinterpret_cast<const char *>(pending.data()));
            stream.avail_in = static_cast<unsigned>(in_size);
            stream.next_out = reinterpret_cast<char *>(output.tail());
            stream.avail_out = static_cast<unsigned>(out_space);

            const int ret = BZ2_bzDecompress(&stream);
            const size_t produced = out_space - stream.avail_out;
            const size_t consumed = in_size - stream.avail_in;
            output.produced(produced);
            pending = pending.subspan(consumed);

            if (ret == BZ_STREAM_END)
            {
                if (inputExhausted())
                    done = true;
                else
                {
                    /// Concatenated streams: restart the decoder and keep the unread input.
                    BZ2_bzDecompressEnd(&stream);
                    init();
                }
            }
            else if (ret != BZ_OK)
                throwAvro(fmt::format("Cannot read stream: bzip2 error code {}", ret));
            else if (produced == 0 && consumed == 0 && inputExhausted())
                throwAvro("Cannot read stream: bzip2 error: unexpected end of data");
        }
        output.finish();
    }

private:
    void init()
    {
        stream = {};
        const int ret = BZ2_bzDecompressInit(&stream, 0, 0);
        if (ret != BZ_OK)
            throwAvro(fmt::format("Cannot initialize the bzip2 decoder: error code {}", ret));
    }

    bz_stream stream{};
};
#endif

class XzStream final : public CodecStream
{
public:
    explicit XzStream(detail::RawPayload raw_)
        : CodecStream(raw_)
    {
        const lzma_ret init = lzma_stream_decoder(&stream, XZ_MEMORY_LIMIT, LZMA_CONCATENATED);
        if (init != LZMA_OK)
            throwAvro(fmt::format("Cannot initialize the xz decoder: error code {}", static_cast<int>(init)));
    }

    ~XzStream() override { lzma_end(&stream); }

    void produce(PODArray<UInt8> & out, size_t limit) override
    {
        GrowingOutput output(out, limit);
        while (!done && !output.full())
        {
            CurrentThread::checkIfNotCancelled();
            feed();
            output.ensureSpace();

            const size_t out_space = output.space();
            const size_t in_size = pending.size();
            stream.next_in = reinterpret_cast<const uint8_t *>(pending.data());
            stream.avail_in = in_size;
            stream.next_out = reinterpret_cast<uint8_t *>(output.tail());
            stream.avail_out = out_space;

            /// Concatenated streams end only at `LZMA_FINISH`, which says that no input follows the piece.
            const lzma_ret ret = lzma_code(&stream, raw.left() == 0 ? LZMA_FINISH : LZMA_RUN);
            const size_t produced = out_space - stream.avail_out;
            const size_t consumed = in_size - stream.avail_in;
            output.produced(produced);
            pending = pending.subspan(consumed);

            if (ret == LZMA_STREAM_END)
                done = true;
            else if (ret != LZMA_OK && ret != LZMA_BUF_ERROR)
                throwAvro(fmt::format("Cannot read stream: lzma error code {}", static_cast<int>(ret)));
            else if (produced == 0 && consumed == 0 && inputExhausted())
                throwAvro("Cannot read stream: lzma error: unexpected end of data");
        }
        output.finish();
    }

private:
    lzma_stream stream = LZMA_STREAM_INIT;
};

std::unique_ptr<detail::PlainStream> makeStream(UInt32 codec, detail::RawPayload raw)
{
    switch (codec)
    {
        case AVRO_CODEC_DEFLATE:
            return std::make_unique<DeflateStream>(raw);
        case AVRO_CODEC_ZSTANDARD:
            return std::make_unique<ZstdStream>(raw);
        case AVRO_CODEC_BZIP2:
#if USE_BZIP2
            return std::make_unique<Bzip2Stream>(raw);
#else
            throwAvro("bzip2 codec is not supported in this build");
#endif
        case AVRO_CODEC_XZ:
            return std::make_unique<XzStream>(raw);
        default:
            throwAvro(fmt::format("Unknown codec in data file: {}", codec));
    }
}

void inflateSnappy([[maybe_unused]] const UInt8 * data, [[maybe_unused]] size_t size, [[maybe_unused]] PODArray<UInt8> & out_buf)
{
#if USE_SNAPPY
    /// The payload is the raw snappy data followed by the big-endian CRC32 of the plain data.
    if (size < 4)
        throwAvro(fmt::format("Cannot read compressed data, expected at least 4 bytes, got {}", size));
    size_t compressed_size = size - 4;

    const char * compressed = reinterpret_cast<const char *>(data);

    /// `IsValidCompressedBuffer` walks the whole compressed payload without producing output, so a payload
    /// that passes proves the decoded length before anything is allocated from it: the header length alone
    /// is never trusted. It also avoids snappy's scattered writer, which leaks its raw blocks if a tracked
    /// allocation throws.
    if (!snappy::IsValidCompressedBuffer(compressed, compressed_size))
        throwAvro("Snappy Compression reported an error when decompressing");
    size_t plain_size = 0;
    if (!snappy::GetUncompressedLength(compressed, compressed_size, &plain_size))
        throwAvro("Snappy Compression reported an error when decompressing");

    out_buf.clear();
    out_buf.resize(plain_size);
    if (!snappy::RawUncompress(compressed, compressed_size, reinterpret_cast<char *>(out_buf.data())))
        throwAvro("Snappy Compression reported an error when decompressing");

    uLong crc = crc32(0L, Z_NULL, 0);
    for (size_t pos = 0; pos < plain_size;)
    {
        size_t chunk = std::min(plain_size - pos, MAX_CODEC_CHUNK);
        crc = crc32(crc, reinterpret_cast<const Bytef *>(out_buf.data() + pos), static_cast<uInt>(chunk));
        pos += chunk;
    }

    const UInt8 * trailer = data + compressed_size;
    UInt32 expected = (static_cast<UInt32>(trailer[0]) << 24) | (static_cast<UInt32>(trailer[1]) << 16)
        | (static_cast<UInt32>(trailer[2]) << 8) | static_cast<UInt32>(trailer[3]);
    if (static_cast<UInt32>(crc) != expected)
        throwAvro(fmt::format(
            "Checksum did not match for Snappy compression: Expected: {}, computed: {}", expected, static_cast<UInt32>(crc)));
#else
    throwAvro("snappy codec is not supported in this build");
#endif
}

}

OcfReader::OcfReader(ReadBuffer & in_)
    : in(in_)
{
    std::unique_ptr<AvroOcfHeader, HeaderDeleter> header;
    char err[ERROR_CAPACITY];
    size_t target = 4;
    uint64_t consumed = 0;
    for (;;)
    {
        /// The parser restarts from the first byte on every call, so the window must grow geometrically (at least
        /// doubling, bounded by `MAX_HEADER_BYTES`) instead of by the few bytes the parser reports: the total work
        /// is linear in the size of the header.
        fillWindow(target);
        fillWindowFromBuffer(std::min(MAX_HEADER_BYTES, windowSize() + HEADER_READ_AHEAD));

        AvroOcfHeader * raw = nullptr;
        uint64_t needed = 0;
        int32_t status = avro_ocf_header_parse(
            reinterpret_cast<const uint8_t *>(windowData()), windowSize(), windowSize() < target, MAX_HEADER_BYTES, &raw, &consumed,
            &needed, err, sizeof(err));
        checkStatus(status, err);
        if (status == AVRO_OK)
        {
            header.reset(raw);
            break;
        }
        if (status != AVRO_NEED_MORE || needed <= windowSize() || needed > MAX_HEADER_BYTES)
            throwUnexpectedStatus("avro_ocf_header_parse");
        target = std::max<size_t>(needed, std::min(MAX_HEADER_BYTES, windowSize() * 2));
    }
    if (consumed > windowSize())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The Avro file header ended at byte {} of {} read", consumed, windowSize());
    /// Whatever was read past the header is the start of the first block.
    consumeWindow(consumed);

    uint64_t meta_count = avro_ocf_header_meta_count(header.get());
    for (uint64_t i = 0; i < meta_count; ++i)
    {
        const uint8_t * key = nullptr;
        size_t key_len = 0;
        const uint8_t * value = nullptr;
        size_t value_len = 0;
        checkStatus(avro_ocf_header_meta_at(header.get(), i, &key, &key_len, &value, &value_len, err, sizeof(err)), err);
        metadata_[std::string(reinterpret_cast<const char *>(key), key_len)].assign(value, value + value_len);
    }

    const uint8_t * schema_json = nullptr;
    size_t schema_len = 0;
    checkStatus(avro_ocf_header_schema(header.get(), &schema_json, &schema_len, err, sizeof(err)), err);
    schema_.emplace(std::string_view(reinterpret_cast<const char *>(schema_json), schema_len));

    codec = avro_ocf_header_codec(header.get());
    avro_ocf_header_sync(header.get(), reinterpret_cast<uint8_t *>(sync));

    min_record_size = schema_->root().info().min_size;
    /// No finite value exists: nothing can be inferred about the size of a row.
    if (min_record_size == std::numeric_limits<uint64_t>::max())
        min_record_size = 0;
}

OcfReader::~OcfReader() = default;

void OcfReader::fillWindow(size_t target)
{
    while (windowSize() < target && !in.eof())
    {
        compactWindow();
        size_t n = std::min(in.available(), target - windowSize());
        const UInt8 * begin = reinterpret_cast<const UInt8 *>(in.position());
        window.insert(begin, begin + n);
        in.position() += n;
    }
}

void OcfReader::fillWindowFromBuffer(size_t limit)
{
    if (windowSize() >= limit)
        return;
    compactWindow();
    size_t n = std::min(in.available(), limit - windowSize());
    if (n == 0)
        return;
    const UInt8 * begin = reinterpret_cast<const UInt8 *>(in.position());
    window.insert(begin, begin + n);
    in.position() += n;
}

void OcfReader::consumeWindow(size_t n)
{
    if (n > windowSize())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot consume {} bytes of an Avro window of {}", n, windowSize());
    window_begin += n;
    if (window_begin == window.size())
        clearWindow();
}

void OcfReader::compactWindow()
{
    /// Only called to append: the bytes that are still unconsumed are the few of a header the parser cut short.
    if (window_begin)
    {
        window.erase(window.begin(), window.begin() + window_begin);
        window_begin = 0;
    }
}

void OcfReader::clearWindow()
{
    window.clear();
    window_begin = 0;
}

void OcfReader::checkSyncOfCurrentBlock()
{
    char err[ERROR_CAPACITY];
    uint64_t needed = 0;

    /// The window may already hold the marker: it is what was read past the payload.
    fillWindow(SYNC_SIZE);
    int32_t status = avro_ocf_check_sync(
        reinterpret_cast<const uint8_t *>(windowData()),
        windowSize(),
        windowSize() < SYNC_SIZE,
        reinterpret_cast<const uint8_t *>(sync),
        &needed,
        err,
        sizeof(err));
    checkStatus(status, err);
    if (status != AVRO_OK)
        throwUnexpectedStatus("avro_ocf_check_sync");
    consumeWindow(SYNC_SIZE);
}

void OcfReader::finishBlock()
{
    detail::RawPayload raw(*this);
    if (stream && !stream->finished())
    {
        /// A compressed stream is read to its end, so that corrupt data after the last row is an error as it is for a
        /// reader that decompresses the whole block. A null payload cannot be corrupt: it is only skipped.
        while (!stream->finished() && codec != AVRO_CODEC_NULL)
        {
            plain.clear();
            plain_begin = 0;
            producePlain(PLAIN_WINDOW_BYTES);
        }
    }
    /// What is left of the payload is only skipped to reach the sync marker: the unread part of a null payload, or the
    /// bytes after the end of a deflate or xz stream (see `DeflateStream`).
    while (raw.left())
    {
        CurrentThread::checkIfNotCancelled();
        raw.advance(raw.peek().size());
    }
    plain.clear();
    plain_begin = 0;
    stream.reset();
}

void OcfReader::loadSnappyBlock()
{
    /// `snappy_storage` grows with the bytes that really arrive; the declared size is only a limit.
    snappy_storage.clear();
    detail::RawPayload raw(*this);
    while (raw.left())
    {
        const auto piece = raw.peek();
        snappy_storage.insert(piece.data(), piece.data() + piece.size());
        raw.advance(piece.size());
    }

    plain.clear();
    plain_begin = 0;
    inflateSnappy(snappy_storage.data(), snappy_storage.size(), plain);
    plain_total = plain.size();
    plain_complete = true;

    char err[ERROR_CAPACITY];
    checkStatus(avro_ocf_check_object_count(block_rows, plain_total, min_record_size, err, sizeof(err)), err);
}

bool OcfReader::nextBlock()
{
    if (have_block)
    {
        finishBlock();
        checkSyncOfCurrentBlock();
        have_block = false;
    }
    block_rows = 0;
    rows_left = 0;
    raw_left = 0;
    plain.clear();
    plain_begin = 0;
    plain_total = 0;
    plain_complete = false;
    plan_begun = false;
    batch_committed = 0;
    in_decode = false;

    char err[ERROR_CAPACITY];
    AvroBlockHeader block{};
    size_t target = 0;
    for (;;)
    {
        fillWindow(target);
        /// An empty window and an exhausted input is the clean end of the file. A window that already holds bytes
        /// must not block on `in` to find that out: the parser asks for more when it needs it.
        const bool eof = windowSize() < target || (windowSize() == 0 && in.eof());

        uint64_t needed = 0;
        int32_t status = avro_ocf_block_header(
            reinterpret_cast<const uint8_t *>(windowData()),
            windowSize(),
            eof,
            codec,
            min_record_size,
            0,
            &block,
            &needed,
            err,
            sizeof(err));
        checkStatus(status, err);
        if (status == AVRO_END)
        {
            clearWindow();
            return false;
        }
        if (status == AVRO_OK)
            break;
        if (status != AVRO_NEED_MORE || needed <= windowSize())
            throwUnexpectedStatus("avro_ocf_block_header");
        target = needed;
    }

    if (block.header_len > windowSize())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The Avro block header ended at byte {} of {} read", block.header_len, windowSize());

    block_rows = block.object_count;
    rows_left = block.object_count;
    raw_left = block.byte_count;
    have_block = true;

    /// What follows the header in the window (bytes that were read with it) is the start of the payload, and what
    /// follows the payload is the sync marker and the next block: the payload is taken from the window first, then
    /// from `in`, and only as far as the declared size says.
    consumeWindow(block.header_len);

    if (codec == AVRO_CODEC_SNAPPY)
        loadSnappyBlock();
    else if (codec != AVRO_CODEC_NULL)
        stream = makeStream(codec, detail::RawPayload(*this));
    return true;
}

void OcfReader::producePlain(size_t limit)
{
    const size_t before = plain.size();
    stream->produce(plain, limit);
    plain_total += plain.size() - before;
    if (!stream->finished())
        return;

    plain_complete = true;
    /// The count that the header declares must fit the payload, which is only known now.
    char err[ERROR_CAPACITY];
    checkStatus(avro_ocf_check_object_count(block_rows, plain_total, min_record_size, err, sizeof(err)), err);
}

bool OcfReader::fillPlain()
{
    if (plain_complete)
        return false;
    CurrentThread::checkIfNotCancelled();

    if (plain_begin)
    {
        plain.erase(plain.begin(), plain.begin() + plain_begin);
        plain_begin = 0;
    }
    /// A row that does not fit what is there makes the window grow by what is there: the work to decode it again
    /// is then geometric in its size, and the window is at most twice the row.
    const size_t before = plain.size();
    producePlain(std::max(PLAIN_WINDOW_BYTES, before));
    return plain.size() > before;
}

void OcfReader::decodeFromPlain(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing)
{
    if (!plainSize())
        fillPlain();

    /// The decoder takes the payload window by window, and asks for the next when its window ends inside a row: it
    /// then holds on to the row (what it has decoded of it is in the columns), consumes every byte that it is done
    /// with, and the bytes that it still needs, with those that arrived, are its next window. A value that the plan
    /// does not read is skipped as it arrives, so a row never has to be held whole; the window grows (by doubling,
    /// see `fillPlain`) only for a value that is read into a column and does not fit it.
    in_decode = true;
    const UInt64 consumed = plan.decodeStream(
        {std::span<const UInt8>(plainData(), plainSize()), !plain_complete},
        [this](UInt64 used) -> Plan::InputWindow
        {
            if (used > plainSize())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", used, plainSize());
            plain_begin += used;
            CurrentThread::checkIfNotCancelled();
            fillPlain();
            if (!plain_complete && plainSize() == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "An Avro block produced no payload bytes and did not end");
            return {std::span<const UInt8>(plainData(), plainSize()), !plain_complete};
        },
        max_rows,
        columns,
        missing);
    in_decode = false;

    if (consumed > plainSize())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", consumed, plainSize());
    plain_begin += consumed;
}

void OcfReader::decodeFromInput(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing)
{
    detail::RawPayload raw(*this);

    /// The payload of the null codec is plain already, so the decoder is given the bytes where the input has them, and
    /// nothing of what the rows skip, or of what they read, is copied. A window is the next contiguous `piece` of the
    /// payload, which `raw` does not hand out (the input is not asked for more bytes) until the decoder is done with its
    /// bytes. The input cuts the payload into pieces at places that have nothing to do with the rows, so a piece ends
    /// inside a value more often than not, and then the window is not contiguous with the next piece: the bytes that
    /// the decoder has not consumed (`carried`) are copied to the start of `plain`, followed by a copy of the start of the
    /// next piece (`lookahead`), which is as much as the decoder is shown of it. What it consumed of the copy, it
    /// consumed of the piece, which goes on after the copy: the decoder continues from that place of the input, without
    /// the copy, and a value that is not whole in the copy widens it.
    std::span<const UInt8> piece;
    /// The bytes of `plain` at the start of the window: what an earlier call, which ended inside the bytes that it had
    /// taken from the input, left of them.
    size_t carried = plain.size();
    size_t lookahead = 0;
    plain_begin = 0;

    auto current = [&]() -> Plan::InputWindow
    {
        if (!carried)
            return {piece, raw.left() > piece.size()};
        return {std::span<const UInt8>(plain.data(), plain.size()), raw.left() > lookahead};
    };

    /// Appends to `plain`, which holds `carried` bytes, `want` bytes of the pieces that follow what was taken from the
    /// input (fewer if the payload ends): whole pieces (carried too) while they are smaller than what is left to
    /// append, then the start of the piece that has the rest (`lookahead`). When the pieces of the input are smaller
    /// than `MIN_LENT_PIECE_BYTES`, which is the case of an input that hands out few bytes at a time, the window is
    /// `PLAIN_WINDOW_BYTES` of them instead, as it is for the other codecs: a call of the decoder is worth more bytes.
    auto fill = [&](size_t want)
    {
        lookahead = 0;
        const size_t start = plain.size();
        for (;;)
        {
            piece = raw.peek();
            if (plain.size() == start && piece.size() < std::max(want, MIN_LENT_PIECE_BYTES))
                want = std::max(want, PLAIN_WINDOW_BYTES);
            const size_t room = want - (plain.size() - start);
            if (piece.size() >= room || raw.left() == piece.size())
            {
                lookahead = std::min(piece.size(), room);
                plain.insert(piece.data(), piece.data() + lookahead);
                return;
            }
            plain.insert(piece.data(), piece.data() + piece.size());
            carried = plain.size();
            raw.advance(piece.size());
        }
    };

    /// Copies `rest`, the end of the last piece that the decoder was given, to `plain`, and the pieces that follow it.
    auto carry = [&](std::span<const UInt8> rest)
    {
        plain.clear();
        plain.insert(rest.data(), rest.data() + rest.size());
        carried = rest.size();
        raw.advance(rest.size());
        fill(std::max(carried, LOOKAHEAD_BYTES));
    };

    /// What an earlier call left in `plain` is the window until the decoder needs more bytes than it has: the input is
    /// not waited for before that.
    if (!carried)
        piece = raw.peek();

    in_decode = true;
    const UInt64 consumed = plan.decodeStream(
        current(),
        [&](UInt64 used) -> Plan::InputWindow
        {
            CurrentThread::checkIfNotCancelled();
            if (!carried)
            {
                if (used > piece.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", used, piece.size());
                const auto rest = piece.subspan(used);
                raw.advance(used);
                if (rest.empty())
                    piece = raw.peek();
                else
                    carry(rest);
                return current();
            }

            if (used > plain.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", used, plain.size());
            if (used < carried)
            {
                /// The value that the last piece cut is not whole in the copy: widen the copy, by doubling.
                plain.erase(plain.begin(), plain.begin() + used);
                carried -= used;
                if (lookahead < piece.size())
                {
                    const size_t widened = std::min(piece.size(), std::max(2 * lookahead, LOOKAHEAD_BYTES));
                    plain.insert(piece.data() + lookahead, piece.data() + widened);
                    lookahead = widened;
                }
                else
                {
                    /// All of the piece is in `plain` already: it is carried too, and the next pieces follow it.
                    carried += lookahead;
                    raw.advance(piece.size());
                    fill(std::max(carried, LOOKAHEAD_BYTES));
                }
                return current();
            }

            const size_t offset = used - carried;
            const size_t unconsumed = lookahead - offset;
            plain.clear();
            carried = 0;
            lookahead = 0;
            raw.advance(offset);
            piece = raw.peek();
            /// The decoder needs bytes that it has not been shown. If the piece has none, it ends in the same value.
            if (piece.size() <= unconsumed && raw.left() > piece.size())
                carry(piece);
            return current();
        },
        max_rows,
        columns,
        missing);
    in_decode = false;

    if (!carried)
    {
        if (consumed > piece.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", consumed, piece.size());
        raw.advance(consumed);
    }
    else if (consumed >= carried)
    {
        if (consumed > plain.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} bytes of an Avro window of {} were decoded", consumed, plain.size());
        raw.advance(consumed - carried);
        plain.clear();
    }
    else
    {
        /// The rows end inside the bytes that were taken from the input: what follows them is the start of the next call.
        plain.erase(plain.begin(), plain.begin() + consumed);
        plain.resize(carried - consumed);
    }
}

void OcfReader::decodeRows(Plan & plan, UInt64 max_rows, MutableColumns & columns, std::vector<std::vector<UInt8>> & missing)
{
    if (max_rows > rows_left)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot decode {} rows of an Avro block with {} left", max_rows, rows_left);
    if (!plan_begun)
    {
        plan.beginBlock();
        plan_begun = true;
    }
    missing.resize(columns.size());
    for (auto & flags : missing)
        flags.clear();
    batch_committed = 0;
    in_decode = false;

    CurrentThread::checkIfNotCancelled();
    if (codec == AVRO_CODEC_NULL)
        decodeFromInput(plan, max_rows, columns, missing);
    else
        decodeFromPlain(plan, max_rows, columns, missing);
    batch_committed = max_rows;
    rows_left -= max_rows;
}

UInt64 OcfReader::committedRows(const Plan & plan) const
{
    return batch_committed + (in_decode ? plan.committedRows() : 0);
}

void OcfReader::discardRows(UInt64 n)
{
    if (!canCountRows())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Avro rows can be discarded without decoding only in a block of the snappy codec");
    if (n > rows_left)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot discard {} rows of an Avro block with {} left", n, rows_left);
    rows_left -= n;
}

void forEachDecodedBatch(OcfReader & reader, Plan & plan, const Block & header, const DecodedBatchCallback & callback)
{
    std::vector<std::vector<UInt8>> missing;
    while (reader.nextBlock())
    {
        while (reader.rowsLeft())
        {
            const UInt64 batch_rows = std::min<UInt64>(reader.rowsLeft(), CANCELLATION_CHECK_PERIOD_ROWS);
            auto columns = header.cloneEmptyColumns();
            reader.decodeRows(plan, batch_rows, columns, missing);
            callback(columns, missing, batch_rows);
        }
    }
}

}

}

#endif
