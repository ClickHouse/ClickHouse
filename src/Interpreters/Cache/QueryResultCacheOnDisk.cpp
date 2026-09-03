#include <Interpreters/Cache/QueryResultCacheOnDisk.h>

#include <Columns/ColumnConst.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/transformEndianness.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <base/unaligned.h>

#include <cstring>

namespace ProfileEvents
{
    extern const Event QueryCacheOnDiskReadBytes;
    extern const Event QueryCacheOnDiskWrittenBytes;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_reads_from_query_cache_on_disk;
    extern const SettingsBool enable_writes_to_query_cache_on_disk;
    extern const SettingsUInt64 filesystem_cache_reserve_space_wait_lock_timeout_milliseconds;
    extern const SettingsString query_cache_on_disk_cache_name;
    extern const SettingsString query_cache_on_disk_codec;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// On-disk entry layout, version 3:
///
///     Fixed header (FIXED_HEADER_SIZE bytes):
///         char[8]  magic "QRCache1"
///         UInt32   format version
///         UInt32   TCP protocol revision the Native blocks are serialized with
///         UInt64   total entry size in bytes, including the fixed header
///         UInt64   created_at, seconds since epoch
///         UInt64   expires_at, seconds since epoch
///         UInt128  SipHash-128 of everything after the fixed header
///
///     Access metadata (uncompressed):
///         UInt8    is_shared
///         UInt8    has_user_id, [UInt128 user_id]
///         varUInt  number of user roles, [UInt128 role]...
///
///     Result payload (compressed with the codec from setting `query_cache_on_disk_codec`; the compression frames are
///     self-describing, so reading does not depend on the setting):
///         varUInt  number of result chunks
///         Native   header block (zero rows)
///         Chunk    one per result chunk
///         UInt8    has_totals, [Chunk]
///         UInt8    has_extremes, [Chunk]
///
///     Each chunk is serialized per column, so that special column representations survive the round trip: Sparse columns through
///     the custom serialization of the Native format, Const columns through an explicit flag plus their single-row data column:
///         varUInt  number of rows
///         Per column of the header: UInt8 is_const, then a single-column Native block (the data column of a Const column with
///         one row, the column itself with `number of rows` rows otherwise)
constexpr char ENTRY_MAGIC[8] = {'Q', 'R', 'C', 'a', 'c', 'h', 'e', '1'};
constexpr UInt32 ENTRY_FORMAT_VERSION = 3;
constexpr size_t FIXED_HEADER_SIZE
    = sizeof(ENTRY_MAGIC) + sizeof(UInt32) + sizeof(UInt32) + sizeof(UInt64) + sizeof(UInt64) + sizeof(UInt64) + sizeof(UInt128);
constexpr size_t TOTAL_SIZE_OFFSET_IN_FIXED_HEADER = sizeof(ENTRY_MAGIC) + sizeof(UInt32) + sizeof(UInt32);
constexpr size_t BODY_CHECKSUM_OFFSET_IN_FIXED_HEADER = FIXED_HEADER_SIZE - sizeof(UInt128);

/// The key of a shared entry depends on the query only, so that every user can find it. The key of a non-shared entry additionally
/// depends on the access context, so that the entry of one user (or of one role set of the same user) neither shadows nor can be
/// overwritten by the entry of another one. Note that the in-memory query result cache instead keeps a single entry per query and
/// rejects it on read if it turns out to be inaccessible.
FileCacheKey makeFileCacheKey(const QueryResultCache::Key & key, bool is_shared)
{
    /// Salt the hash so that keys of the on-disk query result cache cannot intersect with other keys in the same filesystem cache
    /// (those are typically derived from storage paths).
    SipHash hash;
    hash.update(std::string_view("QueryResultCacheOnDisk"));
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);
    hash.update(key.is_subquery);
    hash.update(is_shared);
    if (!is_shared)
    {
        hash.update(key.user_id.has_value());
        if (key.user_id)
            hash.update(key.user_id->toUnderType());
        /// The roles are compared as an ordered sequence, same as in the in-memory query result cache.
        hash.update(key.current_user_roles.size());
        for (const auto & role : key.current_user_roles)
            hash.update(role.toUnderType());
    }
    return FileCacheKey::fromKey(hash.get128());
}

/// A buffer reading the concatenation of the segments' local files, i.e. the byte range [0, sum of segment sizes) of the entry.
std::unique_ptr<ReadBuffer> createReadBufferFromSegments(const FileSegmentsHolder & holder, size_t buf_size)
{
    ConcatReadBuffer::Buffers buffers;
    for (const auto & file_segment : holder)
        buffers.push_back(std::make_unique<ReadBufferFromFile>(file_segment->getPath(), buf_size));
    return std::make_unique<ConcatReadBuffer>(std::move(buffers));
}

void writeUUID(const UUID & uuid, WriteBuffer & out)
{
    writeBinaryLittleEndian(uuid.toUnderType(), out);
}

UUID readUUID(ReadBuffer & in)
{
    UInt128 raw;
    readBinaryLittleEndian(raw, in);
    return UUID(raw);
}

/// Serializes a chunk column by column, keeping the representation of each column: Sparse columns are kept by the custom
/// serialization of the Native format, Const columns are written as an explicit flag plus their single-row data column.
void writeChunk(const Chunk & chunk, const Block & header, NativeWriter & writer, WriteBuffer & out)
{
    const size_t num_rows = chunk.getNumRows();
    writeVarUInt(num_rows, out);

    const Columns & columns = chunk.getColumns();
    chassert(columns.size() == header.columns());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        const auto * column_const = typeid_cast<const ColumnConst *>(columns[i].get());
        writeBinaryLittleEndian(static_cast<UInt8>(column_const != nullptr), out);

        ColumnWithTypeAndName column = header.getByPosition(i);
        column.column = column_const ? column_const->getDataColumnPtr() : columns[i];

        Block block;
        block.insert(std::move(column));
        writer.write(block);
    }
}

Chunk readChunk(size_t num_columns, NativeReader & reader, ReadBuffer & in)
{
    size_t num_rows = 0;
    readVarUInt(num_rows, in);

    Columns columns;
    columns.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        UInt8 is_const = 0;
        readBinaryLittleEndian(is_const, in);

        Block block = reader.read();
        if (block.columns() != 1 || block.rows() != (is_const ? 1 : num_rows))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed chunk in an entry of the on-disk query result cache");

        ColumnPtr column = block.getByPosition(0).column;
        if (is_const)
            column = ColumnConst::create(std::move(column), num_rows);
        columns.push_back(std::move(column));
    }

    return Chunk(std::move(columns), num_rows);
}

}

bool QueryResultCacheOnDisk::FixedHeader::isStale() const
{
    return std::chrono::system_clock::from_time_t(expires_at) < std::chrono::system_clock::now();
}

QueryResultCacheOnDisk::QueryResultCacheOnDisk(
    FileCachePtr file_cache_,
    bool enable_reads_,
    bool enable_writes_,
    const String & codec_name_,
    size_t reserve_space_lock_wait_timeout_milliseconds_)
    : file_cache(file_cache_)
    , enable_reads(enable_reads_)
    , enable_writes(enable_writes_)
    , codec_name(codec_name_)
    , reserve_space_lock_wait_timeout_milliseconds(reserve_space_lock_wait_timeout_milliseconds_)
{
}

std::shared_ptr<const QueryResultCacheOnDisk> QueryResultCacheOnDisk::getFromSettings(const Settings & settings)
{
    const String & cache_name = settings[Setting::query_cache_on_disk_cache_name].value;
    if (cache_name.empty())
        return nullptr;

    const bool enable_reads = settings[Setting::enable_reads_from_query_cache_on_disk];
    const bool enable_writes = settings[Setting::enable_writes_to_query_cache_on_disk];
    if (!enable_reads && !enable_writes)
        return nullptr;

    FileCachePtr file_cache = FileCacheFactory::instance().get(cache_name); /// throws if no filesystem cache with this name exists

    if (!file_cache->isInitialized())
    {
        LOG_DEBUG(getLogger("QueryResultCacheOnDisk"),
            "Filesystem cache {} is not initialized yet, the on-disk query result cache is not used", backQuote(cache_name));
        return nullptr;
    }

    const String & codec_name = settings[Setting::query_cache_on_disk_codec].value;
    if (enable_writes)
        CompressionCodecFactory::instance().get(codec_name); /// validate the codec early, a misconfiguration must fail the query

    return std::make_shared<const QueryResultCacheOnDisk>(
        file_cache, enable_reads, enable_writes, codec_name,
        settings[Setting::filesystem_cache_reserve_space_wait_lock_timeout_milliseconds]);
}

std::optional<QueryResultCacheOnDisk::FixedHeader> QueryResultCacheOnDisk::parseFixedHeader(ReadBuffer & in)
{
    char magic[sizeof(ENTRY_MAGIC)];
    in.readStrict(magic, sizeof(magic));
    if (memcmp(magic, ENTRY_MAGIC, sizeof(magic)) != 0)
        return std::nullopt;

    QueryResultCacheOnDisk::FixedHeader header;
    readBinaryLittleEndian(header.format_version, in);
    readBinaryLittleEndian(header.protocol_revision, in);
    readBinaryLittleEndian(header.total_size, in);
    readBinaryLittleEndian(header.created_at, in);
    readBinaryLittleEndian(header.expires_at, in);
    readBinaryLittleEndian(header.body_checksum, in);

    if (header.format_version != ENTRY_FORMAT_VERSION
        || header.protocol_revision > DBMS_TCP_PROTOCOL_VERSION
        || header.total_size < FIXED_HEADER_SIZE)
        return std::nullopt;

    return header;
}

std::optional<String> QueryResultCacheOnDisk::readCheckedBody(const FileSegmentsHolder & holder, const FixedHeader & header)
{
    /// The caller has verified that all `total_size` bytes of the entry are downloaded.
    String body;
    body.resize(header.total_size - FIXED_HEADER_SIZE);
    auto in = createReadBufferFromSegments(holder, DBMS_DEFAULT_BUFFER_SIZE);
    in->ignore(FIXED_HEADER_SIZE);
    in->readStrict(body.data(), body.size());

    if (sipHash128(body.data(), body.size()) != header.body_checksum)
        return std::nullopt;

    return body;
}

QueryResultCacheOnDisk::ProbeResult QueryResultCacheOnDisk::probeExistingEntry(const FileCacheKey & cache_key) const
{
    const auto & user_id = FileCache::getCommonOrigin().user_id;

    auto holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, FIXED_HEADER_SIZE, user_id);
    if (holder->empty())
        return ProbeResult::None;

    try
    {
        auto in = createReadBufferFromSegments(*holder, FIXED_HEADER_SIZE);
        auto header = parseFixedHeader(*in);
        if (!header || header->isStale())
            return ProbeResult::StaleOrUnreadable;

        /// An entry can lose a segment after its header was read. Do not let such an entry prevent its replacement:
        /// `tryCreateReader` would treat it as a miss once it requests all `total_size` bytes.
        holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, header->total_size, user_id);
        if (holder->empty())
            return ProbeResult::StaleOrUnreadable;

        /// The body must be validated here and not only on the read path: with `enable_reads_from_query_cache_on_disk = 0`, or
        /// when the query hits the in-memory cache before the disk is consulted, nothing would ever notice that the body is
        /// corrupt, and every write would keep skipping the broken entry until it expires or is evicted.
        if (!readCheckedBody(*holder, *header))
            return ProbeResult::StaleOrUnreadable;

        return ProbeResult::Fresh;
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to read an entry header from the on-disk query result cache");
        return ProbeResult::StaleOrUnreadable;
    }
}

void QueryResultCacheOnDisk::write(const QueryResultCache::Key & key, const QueryResultCache::Entry & entry) const
{
    const FileCacheKey cache_key = makeFileCacheKey(key, key.is_shared);
    const auto & origin = FileCache::getCommonOrigin();

    switch (probeExistingEntry(cache_key))
    {
        case ProbeResult::Fresh:
        {
            LOG_TRACE(logger, "Skipped insert into the on-disk query result cache because it contains a non-stale query result for query {}",
                doubleQuoteString(key.query_string));
            return;
        }
        case ProbeResult::StaleOrUnreadable:
        {
            file_cache->removeKeyIfExists(cache_key, origin.user_id);
            break;
        }
        case ProbeResult::None:
            break;
    }

    /// Serialize the entry into memory first: the total size must be known upfront to create the file segments, and the entry is
    /// fully buffered in memory at this point anyway.
    String data;
    try
    {
        WriteBufferFromOwnString out;

        out.write(ENTRY_MAGIC, sizeof(ENTRY_MAGIC));
        writeBinaryLittleEndian(ENTRY_FORMAT_VERSION, out);
        writeBinaryLittleEndian(static_cast<UInt32>(DBMS_TCP_PROTOCOL_VERSION), out);
        writeBinaryLittleEndian(static_cast<UInt64>(0), out); /// total size, patched below
        writeBinaryLittleEndian(static_cast<UInt64>(std::chrono::system_clock::to_time_t(key.created_at)), out);
        writeBinaryLittleEndian(static_cast<UInt64>(std::chrono::system_clock::to_time_t(key.expires_at)), out);
        writeBinaryLittleEndian(UInt128(0), out); /// body checksum, patched below

        writeBinaryLittleEndian(static_cast<UInt8>(key.is_shared), out);
        writeBinaryLittleEndian(static_cast<UInt8>(key.user_id.has_value()), out);
        if (key.user_id)
            writeUUID(*key.user_id, out);
        writeVarUInt(key.current_user_roles.size(), out);
        for (const auto & role : key.current_user_roles)
            writeUUID(role, out);

        {
            CompressedWriteBuffer compressed_out(out, CompressionCodecFactory::instance().get(codec_name));
            NativeWriter writer(compressed_out, DBMS_TCP_PROTOCOL_VERSION, key.header);

            writeVarUInt(entry.chunks.size(), compressed_out);
            writer.write(*key.header);
            for (const auto & chunk : entry.chunks)
                writeChunk(chunk, *key.header, writer, compressed_out);

            writeBinaryLittleEndian(static_cast<UInt8>(entry.totals.has_value()), compressed_out);
            if (entry.totals)
                writeChunk(*entry.totals, *key.header, writer, compressed_out);
            writeBinaryLittleEndian(static_cast<UInt8>(entry.extremes.has_value()), compressed_out);
            if (entry.extremes)
                writeChunk(*entry.extremes, *key.header, writer, compressed_out);

            compressed_out.finalize();
        }

        out.finalize();
        data = std::move(out.str());
        unalignedStoreLittleEndian<UInt64>(data.data() + TOTAL_SIZE_OFFSET_IN_FIXED_HEADER, data.size());

        UInt128 body_checksum = sipHash128(data.data() + FIXED_HEADER_SIZE, data.size() - FIXED_HEADER_SIZE);
        transformEndianness<std::endian::little>(body_checksum);
        memcpy(data.data() + BODY_CHECKSUM_OFFSET_IN_FIXED_HEADER, &body_checksum, sizeof(body_checksum));
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to serialize a query result for the on-disk query result cache");
        return;
    }

    /// Create all file segments of the entry at once. This fails gracefully if another query created an entry for the same key in
    /// the meantime (then we simply skip the insert), and guarantees that we exclusively own all segments of the key while writing.
    /// A reader sees the incomplete entry as a miss because not all of its bytes are downloaded yet.
    auto holder = file_cache->trySet(cache_key, 0, data.size(), CreateFileSegmentSettings(FileSegmentKind::Regular), origin);
    if (!holder)
    {
        LOG_TRACE(logger, "Skipped insert into the on-disk query result cache because of a concurrent insert for query {}",
            doubleQuoteString(key.query_string));
        return;
    }

    try
    {
        size_t offset = 0;
        while (!holder->empty())
        {
            FileSegment & file_segment = holder->front();
            chassert(file_segment.range().left == offset);

            if (file_segment.getOrSetDownloader() != FileSegment::getCallerId())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set a downloader for a newly created file segment: {}",
                    file_segment.getInfoForLog());

            const size_t bytes_to_write = file_segment.range().size();
            chassert(offset + bytes_to_write <= data.size());

            std::string failure_reason;
            if (!file_segment.reserve(bytes_to_write, reserve_space_lock_wait_timeout_milliseconds, failure_reason))
            {
                LOG_TRACE(logger, "Skipped insert into the on-disk query result cache because space reservation failed: {}, query: {}",
                    failure_reason, doubleQuoteString(key.query_string));
                holder = nullptr;
                file_cache->removeKeyIfExists(cache_key, origin.user_id);
                return;
            }

            file_segment.write(data.data() + offset, bytes_to_write, offset);
            file_segment.completePartAndResetDownloader();
            offset += bytes_to_write;

            holder->completeAndPopFront(/*allow_background_download=*/false, /*force_shrink_to_downloaded_size=*/false);
        }
        chassert(offset == data.size());

        ProfileEvents::increment(ProfileEvents::QueryCacheOnDiskWrittenBytes, data.size());
        LOG_TRACE(logger, "Stored query result of query {} on disk ({} bytes)", doubleQuoteString(key.query_string), data.size());
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to write a query result into the on-disk query result cache");
        holder = nullptr;
        file_cache->removeKeyIfExists(cache_key, origin.user_id);
    }
}

QueryResultCacheReader QueryResultCacheOnDisk::createReader(const QueryResultCache::Key & key) const
{
    /// A non-shared entry is stored under a key which covers the access context, a shared entry under a key which does not, and the
    /// reader cannot know which of the two a writer created (`key.is_shared` is not part of a key constructed for reading). So look
    /// for an entry of the current user first and for a shared entry second.
    if (auto reader = tryCreateReader(key, makeFileCacheKey(key, /*is_shared=*/false)))
        return std::move(*reader);

    if (auto reader = tryCreateReader(key, makeFileCacheKey(key, /*is_shared=*/true)))
        return std::move(*reader);

    LOG_TRACE(logger, "No query result found on disk for query {}", doubleQuoteString(key.query_string));
    return QueryResultCacheReader(QueryResultCacheReader::Source::OnDisk);
}

std::optional<QueryResultCacheReader> QueryResultCacheOnDisk::tryCreateReader(const QueryResultCache::Key & key, const FileCacheKey & cache_key) const
{
    const auto source = QueryResultCacheReader::Source::OnDisk;

    try
    {
        const auto & user_id = FileCache::getCommonOrigin().user_id;

        auto header_holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, FIXED_HEADER_SIZE, user_id);
        if (header_holder->empty())
            return std::nullopt;

        std::optional<FixedHeader> fixed_header;
        {
            auto in = createReadBufferFromSegments(*header_holder, FIXED_HEADER_SIZE);
            fixed_header = parseFixedHeader(*in);
        }
        header_holder = nullptr;

        if (!fixed_header)
        {
            LOG_TRACE(logger, "Incompatible query result found on disk for query {}", doubleQuoteString(key.query_string));
            return std::nullopt;
        }

        if (fixed_header->isStale())
        {
            LOG_TRACE(logger, "Stale query result found on disk for query {}", doubleQuoteString(key.query_string));
            return std::nullopt;
        }

        /// Hold all segments of the entry while deserializing it. An entry whose segments were partially evicted is a miss.
        auto holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, fixed_header->total_size, user_id);
        if (holder->empty())
        {
            LOG_TRACE(logger, "Partially evicted query result found on disk for query {}", doubleQuoteString(key.query_string));
            return std::nullopt;
        }

        auto body = readCheckedBody(*holder, *fixed_header);
        if (!body)
        {
            LOG_TRACE(logger, "Corrupt query result found on disk for query {}", doubleQuoteString(key.query_string));
            /// The body is not what was written, so the entry is useless. Drop it, so that the next write stores a fresh one.
            file_cache->removeKeyIfExists(cache_key, user_id);
            return std::nullopt;
        }

        ReadBufferFromString body_in(*body);
        ReadBuffer * in = &body_in;

        UInt8 is_shared = 0;
        readBinaryLittleEndian(is_shared, *in);
        UInt8 has_user_id = 0;
        readBinaryLittleEndian(has_user_id, *in);
        std::optional<UUID> user_id_of_entry;
        if (has_user_id)
            user_id_of_entry = readUUID(*in);
        size_t num_roles = 0;
        readVarUInt(num_roles, *in);
        /// The access metadata is stored uncompressed, so the roles must fit into the entry. Checking that protects against
        /// a huge allocation when the counter in a corrupt entry is nonsense.
        if (num_roles > (fixed_header->total_size - FIXED_HEADER_SIZE) / sizeof(UUID))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Malformed access metadata in an entry of the on-disk query result cache (number of roles: {})", num_roles);
        std::vector<UUID> roles_of_entry;
        roles_of_entry.reserve(num_roles);
        for (size_t i = 0; i < num_roles; ++i)
            roles_of_entry.push_back(readUUID(*in));

        const bool is_same_user_id = ((!user_id_of_entry.has_value() && !key.user_id.has_value())
            || (user_id_of_entry.has_value() && key.user_id.has_value() && *user_id_of_entry == *key.user_id));
        const bool is_same_current_user_roles = (roles_of_entry == key.current_user_roles);
        if (!is_shared && (!is_same_user_id || !is_same_current_user_roles))
        {
            LOG_TRACE(logger, "Inaccessible query result found on disk for query {}", doubleQuoteString(key.query_string));
            return std::nullopt;
        }

        CompressedReadBuffer compressed_in(*in);
        NativeReader reader(compressed_in, fixed_header->protocol_revision);

        size_t num_chunks = 0;
        readVarUInt(num_chunks, compressed_in);
        SharedHeader header = std::make_shared<const Block>(reader.read());

        QueryResultCache::Entry entry;
        /// The number of chunks is read from the compressed stream, so it cannot be validated against the entry size upfront.
        /// Cap the pre-allocation instead: a corrupt counter then fails cheaply while deserializing the chunks, rather than
        /// forcing a huge allocation here. A legitimate entry with more chunks simply grows the vector.
        entry.chunks.reserve(std::min<size_t>(num_chunks, 65536));
        for (size_t i = 0; i < num_chunks; ++i)
            entry.chunks.push_back(readChunk(header->columns(), reader, compressed_in));

        UInt8 has_totals = 0;
        readBinaryLittleEndian(has_totals, compressed_in);
        if (has_totals)
            entry.totals = readChunk(header->columns(), reader, compressed_in);
        UInt8 has_extremes = 0;
        readBinaryLittleEndian(has_extremes, compressed_in);
        if (has_extremes)
            entry.extremes = readChunk(header->columns(), reader, compressed_in);

        /// Treat the read as an access in the eviction policy of the filesystem cache, so that hot entries stay cached.
        for (const auto & file_segment : *holder)
            file_segment->increasePriority();

        ProfileEvents::increment(ProfileEvents::QueryCacheOnDiskReadBytes, fixed_header->total_size);
        LOG_TRACE(logger, "Query result found on disk for query {}", doubleQuoteString(key.query_string));

        return QueryResultCacheReader(
            source,
            header,
            std::move(entry),
            std::chrono::system_clock::from_time_t(fixed_header->created_at),
            std::chrono::system_clock::from_time_t(fixed_header->expires_at));
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to read a query result from the on-disk query result cache");
        try
        {
            /// The write path probes only the fixed header, so an entry with a healthy header but an unreadable body would
            /// never be replaced until it expires. Drop it here so that the next write stores a fresh entry.
            file_cache->removeKeyIfExists(cache_key, FileCache::getCommonOrigin().user_id);
        }
        catch (...)
        {
            tryLogCurrentException(logger, "Failed to remove an unreadable entry from the on-disk query result cache");
        }
        return std::nullopt;
    }
}

}
