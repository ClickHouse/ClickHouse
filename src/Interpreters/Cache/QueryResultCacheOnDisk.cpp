#include <Interpreters/Cache/QueryResultCacheOnDisk.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
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
    extern const int LOGICAL_ERROR;
}

namespace
{

/// On-disk entry layout, version 1:
///
///     Fixed header (FIXED_HEADER_SIZE bytes):
///         char[8]  magic "QRCache1"
///         UInt32   format version
///         UInt32   TCP protocol revision the Native blocks are serialized with
///         UInt64   total entry size in bytes, including the fixed header
///         UInt64   created_at, seconds since epoch
///         UInt64   expires_at, seconds since epoch
///
///     Access metadata (uncompressed):
///         UInt8    is_shared
///         UInt8    has_user_id, [UInt128 user_id]
///         varUInt  number of user roles, [UInt128 role]...
///
///     Result payload (compressed with the codec from setting `query_cache_on_disk_codec`; the compression frames are
///     self-describing, so reading does not depend on the setting):
///         varUInt  number of result chunks
///         Native   header block (zero rows), then one block per result chunk
///         UInt8    has_totals, [Native block]
///         UInt8    has_extremes, [Native block]
constexpr char ENTRY_MAGIC[8] = {'Q', 'R', 'C', 'a', 'c', 'h', 'e', '1'};
constexpr UInt32 ENTRY_FORMAT_VERSION = 1;
constexpr size_t FIXED_HEADER_SIZE = sizeof(ENTRY_MAGIC) + sizeof(UInt32) + sizeof(UInt32) + sizeof(UInt64) + sizeof(UInt64) + sizeof(UInt64);
constexpr size_t TOTAL_SIZE_OFFSET_IN_FIXED_HEADER = sizeof(ENTRY_MAGIC) + sizeof(UInt32) + sizeof(UInt32);

FileCacheKey makeFileCacheKey(const QueryResultCache::Key & key)
{
    /// Salt the hash so that keys of the on-disk query result cache cannot intersect with other keys in the same filesystem cache
    /// (those are typically derived from storage paths).
    SipHash hash;
    hash.update(std::string_view("QueryResultCacheOnDisk"));
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);
    hash.update(key.is_subquery);
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

    if (header.format_version != ENTRY_FORMAT_VERSION
        || header.protocol_revision > DBMS_TCP_PROTOCOL_VERSION
        || header.total_size < FIXED_HEADER_SIZE)
        return std::nullopt;

    return header;
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
        return ProbeResult::Fresh;
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to read an entry header from the on-disk query result cache");
        return ProbeResult::StaleOrUnreadable;
    }
}

bool QueryResultCacheOnDisk::containsFreshEntry(const QueryResultCache::Key & key) const
{
    return probeExistingEntry(makeFileCacheKey(key)) == ProbeResult::Fresh;
}

void QueryResultCacheOnDisk::write(const QueryResultCache::Key & key, const QueryResultCache::Entry & entry) const
{
    const FileCacheKey cache_key = makeFileCacheKey(key);
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
                writer.write(key.header->cloneWithColumns(chunk.getColumns()));

            writeBinaryLittleEndian(static_cast<UInt8>(entry.totals.has_value()), compressed_out);
            if (entry.totals)
                writer.write(key.header->cloneWithColumns(entry.totals->getColumns()));
            writeBinaryLittleEndian(static_cast<UInt8>(entry.extremes.has_value()), compressed_out);
            if (entry.extremes)
                writer.write(key.header->cloneWithColumns(entry.extremes->getColumns()));

            compressed_out.finalize();
        }

        out.finalize();
        data = std::move(out.str());
        unalignedStoreLittleEndian<UInt64>(data.data() + TOTAL_SIZE_OFFSET_IN_FIXED_HEADER, data.size());
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
                holder.reset();
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
        holder.reset();
        file_cache->removeKeyIfExists(cache_key, origin.user_id);
    }
}

QueryResultCacheReader QueryResultCacheOnDisk::createReader(const QueryResultCache::Key & key) const
{
    const auto source = QueryResultCacheReader::Source::OnDisk;

    try
    {
        const FileCacheKey cache_key = makeFileCacheKey(key);
        const auto & user_id = FileCache::getCommonOrigin().user_id;

        auto header_holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, FIXED_HEADER_SIZE, user_id);
        if (header_holder->empty())
        {
            LOG_TRACE(logger, "No query result found on disk for query {}", doubleQuoteString(key.query_string));
            return QueryResultCacheReader(source);
        }

        std::optional<FixedHeader> fixed_header;
        {
            auto in = createReadBufferFromSegments(*header_holder, FIXED_HEADER_SIZE);
            fixed_header = parseFixedHeader(*in);
        }
        header_holder.reset();

        if (!fixed_header)
        {
            LOG_TRACE(logger, "Incompatible query result found on disk for query {}", doubleQuoteString(key.query_string));
            return QueryResultCacheReader(source);
        }

        if (fixed_header->isStale())
        {
            LOG_TRACE(logger, "Stale query result found on disk for query {}", doubleQuoteString(key.query_string));
            return QueryResultCacheReader(source);
        }

        /// Hold all segments of the entry while deserializing it. An entry whose segments were partially evicted is a miss.
        auto holder = file_cache->getDownloadedContiguousOrEmpty(cache_key, 0, fixed_header->total_size, user_id);
        if (holder->empty())
        {
            LOG_TRACE(logger, "Partially evicted query result found on disk for query {}", doubleQuoteString(key.query_string));
            return QueryResultCacheReader(source);
        }

        auto in = createReadBufferFromSegments(*holder, DBMS_DEFAULT_BUFFER_SIZE);
        in->ignore(FIXED_HEADER_SIZE);

        UInt8 is_shared = 0;
        readBinaryLittleEndian(is_shared, *in);
        UInt8 has_user_id = 0;
        readBinaryLittleEndian(has_user_id, *in);
        std::optional<UUID> user_id_of_entry;
        if (has_user_id)
            user_id_of_entry = readUUID(*in);
        size_t num_roles = 0;
        readVarUInt(num_roles, *in);
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
            return QueryResultCacheReader(source);
        }

        CompressedReadBuffer compressed_in(*in);
        NativeReader reader(compressed_in, fixed_header->protocol_revision);

        size_t num_chunks = 0;
        readVarUInt(num_chunks, compressed_in);
        SharedHeader header = std::make_shared<const Block>(reader.read());

        QueryResultCache::Entry entry;
        entry.chunks.reserve(num_chunks);
        for (size_t i = 0; i < num_chunks; ++i)
        {
            Block block = reader.read();
            entry.chunks.emplace_back(block.getColumns(), block.rows());
        }

        UInt8 has_totals = 0;
        readBinaryLittleEndian(has_totals, compressed_in);
        if (has_totals)
        {
            Block block = reader.read();
            entry.totals = Chunk(block.getColumns(), block.rows());
        }
        UInt8 has_extremes = 0;
        readBinaryLittleEndian(has_extremes, compressed_in);
        if (has_extremes)
        {
            Block block = reader.read();
            entry.extremes = Chunk(block.getColumns(), block.rows());
        }

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
        return QueryResultCacheReader(source);
    }
}

}
