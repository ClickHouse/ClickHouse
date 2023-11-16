#include <Storages/MergeTree/DataPartsExchange.h>

#include "base/FnTraits.h"
#include "config.h"

#include <Formats/NativeWriter.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPCommon.h>
#include <IO/S3Common.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <base/scope_guard.h>
#include <Poco/Net/HTTPRequest.h>
#include <boost/algorithm/string/join.hpp>
#include <iterator>
#include <base/sort.h>


namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric ReplicatedSend;
    extern const Metric ReplicatedFetch;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_DATA_PART;
    extern const int ABORTED;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int INSECURE_PATH;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int S3_ERROR;
    extern const int INCORRECT_PART_TYPE;
    extern const int ZERO_COPY_REPLICATION_ERROR;
}

namespace DataPartsExchange
{

namespace
{
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE = 1;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS = 2;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE = 3;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_DEFAULT_COMPRESSION = 4;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID = 5;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY = 6;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION = 7;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION = 8;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_OBJECT_STORAGE_VFS = 9;

constexpr auto OBJECT_STORAGE_VFS = "disk_vfs";
constexpr auto ZERO_COPY = "remote_fs_metadata";

std::string getEndpointId(const std::string & node_id)
{
    return "DataPartsExchange:" + node_id;
}

/// Simple functor for tracking fetch progress in system.replicated_fetches table.
struct ReplicatedFetchReadCallback
{
    ReplicatedFetchList::Entry & replicated_fetch_entry;

    explicit ReplicatedFetchReadCallback(ReplicatedFetchList::Entry & replicated_fetch_entry_)
        : replicated_fetch_entry(replicated_fetch_entry_)
    {}

    void operator() (size_t bytes_count)
    {
        replicated_fetch_entry->bytes_read_compressed.store(bytes_count, std::memory_order_relaxed);

        /// It's possible when we fetch part from very old clickhouse version
        /// which doesn't send total size.
        if (replicated_fetch_entry->total_size_bytes_compressed != 0)
        {
            replicated_fetch_entry->progress.store(
                    static_cast<double>(bytes_count) / replicated_fetch_entry->total_size_bytes_compressed,
                    std::memory_order_relaxed);
        }
    }
};

}


Service::Service(StorageReplicatedMergeTree & data_)
    : data(data_)
    , log(&Poco::Logger::get(data.getStorageID().getNameForLogs() + " (Replicated PartsService)"))
{}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

Strings parseCommaDelimited(const String& str)
{
    std::regex re("\\s*,\\s*");
    return {
        std::sregex_token_iterator(str.begin(), str.end(), re, -1),
        std::sregex_token_iterator()};
}

bool contains(auto && range, auto && value)
{
    return std::find(range.begin(), range.end(), value) != range.end();
}

void Service::processQuery(const HTMLForm & params, ReadBuffer & /*body*/, WriteBuffer & out, HTTPServerResponse & response)
{
    int client_protocol_version = parse<int>(params.get("client_protocol_version", "0"));

    String part_name = params.get("part");

    const auto data_settings = data.getSettings();

    /// Validation of the input that may come from malicious replica.
    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    /// We pretend to work as older server version, to be sure that client will correctly process our version
    response.addCookie({"server_protocol_version", toString(std::min(
        client_protocol_version,
        REPLICATION_PROTOCOL_VERSION_WITH_OBJECT_STORAGE_VFS))});

    LOG_TRACE(log, "Sending part {}", part_name);

    MergeTreeData::DataPartPtr part;

    auto report_broken_part = [&]()
    {
        if (part)
            data.reportBrokenPart(part);
        else
            LOG_TRACE(log, "Part {} was not found, do not report it as broken", part_name);
    };

    try
    {
        part = findPart(part_name);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

        if (part->getDataPartStorage().isStoredOnRemoteDisk())
        {
            UInt64 revision = parse<UInt64>(params.get("disk_revision", "0"));
            if (revision)
                part->getDataPartStorage().syncRevision(revision);

            revision = part->getDataPartStorage().getRevision();
            if (revision)
                response.addCookie({"disk_revision", toString(revision)});
        }

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
            writeBinary(part->checksums.getTotalSizeOnDisk(), out);

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            WriteBufferFromOwnString ttl_infos_buffer;
            part->ttl_infos.write(ttl_infos_buffer);
            writeBinary(ttl_infos_buffer.str(), out);
        }

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
            writeStringBinary(part->getType().toString(), out);

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
            writeUUIDText(part->uuid, out);

        bool send_projections = client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION;

        if (send_projections)
        {
            const auto & projections = part->getProjectionParts();
            writeBinary(projections.size(), out);
        }

        const auto& part_storage = part->getDataPartStorage();
        const String disk_type = part_storage.getDiskType();

        const Strings desired_zero_copy_disk_types = parseCommaDelimited(params.get(ZERO_COPY, ""));
        if (data_settings->allow_remote_fs_zero_copy_replication
            && !isInMemoryPart(part)
            && client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY
            && contains(desired_zero_copy_disk_types, disk_type))
        {
            response.addCookie({ZERO_COPY, disk_type});
            sendPartFromDisk(part, out, client_protocol_version,
                /* from_disk_with_zero_copy=*/ true,
                send_projections,
                /* verify_checksums=*/false);
            return;
        }

        const Strings desired_vfs_disk_types = parseCommaDelimited(params.get(OBJECT_STORAGE_VFS, ""));
        bool verify_checksums = true;
        if (data_settings->allow_object_storage_vfs
            && !isInMemoryPart(part)
            && client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_OBJECT_STORAGE_VFS
            && contains(desired_vfs_disk_types, disk_type))
        {
            response.addCookie({OBJECT_STORAGE_VFS, disk_type});
            verify_checksums = false;
        }

        if (isInMemoryPart(part))
            sendPartFromMemory(part, out, send_projections);
        else
            sendPartFromDisk(part, out, client_protocol_version,
                false, send_projections, verify_checksums);

        data.addLastSentPart(part->info);
    }
    catch (const NetException &)
    {
        /// Network error or error on remote side. No need to enqueue part for check.
        throw;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ABORTED && e.code() != ErrorCodes::CANNOT_WRITE_TO_OSTREAM)
            report_broken_part();

        throw;
    }
    catch (...)
    {
        report_broken_part();
        throw;
    }
}

void Service::sendPartFromMemory(
    const MergeTreeData::DataPartPtr & part, WriteBuffer & out, bool send_projections)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    if (send_projections)
    {
        for (const auto & [name, projection] : part->getProjectionParts())
        {
            auto projection_sample_block = metadata_snapshot->projections.get(name).sample_block;
            auto part_in_memory = asInMemoryPart(projection);
            if (!part_in_memory)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection {} of part {} is not stored in memory", name, part->name);

            writeStringBinary(name, out);
            projection->checksums.write(out);
            NativeWriter block_out(out, 0, projection_sample_block);
            block_out.write(part_in_memory->block);
        }
    }

    auto part_in_memory = asInMemoryPart(part);
    if (!part_in_memory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} is not stored in memory", part->name);

    NativeWriter block_out(out, 0, metadata_snapshot->getSampleBlock());
    part->checksums.write(out);
    block_out.write(part_in_memory->block);

    data.getSendsThrottler()->add(part_in_memory->block.bytes());
}

MergeTreeData::DataPart::Checksums Service::sendPartFromDisk(
    const MergeTreeData::DataPartPtr & part,
    WriteBuffer & out,
    int client_protocol_version,
    bool from_disk_with_zero_copy,
    bool send_projections,
    bool verify_checksums)
{
    NameSet files_to_replicate;
    auto file_names_without_checksums = part->getFileNamesWithoutChecksums();

    for (const auto & [name, _] : part->checksums.files)
    {
        if (endsWith(name, ".proj"))
            continue;

        files_to_replicate.insert(name);
    }

    for (const auto & name : file_names_without_checksums)
    {
        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_DEFAULT_COMPRESSION
            && name == IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME)
            continue;

        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION
            && name == IMergeTreeDataPart::METADATA_VERSION_FILE_NAME)
            continue;

        files_to_replicate.insert(name);
    }

    auto data_part_storage = part->getDataPartStoragePtr();
    const IDataPartStorage::ReplicatedFilesDescription replicated_description =
        data_part_storage->getReplicatedFilesDescription(files_to_replicate, from_disk_with_zero_copy);

    if (from_disk_with_zero_copy && !part->isProjectionPart())
        writeStringBinary(replicated_description.unique_id, out);

    MergeTreeData::DataPart::Checksums data_checksums;
    for (const auto & [name, projection] : part->getProjectionParts())
    {
        if (send_projections)
        {
            writeStringBinary(name, out);
            MergeTreeData::DataPart::Checksums projection_checksum = sendPartFromDisk(
                projection, out, client_protocol_version,
                from_disk_with_zero_copy, false, verify_checksums);
            data_checksums.addFile(name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
        }
        else if (part->checksums.has(name + ".proj"))
        {
            // We don't send this projection, just add out checksum to bypass the following check
            const auto & our_checksum = part->checksums.files.find(name + ".proj")->second;
            data_checksums.addFile(name + ".proj", our_checksum.file_size, our_checksum.file_hash);
        }
    }

    writeBinary(replicated_description.files.size(), out);
    for (const auto & [file_name, desc] : replicated_description.files)
    {
        writeStringBinary(file_name, out);
        writeBinary(desc.file_size, out);

        auto file_in = desc.input_buffer_getter();
        HashingWriteBuffer hashing_out(out);
        copyDataWithThrottler(*file_in, hashing_out, blocker.getCounter(), data.getSendsThrottler());
        hashing_out.finalize();

        if (blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

        if (hashing_out.count() != desc.file_size)
            throw Exception(
                ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                "Unexpected size of file {}, expected {} got {}",
                std::string(fs::path(part->getDataPartStorage().getRelativePath()) / file_name),
                desc.file_size, hashing_out.count());

        writePODBinary(hashing_out.getHash(), out);

        if (!file_names_without_checksums.contains(file_name))
            data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
    }

    if (verify_checksums && isFullPartStorage(part->getDataPartStorage()))
        part->checksums.checkEqual(data_checksums, false);

    return data_checksums;
}

bool wait_loop(UInt32 wait_timeout_ms, const std::function<bool()> & pred)
{
    static const UInt32 loop_delay_ms = 5;

    /// this is sleep-based wait, it has to be short
    chassert(wait_timeout_ms < 2000);

    if (pred())
        return true;

    Stopwatch timer;
    sleepForMilliseconds(loop_delay_ms);
    while (!pred() && timer.elapsedMilliseconds() < wait_timeout_ms)
    {
        sleepForMilliseconds(loop_delay_ms);
    }

    return pred();
}

MergeTreeData::DataPartPtr Service::findPart(const String & name)
{
    /// It is important to include Outdated parts here because remote replicas cannot reliably
    /// determine the local state of the part, so queries for the parts in these states are completely normal.
    MergeTreeData::DataPartPtr part;

    part = data.getPartIfExists(name, {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in table", name);

    bool zero_copy_enabled = data.getSettings()->allow_remote_fs_zero_copy_replication;
    if (!zero_copy_enabled)
        return part;

    /// Ephemeral zero-copy lock may be lost for PreActive parts
    /// do not expose PreActive parts for zero-copy

    static const UInt32 wait_timeout_ms = 1000;
    auto pred = [&] ()
    {
        auto lock = data.lockParts();
        return part->getState() != MergeTreeDataPartState::PreActive;
    };

    bool pred_result = wait_loop(wait_timeout_ms, pred);
    if (!pred_result)
        throw Exception(
                ErrorCodes::ABORTED,
                "Could not exchange part {} as it's in preActive state ({} ms) and it uses zero copy replication. "
                "This is expected behaviour and the client will retry fetching the part automatically.",
                name, wait_timeout_ms);

    return part;
}

Fetcher::Fetcher(StorageReplicatedMergeTree & data_)
    : data(data_)
    , log(&Poco::Logger::get(data.getStorageID().getNameForLogs() + " (Fetcher)"))
{}

Strings getCapableDiskTypes(DiskPtr disk, const Disks& other, Fn<bool(DiskPtr)> auto && capability_checker)
{
    Strings out;
    if (!disk)
    {
        for (const auto & data_disk : other)
            if (capability_checker(data_disk))
                out.emplace_back(toString(data_disk->getDataSourceDescription().type));

        std::sort(out.begin(), out.end());
        out.erase(std::unique(out.begin(), out.end()), out.end());
    }
    else if (capability_checker(disk))
        out.emplace_back(toString(disk->getDataSourceDescription().type));

    return out;
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> Fetcher::fetchSelectedPart(
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const String & part_name,
    const String & zookeeper_name,
    const String & replica_path,
    const String & host,
    int port,
    const ConnectionTimeouts & timeouts,
    const String & user,
    const String & password,
    const String & interserver_scheme,
    ThrottlerPtr throttler,
    bool to_detached,
    const String & tmp_prefix_,
    std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
    bool try_zero_copy,
    DiskPtr disk)
{
    if (blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Fetching of part was cancelled");

    const auto data_settings = data.getSettings();

    if (data.canUseZeroCopyReplication() && !try_zero_copy)
        LOG_INFO(log, "Zero copy replication enabled, but trying to fetch part {} without zero copy", part_name);

    /// It should be "tmp-fetch_" and not "tmp_fetch_", because we can fetch part to detached/,
    /// but detached part name prefix should not contain underscore.
    static const String TMP_PREFIX = "tmp-fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;
    String part_dir = tmp_prefix + part_name;
    auto temporary_directory_lock = data.getTemporaryPartDirectoryHolder(part_dir);

    /// Validation of the input that may come from malicious replica.
    auto part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);

    String endpoint_id = getEndpointId(
        data_settings->enable_the_endpoint_id_with_zookeeper_name_prefix ?
        zookeeper_name + ":" + replica_path :
        replica_path);

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
    {
        {"endpoint",                endpoint_id},
        {"part",                    part_name},
        {"client_protocol_version", toString(REPLICATION_PROTOCOL_VERSION_WITH_OBJECT_STORAGE_VFS)},
        {"compress",                "false"}
    });

    if (disk)
    {
        LOG_TRACE(log, "Will fetch to disk {} with type {}", disk->getName(), toString(disk->getDataSourceDescription().type));
        UInt64 revision = disk->getRevision();
        if (revision)
            uri.addQueryParameter("disk_revision", toString(revision));
    }

    Strings zero_copy_disk_types;
    if (try_zero_copy && data_settings->allow_remote_fs_zero_copy_replication)
    {
        zero_copy_disk_types = getCapableDiskTypes(disk, data.getDisks(),
            [](DiskPtr d) { return d->supportZeroCopyReplication(); });
        if (zero_copy_disk_types.empty())
        {
            if (data.canUseZeroCopyReplication())
                LOG_INFO(log, "Cannot select any zero-copy disk for {}", part_name);
            try_zero_copy = false;
        }
        else
            uri.addQueryParameter(ZERO_COPY, fmt::format("{}", fmt::join(zero_copy_disk_types, ", ")));
    }

    Strings vfs_disk_types;
    if (data_settings->allow_object_storage_vfs)
    {
        vfs_disk_types = getCapableDiskTypes(disk, data.getDisks(),
            [](DiskPtr d) { return d->isObjectStorageVFS(); });
        if (!vfs_disk_types.empty())
            uri.addQueryParameter(OBJECT_STORAGE_VFS, fmt::format("{}", fmt::join(vfs_disk_types, ", ")));
    }

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(password);
    }

    std::unique_ptr<PooledReadWriteBufferFromHTTP> in = std::make_unique<PooledReadWriteBufferFromHTTP>(
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        nullptr,
        creds,
        DBMS_DEFAULT_BUFFER_SIZE,
        0, /* no redirects */
        context->getCommonFetchesSessionFactory());

    int server_protocol_version = parse<int>(in->getResponseCookie("server_protocol_version", "0"));
    const String response_disk_type_with_zero_copy = in->getResponseCookie(ZERO_COPY, "");
    DiskPtr preffered_disk = disk;
    if (try_zero_copy
        && !response_disk_type_with_zero_copy.empty()
        && !preffered_disk)
    {
        for (const auto & disk_candidate : data.getDisks())
        {
            if (toString(disk_candidate->getDataSourceDescription().type) == response_disk_type_with_zero_copy)
            {
                preffered_disk = disk_candidate;
                break;
            }
        }
    }

    const String response_disk_type_with_vfs = in->getResponseCookie(OBJECT_STORAGE_VFS, "");
    if (!response_disk_type_with_vfs.empty() && !preffered_disk)
        for (const auto & candidate : data.getDisks())
            if (toString(candidate->getDataSourceDescription().type) == response_disk_type_with_vfs)
            {
                preffered_disk = candidate;
                break;
            }

    ReservationPtr reservation;
    size_t sum_files_size = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
    {
        readBinary(sum_files_size, *in);
        if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            IMergeTreeDataPart::TTLInfos ttl_infos;
            String ttl_infos_string;
            readBinary(ttl_infos_string, *in);
            ReadBufferFromString ttl_infos_buffer(ttl_infos_string);
            assertString("ttl format version: 1\n", ttl_infos_buffer);
            ttl_infos.read(ttl_infos_buffer);

            if (!disk)
            {
                LOG_TEST(log, "Disk for fetch is not provided, reserving space using storage balanced reservation");
                reservation
                    = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, &ttl_infos, true);

                if (!reservation)
                {
                    LOG_TEST(log, "Disk for fetch is not provided, reserving space using TTL rules");
                    reservation
                        = data.reserveSpacePreferringTTLRules(metadata_snapshot, sum_files_size, ttl_infos, std::time(nullptr), 0, true, preffered_disk);
                }
            }
        }
        else if (!disk)
        {
            LOG_TEST(log, "Making balanced reservation");
            reservation = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, nullptr);
            if (!reservation)
            {
                LOG_TEST(log, "Making simple reservation");
                reservation = data.reserveSpace(sum_files_size);
            }
        }
    }
    else if (!disk)
    {
        LOG_TEST(log, "Making reservation on the largest disk");
        /// We don't know real size of part because sender server version is too old
        reservation = data.makeEmptyReservationOnLargestDisk();
    }

    if (!disk)
    {
        disk = reservation->getDisk();
        LOG_TRACE(log, "Disk for fetch is not provided, getting disk from reservation {} with type '{}'", disk->getName(), toString(disk->getDataSourceDescription().type));
    }
    else
    {
        LOG_TEST(log, "Disk for fetch is disk {} with type {}", disk->getName(), toString(disk->getDataSourceDescription().type));
    }

    UInt64 revision = parse<UInt64>(in->getResponseCookie("disk_revision", "0"));
    if (revision)
        disk->syncRevision(revision);

    bool sync = (data_settings->min_compressed_bytes_to_fsync_after_fetch
                    && sum_files_size >= data_settings->min_compressed_bytes_to_fsync_after_fetch);

    using PartType = MergeTreeDataPartType;
    PartType part_type = PartType::Wide;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
    {
        String part_type_str;
        readStringBinary(part_type_str, *in);
        part_type.fromString(part_type_str);
    }

    UUID part_uuid = UUIDHelpers::Nil;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
        readUUIDText(part_uuid, *in);

    size_t projections = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
        readBinary(projections, *in);

    if (!response_disk_type_with_zero_copy.empty())
    {
        if (!try_zero_copy)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected '{}' cookie",
                ZERO_COPY);
        if (!contains(zero_copy_disk_types, response_disk_type_with_zero_copy))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Got '{}' cookie {}, expected one from {}",
                ZERO_COPY,
                response_disk_type_with_zero_copy,
                fmt::join(zero_copy_disk_types, ", "));
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Got '{}' cookie with old protocol version {}",
                ZERO_COPY,
                server_protocol_version);
        if (part_type == PartType::InMemory)
            throw Exception(ErrorCodes::INCORRECT_PART_TYPE,
                "Got '{}' cookie for in-memory part",
                ZERO_COPY);

        try
        {
            auto output_buffer_getter = [](
                IDataPartStorage & part_storage, const auto & file_name, size_t file_size)
            {
                auto full_path = fs::path(part_storage.getFullPath()) / file_name;
                return std::make_unique<WriteBufferFromFile>(
                    full_path,
                    std::min<UInt64>(DBMS_DEFAULT_BUFFER_SIZE, file_size));
            };

            return
            {
                downloadPartToDisk(
                    part_name, replica_path, to_detached, tmp_prefix, disk, true, *in,
                    output_buffer_getter, projections, throttler, sync),
                std::move(temporary_directory_lock)
            };
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::S3_ERROR && e.code() != ErrorCodes::ZERO_COPY_REPLICATION_ERROR)
                throw;

#if USE_AWS_S3
            if (const auto * s3_exception = dynamic_cast<const S3Exception *>(&e))
            {
                /// It doesn't make sense to retry Access Denied or No Such Key
                if (!s3_exception->isRetryableError())
                {
                    tryLogCurrentException(log, fmt::format("while fetching part: {}", part_name));
                    throw;
                }
            }
#endif

            LOG_WARNING(log, "Will retry fetching part without zero-copy: {}", e.message());

            /// It's important to release session from HTTP pool. Otherwise it's possible to get deadlock
            /// on http pool.
            try
            {
                in.reset();
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }

            temporary_directory_lock = {};

            /// Try again but without zero-copy
            return fetchSelectedPart(
                metadata_snapshot,
                context,
                part_name,
                zookeeper_name,
                replica_path,
                host,
                port,
                timeouts,
                user, password, interserver_scheme, throttler, to_detached, tmp_prefix, nullptr, false, disk);
        }
    }

    if (!response_disk_type_with_vfs.empty())
    {
        if (!contains(vfs_disk_types, response_disk_type_with_vfs))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Got '{}' cookie {}, expected one from {}",
                OBJECT_STORAGE_VFS,
                response_disk_type_with_vfs,
                fmt::join(vfs_disk_types, ", "));
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_OBJECT_STORAGE_VFS)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Got '{}' cookie with old protocol version {}",
                OBJECT_STORAGE_VFS,
                server_protocol_version);
        if (part_type == PartType::InMemory)
            throw Exception(ErrorCodes::INCORRECT_PART_TYPE,
                "Got '{}' cookie for in-memory part",
                OBJECT_STORAGE_VFS);

        auto output_buffer_getter = [](
            IDataPartStorage & part_storage, const String & file_name, size_t file_size)
        {
            return part_storage.writeFile(file_name,
                std::min<UInt64>(file_size, DBMS_DEFAULT_BUFFER_SIZE),
                WriteSettings{.is_metadata_file_for_vfs=true});
        };

        return
        {
            downloadPartToDisk(
                part_name, replica_path, to_detached, tmp_prefix, disk, false, *in,
                output_buffer_getter, projections, throttler, sync,
                /*object_storage_vfs=*/true),
            std::move(temporary_directory_lock)
        };
    }

    auto storage_id = data.getStorageID();
    String new_part_path = part_type == PartType::InMemory ? "memory" : fs::path(data.getFullPathOnDisk(disk)) / part_name / "";
    auto entry = data.getContext()->getReplicatedFetchList().insert(
        storage_id.getDatabaseName(), storage_id.getTableName(),
        part_info.partition_id, part_name, new_part_path,
        replica_path, uri, to_detached, sum_files_size);

    in->setNextCallback(ReplicatedFetchReadCallback(*entry));

    if (part_type == PartType::InMemory)
    {
        auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);

        auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(
            volume,
            data.getRelativeDataPath(),
            part_name);

        return std::make_pair(downloadPartToMemory(
            data_part_storage, part_name,
            MergeTreePartInfo::fromPartName(part_name, data.format_version),
            part_uuid, metadata_snapshot, context, *in,
            projections, false, throttler), std::move(temporary_directory_lock));
    }

    auto output_buffer_getter = [](IDataPartStorage & part_storage, const String & file_name, size_t file_size)
    {
        return part_storage.writeFile(file_name, std::min<UInt64>(file_size, DBMS_DEFAULT_BUFFER_SIZE), {});
    };

    return std::make_pair(downloadPartToDisk(
        part_name, replica_path, to_detached, tmp_prefix,
        disk, false, *in, output_buffer_getter,
        projections, throttler, sync),std::move(temporary_directory_lock));
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToMemory(
    MutableDataPartStoragePtr data_part_storage,
    const String & part_name,
    const MergeTreePartInfo & part_info,
    const UUID & part_uuid,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    PooledReadWriteBufferFromHTTP & in,
    size_t projections,
    bool is_projection,
    ThrottlerPtr throttler)
{
    auto new_data_part = std::make_shared<MergeTreeDataPartInMemory>(data, part_name, part_info, data_part_storage);

    for (size_t i = 0; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);

        MergeTreePartInfo new_part_info("all", 0, 0, 0);
        auto projection_part_storage = data_part_storage->getProjection(projection_name + ".proj");

        auto new_projection_part = downloadPartToMemory(
            projection_part_storage, projection_name,
            new_part_info, part_uuid, metadata_snapshot,
            context, in, 0, true, throttler);

        new_data_part->addProjectionPart(projection_name, std::move(new_projection_part));
    }

    MergeTreeData::DataPart::Checksums checksums;
    if (!checksums.read(in))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot deserialize checksums");

    NativeReader block_in(in, 0);
    auto block = block_in.read();
    throttler->add(block.bytes());

    new_data_part->setColumns(block.getNamesAndTypesList(), {}, metadata_snapshot->getMetadataVersion());

    if (!is_projection)
    {
        new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        new_data_part->uuid = part_uuid;
        new_data_part->is_temp = true;
        new_data_part->minmax_idx->update(block, data.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));
        new_data_part->partition.create(metadata_snapshot, block, 0, context);
    }

    MergedBlockOutputStream part_out(
        new_data_part, metadata_snapshot, block.getNamesAndTypesList(), {}, {},
        CompressionCodecFactory::instance().get("NONE", {}), NO_TRANSACTION_PTR);

    part_out.write(block);
    part_out.finalizePart(new_data_part, false);
    new_data_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);

    return new_data_part;
}

void Fetcher::downloadBaseOrProjectionPartToDisk(
    const String & replica_path,
    const MutableDataPartStoragePtr & data_part_storage,
    PooledReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    MergeTreeData::DataPart::Checksums & checksums,
    ThrottlerPtr throttler,
    bool sync) const
{
    size_t files;
    readBinary(files, in);

    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;

    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        /// File must be inside "absolute_part_path" directory.
        /// Otherwise malicious ClickHouse replica may force us to write to arbitrary path.
        String absolute_file_path = fs::weakly_canonical(fs::path(data_part_storage->getRelativePath()) / file_name);
        if (!startsWith(absolute_file_path, fs::weakly_canonical(data_part_storage->getRelativePath()).string()))
            throw Exception(ErrorCodes::INSECURE_PATH,
                "File path ({}) doesn't appear to be inside part path ({}). "
                "This may happen if we are trying to download part from malicious replica or logical error.",
                absolute_file_path, data_part_storage->getRelativePath());

        written_files.emplace_back(output_buffer_getter(*data_part_storage, file_name, file_size));
        HashingWriteBuffer hashing_out(*written_files.back());
        copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);
        hashing_out.finalize();

        if (blocker.isCancelled())
        {
            /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
            /// performing a poll with a not very large timeout.
            /// And now we check it only between read chunks (in the `copyData` function).
            throw Exception(ErrorCodes::ABORTED, "Fetching of part was cancelled");
        }

        MergeTreeDataPartChecksum::uint128 expected_hash;
        readPODBinary(expected_hash, in);

        if (expected_hash != hashing_out.getHash())
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                "Checksum mismatch for file {} transferred from {}",
                (fs::path(data_part_storage->getFullPath()) / file_name).string(),
                replica_path);

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt" &&
            file_name != IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME &&
            file_name != IMergeTreeDataPart::METADATA_VERSION_FILE_NAME)
            checksums.addFile(file_name, file_size, expected_hash);
    }

    /// Call fsync for all files at once in attempt to decrease the latency
    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToDisk(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix,
    DiskPtr disk,
    bool zero_copy,
    PooledReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    size_t projections,
    ThrottlerPtr throttler,
    bool sync,
    bool is_object_storage_vfs)
{
    String part_id;
    const auto data_settings = data.getSettings();
    MergeTreeData::DataPart::Checksums data_checksums;

    zkutil::EphemeralNodeHolderPtr zero_copy_temporary_lock_holder;
    if (zero_copy)
    {
        readStringBinary(part_id, in);

        if (!disk->supportZeroCopyReplication() || !disk->checkUniqueId(part_id))
            throw Exception(ErrorCodes::ZERO_COPY_REPLICATION_ERROR, "Part {} unique id {} doesn't exist on {} (with type {}).", part_name, part_id, disk->getName(), toString(disk->getDataSourceDescription().type));

        LOG_DEBUG(log, "Downloading part {} unique id {} metadata onto disk {}.", part_name, part_id, disk->getName());
        zero_copy_temporary_lock_holder = data.lockSharedDataTemporary(part_name, part_id, disk);
    }
    else
    {
        LOG_DEBUG(log, "Downloading part {} onto disk {}.", part_name, disk->getName());
    }

    /// We will remove directory if it's already exists. Make precautions.
    if (tmp_prefix.empty()
        || part_name.empty()
        || std::string::npos != tmp_prefix.find_first_of("/.")
        || std::string::npos != part_name.find_first_of("/."))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: tmp_prefix and part_name cannot be empty or contain '.' or '/' characters.");

    auto part_dir = tmp_prefix + part_name;
    auto part_relative_path = data.getRelativeDataPath() + String(to_detached ? "detached/" : "");
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);

    /// Create temporary part storage to write sent files.
    /// Actual part storage will be initialized later from metadata.
    auto part_storage_for_loading = std::make_shared<DataPartStorageOnDiskFull>(volume, part_relative_path, part_dir);
    part_storage_for_loading->beginTransaction();

    if (part_storage_for_loading->exists())
    {
        LOG_WARNING(log, "Directory {} already exists, probably result of a failed fetch. Will remove it before fetching part.",
            part_storage_for_loading->getFullPath());

        /// Even if it's a temporary part it could be downloaded with zero copy replication and this function
        /// is executed as a callback.
        ///
        /// We don't control the amount of refs for temporary parts so we cannot decide can we remove blobs
        /// or not. So we are not doing it
        bool keep_shared = part_storage_for_loading->supportZeroCopyReplication() && data_settings->allow_remote_fs_zero_copy_replication;
        part_storage_for_loading->removeSharedRecursive(keep_shared);
    }

    part_storage_for_loading->createDirectories();

    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = part_storage_for_loading->getDirectorySyncGuard();

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    try
    {
        for (size_t i = 0; i < projections; ++i)
        {
            String projection_name;
            readStringBinary(projection_name, in);
            MergeTreeData::DataPart::Checksums projection_checksum;

            auto projection_part_storage = part_storage_for_loading->getProjection(projection_name + ".proj");
            projection_part_storage->createDirectories();

            downloadBaseOrProjectionPartToDisk(
                replica_path, projection_part_storage, in, output_buffer_getter, projection_checksum, throttler, sync);

            data_checksums.addFile(
                projection_name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
        }

        downloadBaseOrProjectionPartToDisk(
            replica_path, part_storage_for_loading, in, output_buffer_getter, data_checksums, throttler, sync);
    }
    catch (const Exception & e)
    {
        /// Remove the whole part directory if fetch of base
        /// part or fetch of any projection was stopped.
        if (e.code() == ErrorCodes::ABORTED)
        {
            part_storage_for_loading->removeSharedRecursive(true);
            part_storage_for_loading->commitTransaction();
        }
        throw;
    }

    assertEOF(in);
    MergeTreeData::MutableDataPartPtr new_data_part;
    try
    {
        part_storage_for_loading->commitTransaction();

        MergeTreeDataPartBuilder builder(data, part_name, volume, part_relative_path, part_dir);
        new_data_part = builder.withPartFormatFromDisk().build();

        new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        new_data_part->is_temp = true;
        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
        /// The blobs have to stay intact, this temporary part does not own them and does not share them yet.
        new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
        new_data_part->modification_time = time(nullptr);
        new_data_part->loadColumnsChecksumsIndexes(true, false);
    }
#if USE_AWS_S3
    catch (const S3Exception & ex)
    {
        if (ex.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
        {
            throw Exception(ErrorCodes::S3_ERROR, "Cannot fetch part {} because we lost lock and it was concurrently removed", part_name);
        }
        throw;
    }
#endif
    catch (...) /// Redundant catch, just to be able to add first one with #if
    {
        throw;
    }

    if (zero_copy || is_object_storage_vfs)
    {
        LOG_DEBUG(log, "Download of part {} unique id {} metadata onto disk {} finished.", part_name, part_id, disk->getName());
    }
    else
    {
        if (isFullPartStorage(new_data_part->getDataPartStorage()))
            new_data_part->checksums.checkEqual(data_checksums, false);
        LOG_DEBUG(log, "Download of part {} onto disk {} finished.", part_name, disk->getName());
    }

    if (zero_copy_temporary_lock_holder)
        zero_copy_temporary_lock_holder->setAlreadyRemoved();

    return new_data_part;
}

}

}
