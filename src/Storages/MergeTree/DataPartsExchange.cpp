#include <Storages/MergeTree/DataPartsExchange.h>

#include "config.h"

#include <Formats/NativeWriter.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/HTTPCommon.h>
#include <IO/S3Common.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
#include <Common/randomDelay.h>
#include <Common/FailPoint.h>
#include <Common/thread_local_rng.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <base/scope_guard.h>
#include <Poco/Net/HTTPRequest.h>
#include <boost/algorithm/string/join.hpp>
#include <base/sort.h>
#include <random>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric ReplicatedSend;
    extern const Metric ReplicatedFetch;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool enable_the_endpoint_id_with_zookeeper_name_prefix;
    extern const MergeTreeSettingsBool fsync_part_directory;
    extern const MergeTreeSettingsUInt64 min_compressed_bytes_to_fsync_after_fetch;
}

namespace ErrorCodes
{
    extern const int NO_SUCH_DATA_PART;
    extern const int ABORTED;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int INSECURE_PATH;
    extern const int LOGICAL_ERROR;
    extern const int S3_ERROR;
    extern const int ZERO_COPY_REPLICATION_ERROR;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char replicated_sends_failpoint[];
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
    , log(getLogger(data.getStorageID().getNameForLogs() + " (Replicated PartsService)"))
{}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

void Service::processQuery(const HTMLForm & params, ReadBuffer & /*body*/, WriteBuffer & out, HTTPServerResponse & response)
{
    int client_protocol_version = parse<int>(params.get("client_protocol_version", "0"));

    String part_name = params.get("part");

    const auto data_settings = data.getSettings();

    /// Validation of the input that may come from malicious replica.
    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    /// We pretend to work as older server version, to be sure that client will correctly process our version
    response.addCookie({"server_protocol_version", toString(std::min(client_protocol_version, REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION))});

    LOG_TRACE(log, "Sending part {}", part_name);

    static const auto test_delay = data.getContext()->getConfigRef().getUInt64("test.data_parts_exchange.delay_before_sending_part_ms", 0);
    if (test_delay)
        randomDelayForMaxMilliseconds(test_delay, log, "DataPartsExchange: Before sending part");

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

        String remote_fs_metadata = parse<String>(params.get("remote_fs_metadata", ""));

        /// Tokenize capabilities from remote_fs_metadata
        /// E.g. remote_fs_metadata = "local, s3_plain, web" --> capabilities = ["local", "s3_plain", "web"]
        Strings capabilities;
        const String delimiter(", ");
        size_t pos_start = 0;
        size_t pos_end;
        while ((pos_end = remote_fs_metadata.find(delimiter, pos_start)) != std::string::npos)
        {
            const String token = remote_fs_metadata.substr(pos_start, pos_end - pos_start);
            pos_start = pos_end + delimiter.size();
            capabilities.push_back(token);
        }
        capabilities.push_back(remote_fs_metadata.substr(pos_start));

        bool send_projections = client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION;

        if (send_projections)
        {
            const auto & projections = part->getProjectionParts();
            writeBinary(projections.size(), out);
        }

        if ((*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication] &&
            client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
        {
            auto disk_type = part->getDataPartStorage().getDiskType();
            if (part->getDataPartStorage().supportZeroCopyReplication() && std::find(capabilities.begin(), capabilities.end(), disk_type) != capabilities.end())
            {
                /// Send metadata if the receiver's capabilities covers the source disk type.
                response.addCookie({"remote_fs_metadata", disk_type});
                sendPartFromDisk(part, out, client_protocol_version, true, send_projections);
                return;
            }
        }

        sendPartFromDisk(part, out, client_protocol_version, false, send_projections);
        data.addLastSentPart(part->info);
    }
    catch (const NetException &)
    {
        /// Network error or error on remote side. No need to enqueue part for check.
        throw;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::CANNOT_WRITE_TO_OSTREAM
            && !isRetryableException(std::current_exception()))
        {
            report_broken_part();
        }

        throw;
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            report_broken_part();
        throw;
    }
}


MergeTreeData::DataPart::Checksums Service::sendPartFromDisk(
    const MergeTreeData::DataPartPtr & part,
    WriteBuffer & out,
    int client_protocol_version,
    bool from_remote_disk,
    bool send_projections)
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
    IDataPartStorage::ReplicatedFilesDescription replicated_description;

    if (from_remote_disk)
    {
        replicated_description = data_part_storage->getReplicatedFilesDescriptionForRemoteDisk(files_to_replicate);
        if (!part->isProjectionPart())
            writeStringBinary(replicated_description.unique_id, out);
    }
    else
    {
        replicated_description = data_part_storage->getReplicatedFilesDescription(files_to_replicate);
    }

    MergeTreeData::DataPart::Checksums data_checksums;
    for (const auto & [name, projection] : part->getProjectionParts())
    {
        if (send_projections)
        {
            writeStringBinary(name, out);
            MergeTreeData::DataPart::Checksums projection_checksum = sendPartFromDisk(projection, out, client_protocol_version, from_remote_disk, false);
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

        const auto & is_cancelled = blocker.getCounter();
        auto cancellation_hook = [&]()
        {
            if (is_cancelled)
                throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

            fiu_do_on(FailPoints::replicated_sends_failpoint,
            {
                std::bernoulli_distribution fault(0.1);
                if (fault(thread_local_rng))
                    throw Exception(ErrorCodes::FAULT_INJECTED, "Failpoint replicated_sends_failpoint is triggered");
            });
        };
        copyDataWithThrottler(*file_in, hashing_out, cancellation_hook, data.getSendsThrottler());

        hashing_out.finalize();

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

    if (!from_remote_disk && isFullPartStorage(part->getDataPartStorage()))
        part->checksums.checkEqual(data_checksums, false, part->name);

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

    bool zero_copy_enabled = (*data.getSettings())[MergeTreeSetting::allow_remote_fs_zero_copy_replication];
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
    , log(getLogger(data.getStorageID().getNameForLogs() + " (Fetcher)"))
{}

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
            (*data_settings)[MergeTreeSetting::enable_the_endpoint_id_with_zookeeper_name_prefix] ?
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
        {"client_protocol_version", toString(REPLICATION_PROTOCOL_VERSION_WITH_METADATA_VERSION)},
        {"compress",                "false"}
    });

    if (disk)
    {
        LOG_TRACE(log, "Will fetch to disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());
        UInt64 revision = disk->getRevision();
        if (revision)
            uri.addQueryParameter("disk_revision", toString(revision));
    }

    Strings capability;
    if (try_zero_copy && (*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        if (!disk)
        {
            LOG_TRACE(log, "Trying to fetch with zero-copy replication, but disk is not provided, will try to select");
            Disks disks = data.getDisks();
            for (const auto & data_disk : disks)
            {
                LOG_TRACE(log, "Checking disk {} with type {}", data_disk->getName(), data_disk->getDataSourceDescription().toString());
                if (data_disk->supportZeroCopyReplication())
                {
                    LOG_TRACE(log, "Disk {} (with type {}) supports zero-copy replication", data_disk->getName(), data_disk->getDataSourceDescription().toString());
                    capability.push_back(data_disk->getDataSourceDescription().toString());
                }
            }
        }
        else if (disk->supportZeroCopyReplication())
        {
            LOG_TRACE(log, "Trying to fetch with zero copy replication, provided disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());
            capability.push_back(disk->getDataSourceDescription().toString());
        }
    }

    if (!capability.empty())
    {
        ::sort(capability.begin(), capability.end());
        capability.erase(std::unique(capability.begin(), capability.end()), capability.end());
        const String & remote_fs_metadata = boost::algorithm::join(capability, ", ");
        uri.addQueryParameter("remote_fs_metadata", remote_fs_metadata);
    }
    else
    {
        if (data.canUseZeroCopyReplication())
            LOG_INFO(log, "Cannot select any zero-copy disk for {}", part_name);

        try_zero_copy = false;
    }

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(password);
    }

    ReadSettings read_settings = context->getReadSettings();
    /// Disable retries for fetches, this will be done by the engine itself.
    read_settings.http_max_tries = 1;

    auto in = BuilderRWBufferFromHTTP(uri)
                  .withConnectionGroup(HTTPConnectionGroupType::HTTP)
                  .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                  .withTimeouts(timeouts)
                  .withSettings(read_settings)
                  .withDelayInit(false)
                  .create(creds);

    int server_protocol_version = parse<int>(in->getResponseCookie("server_protocol_version", "0"));
    String remote_fs_metadata = parse<String>(in->getResponseCookie("remote_fs_metadata", ""));

    DiskPtr preffered_disk = disk;

    if (!preffered_disk)
    {
        for (const auto & disk_candidate : data.getDisks())
        {
            if (disk_candidate->getDataSourceDescription().toString() == remote_fs_metadata)
            {
                preffered_disk = disk_candidate;
                break;
            }
        }
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
        LOG_TRACE(log, "Disk for fetch is not provided, getting disk from reservation {} with type '{}'", disk->getName(), disk->getDataSourceDescription().toString());
    }
    else
    {
        LOG_TEST(log, "Disk for fetch is disk {} with type {}", disk->getName(), disk->getDataSourceDescription().toString());
    }

    UInt64 revision = parse<UInt64>(in->getResponseCookie("disk_revision", "0"));

    if (revision)
        disk->syncRevision(revision);

    bool sync = ((*data_settings)[MergeTreeSetting::min_compressed_bytes_to_fsync_after_fetch]
                    && sum_files_size >= (*data_settings)[MergeTreeSetting::min_compressed_bytes_to_fsync_after_fetch]);

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

    if (!remote_fs_metadata.empty())
    {
        if (!try_zero_copy)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got unexpected 'remote_fs_metadata' cookie");
        if (std::find(capability.begin(), capability.end(), remote_fs_metadata) == capability.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie {}, expect one from {}",
                            remote_fs_metadata, fmt::join(capability, ", "));
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie with old protocol version {}", server_protocol_version);

        try
        {
            auto output_buffer_getter = [](IDataPartStorage & part_storage, const auto & file_name, size_t file_size)
            {
                auto full_path = fs::path(part_storage.getFullPath()) / file_name;
                return std::make_unique<WriteBufferFromFile>(full_path, std::min<UInt64>(DBMS_DEFAULT_BUFFER_SIZE, file_size));
            };

            return std::make_pair(downloadPartToDisk(part_name, replica_path, to_detached, tmp_prefix, disk, true, *in, output_buffer_getter, projections, throttler, sync), std::move(temporary_directory_lock));
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

    auto storage_id = data.getStorageID();
    String new_part_path = fs::path(data.getFullPathOnDisk(disk)) / part_name / "";
    auto entry = data.getContext()->getReplicatedFetchList().insert(
        storage_id.getDatabaseName(), storage_id.getTableName(),
        part_info.partition_id, part_name, new_part_path,
        replica_path, uri, to_detached, sum_files_size);

    in->setNextCallback(ReplicatedFetchReadCallback(*entry));

    auto output_buffer_getter = [](IDataPartStorage & part_storage, const String & file_name, size_t file_size)
    {
        return part_storage.writeFile(file_name, std::min<UInt64>(file_size, DBMS_DEFAULT_BUFFER_SIZE), {});
    };

    return std::make_pair(downloadPartToDisk(
        part_name, replica_path, to_detached, tmp_prefix,
        disk, false, *in, output_buffer_getter,
        projections, throttler, sync),std::move(temporary_directory_lock));
}


void Fetcher::downloadBaseOrProjectionPartToDisk(
    const String & replica_path,
    const MutableDataPartStoragePtr & data_part_storage,
    ReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    MergeTreeData::DataPart::Checksums & checksums,
    ThrottlerPtr throttler,
    bool sync) const
{
    size_t files;
    readBinary(files, in);
    LOG_DEBUG(log, "Downloading files {}", files);


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
                "Checksum mismatch for file {} transferred from {} (0x{} vs 0x{})",
                (fs::path(data_part_storage->getFullPath()) / file_name).string(),
                replica_path,
                getHexUIntLowercase(expected_hash),
                getHexUIntLowercase(hashing_out.getHash()));

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
    bool to_remote_disk,
    ReadWriteBufferFromHTTP & in,
    OutputBufferGetter output_buffer_getter,
    size_t projections,
    ThrottlerPtr throttler,
    bool sync)
{
    String part_id;
    const auto data_settings = data.getSettings();
    MergeTreeData::DataPart::Checksums data_checksums;

    zkutil::EphemeralNodeHolderPtr zero_copy_temporary_lock_holder;
    if (to_remote_disk)
    {
        readStringBinary(part_id, in);

        if (!disk->supportZeroCopyReplication() || !disk->checkUniqueId(part_id))
            throw Exception(ErrorCodes::ZERO_COPY_REPLICATION_ERROR, "Part {} unique id {} doesn't exist on {} (with type {}).", part_name, part_id, disk->getName(), disk->getDataSourceDescription().toString());

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`tmp_prefix` and `part_name` cannot be empty or contain '.' or '/' characters.");

    auto part_dir = tmp_prefix + part_name;
    auto part_relative_path = data.getRelativeDataPath() + String(to_detached ? MergeTreeData::DETACHED_DIR_NAME : "");
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
        bool keep_shared = part_storage_for_loading->supportZeroCopyReplication() && (*data_settings)[MergeTreeSetting::allow_remote_fs_zero_copy_replication];
        part_storage_for_loading->removeSharedRecursive(keep_shared);
    }

    part_storage_for_loading->createDirectories();

    SyncGuardPtr sync_guard;
    if ((*data.getSettings())[MergeTreeSetting::fsync_part_directory])
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

        MergeTreeDataPartBuilder builder(data, part_name, volume, part_relative_path, part_dir, getReadSettings());
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

    if (to_remote_disk)
    {
        LOG_DEBUG(log, "Download of part {} unique id {} metadata onto disk {} finished.", part_name, part_id, disk->getName());
    }
    else
    {
        if (isFullPartStorage(new_data_part->getDataPartStorage()))
            new_data_part->checksums.checkEqual(data_checksums, false, new_data_part->name);
        LOG_DEBUG(log, "Download of part {} onto disk {} finished.", part_name, disk->getName());
    }

    if (zero_copy_temporary_lock_holder)
        zero_copy_temporary_lock_holder->setAlreadyRemoved();

    return new_data_part;
}

}

}
