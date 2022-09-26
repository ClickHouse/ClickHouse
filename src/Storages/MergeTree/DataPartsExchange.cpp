#include <Storages/MergeTree/DataPartsExchange.h>

#include <Formats/NativeWriter.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <IO/HTTPCommon.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
#include <Storages/MergeTree/DataPartStorageOnDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <base/scope_guard.h>
#include <Poco/Net/HTTPRequest.h>
#include <boost/algorithm/string/join.hpp>
#include <iterator>
#include <regex>
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
    extern const int DIRECTORY_ALREADY_EXISTS;
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
// Reserved for ALTER PRIMARY KEY
// constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PRIMARY_KEY = 8;


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


Service::Service(StorageReplicatedMergeTree & data_) :
    data(data_), log(&Poco::Logger::get(data.getLogName() + " (Replicated PartsService)")) {}

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
    response.addCookie({"server_protocol_version", toString(std::min(client_protocol_version, REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION))});

    LOG_TRACE(log, "Sending part {}", part_name);

    MergeTreeData::DataPartPtr part;

    auto report_broken_part = [&]()
    {
        if (part && part->isProjectionPart())
        {
            auto parent_part = part->getParentPart()->shared_from_this();
            data.reportBrokenPart(parent_part);
        }
        else if (part)
            data.reportBrokenPart(part);
        else
            LOG_TRACE(log, "Part {} was not found, do not report it as broken", part_name);
    };

    try
    {
        part = findPart(part_name);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

        if (part->data_part_storage->isStoredOnRemoteDisk())
        {
            UInt64 revision = parse<UInt64>(params.get("disk_revision", "0"));
            if (revision)
                part->data_part_storage->syncRevision(revision);
            revision = part->data_part_storage->getRevision();
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
        std::regex re("\\s*,\\s*");
        Strings capability(
            std::sregex_token_iterator(remote_fs_metadata.begin(), remote_fs_metadata.end(), re, -1),
            std::sregex_token_iterator());

        if (data_settings->allow_remote_fs_zero_copy_replication &&
            /// In memory data part does not have metadata yet.
            !isInMemoryPart(part) &&
            client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
        {
            auto disk_type = part->data_part_storage->getDiskType();
            if (part->data_part_storage->supportZeroCopyReplication() && std::find(capability.begin(), capability.end(), disk_type) != capability.end())
            {
                /// Send metadata if the receiver's capability covers the source disk type.
                response.addCookie({"remote_fs_metadata", disk_type});
                sendPartFromDiskRemoteMeta(part, out);
                return;
            }
        }

        if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
        {
            const auto & projections = part->getProjectionParts();
            writeBinary(projections.size(), out);
            if (isInMemoryPart(part))
                sendPartFromMemory(part, out, projections);
            else
                sendPartFromDisk(part, out, client_protocol_version, projections);
        }
        else
        {
            if (isInMemoryPart(part))
                sendPartFromMemory(part, out);
            else
                sendPartFromDisk(part, out, client_protocol_version);
        }
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
    const MergeTreeData::DataPartPtr & part, WriteBuffer & out, const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & projections)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    for (const auto & [name, projection] : projections)
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
    const std::map<String, std::shared_ptr<IMergeTreeDataPart>> & projections)
{
    /// We'll take a list of files from the list of checksums.
    MergeTreeData::DataPart::Checksums checksums = part->checksums;
    /// Add files that are not in the checksum list.
    auto file_names_without_checksums = part->getFileNamesWithoutChecksums();
    for (const auto & file_name : file_names_without_checksums)
    {
        if (client_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_DEFAULT_COMPRESSION && file_name == IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME)
            continue;

        checksums.files[file_name] = {};
    }

    //auto disk = part->volume->getDisk();
    MergeTreeData::DataPart::Checksums data_checksums;
    for (const auto & [name, projection] : part->getProjectionParts())
    {
        // Get rid of projection files
        checksums.files.erase(name + ".proj");
        auto it = projections.find(name);
        if (it != projections.end())
        {
            writeStringBinary(name, out);
            MergeTreeData::DataPart::Checksums projection_checksum = sendPartFromDisk(it->second, out, client_protocol_version);
            data_checksums.addFile(name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
        }
        else if (part->checksums.has(name + ".proj"))
        {
            // We don't send this projection, just add out checksum to bypass the following check
            const auto & our_checksum = part->checksums.files.find(name + ".proj")->second;
            data_checksums.addFile(name + ".proj", our_checksum.file_size, our_checksum.file_hash);
        }
    }

    writeBinary(checksums.files.size(), out);
    for (const auto & it : checksums.files)
    {
        String file_name = it.first;

        UInt64 size = part->data_part_storage->getFileSize(file_name);

        writeStringBinary(it.first, out);
        writeBinary(size, out);

        auto file_in = part->data_part_storage->readFile(file_name, {}, std::nullopt, std::nullopt);
        HashingWriteBuffer hashing_out(out);
        copyDataWithThrottler(*file_in, hashing_out, blocker.getCounter(), data.getSendsThrottler());

        if (blocker.isCancelled())
            throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

        if (hashing_out.count() != size)
            throw Exception(
                ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
                "Unexpected size of file {}, expected {} got {}",
                std::string(fs::path(part->data_part_storage->getRelativePath()) / file_name),
                hashing_out.count(), size);

        writePODBinary(hashing_out.getHash(), out);

        if (!file_names_without_checksums.contains(file_name))
            data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
    }

    part->checksums.checkEqual(data_checksums, false);
    return data_checksums;
}

void Service::sendPartFromDiskRemoteMeta(const MergeTreeData::DataPartPtr & part, WriteBuffer & out)
{
    const auto * data_part_storage_on_disk = dynamic_cast<const DataPartStorageOnDisk *>(part->data_part_storage.get());
    if (!data_part_storage_on_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage '{}' doesn't support zero-copy replication", part->data_part_storage->getDiskName());

    if (!data_part_storage_on_disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk '{}' doesn't support zero-copy replication", data_part_storage_on_disk->getDiskName());

    /// We'll take a list of files from the list of checksums.
    MergeTreeData::DataPart::Checksums checksums = part->checksums;
    /// Add files that are not in the checksum list.
    auto file_names_without_checksums = part->getFileNamesWithoutChecksums();
    for (const auto & file_name : file_names_without_checksums)
        checksums.files[file_name] = {};

    std::vector<std::string> paths;
    paths.reserve(checksums.files.size());
    for (const auto & it : checksums.files)
        paths.push_back(fs::path(part->data_part_storage->getRelativePath()) / it.first);

    /// Serialized metadatadatas with zero ref counts.
    auto metadatas = data_part_storage_on_disk->getSerializedMetadata(paths);

    String part_id = data_part_storage_on_disk->getUniqueId();
    writeStringBinary(part_id, out);

    writeBinary(checksums.files.size(), out);
    for (const auto & it : checksums.files)
    {
        const String & file_name = it.first;
        String file_path_prefix = fs::path(part->data_part_storage->getRelativePath()) / file_name;

        /// Just some additional checks
        String metadata_file_path = fs::path(data_part_storage_on_disk->getDiskPath()) / file_path_prefix;
        fs::path metadata(metadata_file_path);
        if (!fs::exists(metadata))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not exists", file_name);
        if (!fs::is_regular_file(metadata))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not a file", file_name);

        /// Actual metadata send
        auto metadata_str = metadatas[file_path_prefix];
        UInt64 file_size = metadata_str.size();
        ReadBufferFromString buf(metadata_str);

        writeStringBinary(it.first, out);
        writeBinary(file_size, out);

        HashingWriteBuffer hashing_out(out);
        copyDataWithThrottler(buf, hashing_out, blocker.getCounter(), data.getSendsThrottler());
        if (blocker.isCancelled())
            throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

        if (hashing_out.count() != file_size)
            throw Exception(ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART, "Unexpected size of file {}", metadata_file_path);

        writePODBinary(hashing_out.getHash(), out);
    }
}

MergeTreeData::DataPartPtr Service::findPart(const String & name)
{
    /// It is important to include PreActive and Outdated parts here because remote replicas cannot reliably
    /// determine the local state of the part, so queries for the parts in these states are completely normal.
    auto part = data.getPartIfExists(
        name, {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
    if (part)
        return part;

    throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in table", name);
}

MergeTreeData::MutableDataPartPtr Fetcher::fetchPart(
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const String & part_name,
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
        throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);

    /// Validation of the input that may come from malicious replica.
    auto part_info = MergeTreePartInfo::fromPartName(part_name, data.format_version);
    const auto data_settings = data.getSettings();

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
    {
        {"endpoint",                getEndpointId(replica_path)},
        {"part",                    part_name},
        {"client_protocol_version", toString(REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)},
        {"compress",                "false"}
    });

    if (disk)
    {
        UInt64 revision = disk->getRevision();
        if (revision)
            uri.addQueryParameter("disk_revision", toString(revision));
    }

    Strings capability;
    if (try_zero_copy && data_settings->allow_remote_fs_zero_copy_replication)
    {
        if (!disk)
        {
            Disks disks = data.getDisks();
            for (const auto & data_disk : disks)
                if (data_disk->supportZeroCopyReplication())
                    capability.push_back(toString(data_disk->getType()));
        }
        else if (disk->supportZeroCopyReplication())
        {
            capability.push_back(toString(disk->getType()));
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
        try_zero_copy = false;
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
        timeouts,
        creds,
        DBMS_DEFAULT_BUFFER_SIZE,
        0, /* no redirects */
        static_cast<uint64_t>(data_settings->replicated_max_parallel_fetches_for_host));

    int server_protocol_version = parse<int>(in->getResponseCookie("server_protocol_version", "0"));

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
                reservation
                    = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, &ttl_infos, true);
                if (!reservation)
                    reservation
                        = data.reserveSpacePreferringTTLRules(metadata_snapshot, sum_files_size, ttl_infos, std::time(nullptr), 0, true);
            }
        }
        else if (!disk)
        {
            reservation = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, nullptr);
            if (!reservation)
                reservation = data.reserveSpace(sum_files_size);
        }
    }
    else if (!disk)
    {
        /// We don't know real size of part because sender server version is too old
        reservation = data.makeEmptyReservationOnLargestDisk();
    }
    if (!disk)
        disk = reservation->getDisk();

    UInt64 revision = parse<UInt64>(in->getResponseCookie("disk_revision", "0"));
    if (revision)
        disk->syncRevision(revision);

    bool sync = (data_settings->min_compressed_bytes_to_fsync_after_fetch
                    && sum_files_size >= data_settings->min_compressed_bytes_to_fsync_after_fetch);

    String part_type = "Wide";
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
        readStringBinary(part_type, *in);

    UUID part_uuid = UUIDHelpers::Nil;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
        readUUIDText(part_uuid, *in);

    String remote_fs_metadata = parse<String>(in->getResponseCookie("remote_fs_metadata", ""));
    if (!remote_fs_metadata.empty())
    {
        if (!try_zero_copy)
            throw Exception("Got unexpected 'remote_fs_metadata' cookie", ErrorCodes::LOGICAL_ERROR);
        if (std::find(capability.begin(), capability.end(), remote_fs_metadata) == capability.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie {}, expect one from {}", remote_fs_metadata, fmt::join(capability, ", "));
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_ZERO_COPY)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got 'remote_fs_metadata' cookie with old protocol version {}", server_protocol_version);
        if (part_type == "InMemory")
            throw Exception("Got 'remote_fs_metadata' cookie for in-memory part", ErrorCodes::INCORRECT_PART_TYPE);

        try
        {
            return downloadPartToDiskRemoteMeta(part_name, replica_path, to_detached, tmp_prefix_, disk, *in, throttler);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::S3_ERROR && e.code() != ErrorCodes::ZERO_COPY_REPLICATION_ERROR)
                throw;

            LOG_WARNING(log, fmt::runtime(e.message() + " Will retry fetching part without zero-copy."));

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

            /// Try again but without zero-copy
            return fetchPart(metadata_snapshot, context, part_name, replica_path, host, port, timeouts,
                user, password, interserver_scheme, throttler, to_detached, tmp_prefix_, nullptr, false, disk);
        }
    }

    auto storage_id = data.getStorageID();
    String new_part_path = part_type == "InMemory" ? "memory" : fs::path(data.getFullPathOnDisk(disk)) / part_name / "";
    auto entry = data.getContext()->getReplicatedFetchList().insert(
        storage_id.getDatabaseName(), storage_id.getTableName(),
        part_info.partition_id, part_name, new_part_path,
        replica_path, uri, to_detached, sum_files_size);

    in->setNextCallback(ReplicatedFetchReadCallback(*entry));

    size_t projections = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
        readBinary(projections, *in);

    MergeTreeData::DataPart::Checksums checksums;
    return part_type == "InMemory"
        ? downloadPartToMemory(part_name, part_uuid, metadata_snapshot, context, disk, *in, projections, throttler)
        : downloadPartToDisk(part_name, replica_path, to_detached, tmp_prefix_, sync, disk, *in, projections, checksums, throttler);
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToMemory(
    const String & part_name,
    const UUID & part_uuid,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    DiskPtr disk,
    PooledReadWriteBufferFromHTTP & in,
    size_t projections,
    ThrottlerPtr throttler)
{
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);

    auto data_part_storage = std::make_shared<DataPartStorageOnDisk>(
        volume,
        data.getRelativeDataPath(),
        part_name);

    auto data_part_storage_builder = std::make_shared<DataPartStorageBuilderOnDisk>(
        volume,
        data.getRelativeDataPath(),
        part_name);

    MergeTreeData::MutableDataPartPtr new_data_part =
        std::make_shared<MergeTreeDataPartInMemory>(data, part_name, data_part_storage);
    new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);

    for (auto i = 0ul; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);
        MergeTreeData::DataPart::Checksums checksums;
        if (!checksums.read(in))
            throw Exception("Cannot deserialize checksums", ErrorCodes::CORRUPTED_DATA);

        NativeReader block_in(in, 0);
        auto block = block_in.read();
        throttler->add(block.bytes());

        auto projection_part_storage = data_part_storage->getProjection(projection_name + ".proj");
        auto projection_part_storage_builder = data_part_storage_builder->getProjection(projection_name + ".proj");

        MergeTreePartInfo new_part_info("all", 0, 0, 0);
        MergeTreeData::MutableDataPartPtr new_projection_part =
            std::make_shared<MergeTreeDataPartInMemory>(data, projection_name, new_part_info, projection_part_storage, new_data_part.get());

        new_projection_part->is_temp = false;
        new_projection_part->setColumns(block.getNamesAndTypesList());
        MergeTreePartition partition{};
        new_projection_part->partition = std::move(partition);
        new_projection_part->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();

        MergedBlockOutputStream part_out(
            new_projection_part,
            projection_part_storage_builder,
            metadata_snapshot->projections.get(projection_name).metadata,
            block.getNamesAndTypesList(),
            {},
            CompressionCodecFactory::instance().get("NONE", {}),
            NO_TRANSACTION_PTR);

        part_out.write(block);
        part_out.finalizePart(new_projection_part, false);
        new_projection_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);
        new_data_part->addProjectionPart(projection_name, std::move(new_projection_part));
    }

    MergeTreeData::DataPart::Checksums checksums;
    if (!checksums.read(in))
        throw Exception("Cannot deserialize checksums", ErrorCodes::CORRUPTED_DATA);

    NativeReader block_in(in, 0);
    auto block = block_in.read();
    throttler->add(block.bytes());

    new_data_part->uuid = part_uuid;
    new_data_part->is_temp = true;
    new_data_part->setColumns(block.getNamesAndTypesList());
    new_data_part->minmax_idx->update(block, data.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));
    new_data_part->partition.create(metadata_snapshot, block, 0, context);

    MergedBlockOutputStream part_out(
        new_data_part, data_part_storage_builder, metadata_snapshot, block.getNamesAndTypesList(), {},
        CompressionCodecFactory::instance().get("NONE", {}), NO_TRANSACTION_PTR);

    part_out.write(block);
    part_out.finalizePart(new_data_part, false);
    new_data_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);

    return new_data_part;
}

void Fetcher::downloadBaseOrProjectionPartToDisk(
    const String & replica_path,
    DataPartStorageBuilderPtr & data_part_storage_builder,
    bool sync,
    PooledReadWriteBufferFromHTTP & in,
    MergeTreeData::DataPart::Checksums & checksums,
    ThrottlerPtr throttler) const
{
    size_t files;
    readBinary(files, in);

    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        /// File must be inside "absolute_part_path" directory.
        /// Otherwise malicious ClickHouse replica may force us to write to arbitrary path.
        String absolute_file_path = fs::weakly_canonical(fs::path(data_part_storage_builder->getRelativePath()) / file_name);
        if (!startsWith(absolute_file_path, fs::weakly_canonical(data_part_storage_builder->getRelativePath()).string()))
            throw Exception(ErrorCodes::INSECURE_PATH,
                "File path ({}) doesn't appear to be inside part path ({}). "
                "This may happen if we are trying to download part from malicious replica or logical error.",
                absolute_file_path, data_part_storage_builder->getRelativePath());

        auto file_out = data_part_storage_builder->writeFile(file_name, std::min<UInt64>(file_size, DBMS_DEFAULT_BUFFER_SIZE), {});
        HashingWriteBuffer hashing_out(*file_out);
        copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);

        if (blocker.isCancelled())
        {
            /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
            /// performing a poll with a not very large timeout.
            /// And now we check it only between read chunks (in the `copyData` function).
            data_part_storage_builder->removeRecursive();
            throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
        }

        MergeTreeDataPartChecksum::uint128 expected_hash;
        readPODBinary(expected_hash, in);

        if (expected_hash != hashing_out.getHash())
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                "Checksum mismatch for file {} transferred from {}",
                (fs::path(data_part_storage_builder->getFullPath()) / file_name).string(),
                replica_path);

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt" &&
            file_name != IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME)
            checksums.addFile(file_name, file_size, expected_hash);

        if (sync)
            hashing_out.sync();
    }
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToDisk(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix_,
    bool sync,
    DiskPtr disk,
    PooledReadWriteBufferFromHTTP & in,
    size_t projections,
    MergeTreeData::DataPart::Checksums & checksums,
    ThrottlerPtr throttler)
{
    static const String TMP_PREFIX = "tmp-fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;

    /// We will remove directory if it's already exists. Make precautions.
    if (tmp_prefix.empty() //-V560
        || part_name.empty()
        || std::string::npos != tmp_prefix.find_first_of("/.")
        || std::string::npos != part_name.find_first_of("/."))
        throw Exception("Logical error: tmp_prefix and part_name cannot be empty or contain '.' or '/' characters.", ErrorCodes::LOGICAL_ERROR);

    String part_dir = tmp_prefix + part_name;
    String part_relative_path = data.getRelativeDataPath() + String(to_detached ? "detached/" : "");

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);

    auto data_part_storage = std::make_shared<DataPartStorageOnDisk>(
        volume,
        part_relative_path,
        part_dir);

    DataPartStorageBuilderPtr data_part_storage_builder = std::make_shared<DataPartStorageBuilderOnDisk>(
        volume,
        part_relative_path,
        part_dir);

    if (data_part_storage_builder->exists())
    {
        LOG_WARNING(log, "Directory {} already exists, probably result of a failed fetch. Will remove it before fetching part.",
            data_part_storage_builder->getFullPath());
        data_part_storage_builder->removeRecursive();
    }

    data_part_storage_builder->createDirectories();

    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(data_part_storage->getRelativePath());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    for (auto i = 0ul; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);
        MergeTreeData::DataPart::Checksums projection_checksum;

        auto projection_part_storage = data_part_storage->getProjection(projection_name + ".proj");
        auto projection_part_storage_builder = data_part_storage_builder->getProjection(projection_name + ".proj");

        projection_part_storage_builder->createDirectories();
        downloadBaseOrProjectionPartToDisk(
            replica_path, projection_part_storage_builder, sync, in, projection_checksum, throttler);
        checksums.addFile(
            projection_name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
    }

    // Download the base part
    downloadBaseOrProjectionPartToDisk(replica_path, data_part_storage_builder, sync, in, checksums, throttler);

    assertEOF(in);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, data_part_storage);
    new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
    new_data_part->is_temp = true;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);
    new_data_part->checksums.checkEqual(checksums, false);
    return new_data_part;
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToDiskRemoteMeta(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix_,
    DiskPtr disk,
    PooledReadWriteBufferFromHTTP & in,
    ThrottlerPtr throttler)
{
    String part_id;
    readStringBinary(part_id, in);

    if (!disk->supportZeroCopyReplication() || !disk->checkUniqueId(part_id))
    {
        throw Exception(ErrorCodes::ZERO_COPY_REPLICATION_ERROR, "Part {} unique id {} doesn't exist on {}.", part_name, part_id, disk->getName());
    }

    LOG_DEBUG(log, "Downloading Part {} unique id {} metadata onto disk {}.",
        part_name, part_id, disk->getName());

    data.lockSharedDataTemporary(part_name, part_id, disk);

    static const String TMP_PREFIX = "tmp-fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;

    String part_dir = tmp_prefix + part_name;
    String part_relative_path = data.getRelativeDataPath() + String(to_detached ? "detached/" : "");

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);

    auto data_part_storage = std::make_shared<DataPartStorageOnDisk>(
        volume,
        part_relative_path,
        part_dir);

    DataPartStorageBuilderPtr data_part_storage_builder = std::make_shared<DataPartStorageBuilderOnDisk>(
        volume,
        part_relative_path,
        part_dir);

    if (data_part_storage->exists())
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory {} already exists.", data_part_storage->getFullPath());

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    volume->getDisk()->createDirectories(data_part_storage->getFullPath());

    size_t files;
    readBinary(files, in);

    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        String metadata_file = fs::path(data_part_storage->getFullPath()) / file_name;

        {
            auto file_out = std::make_unique<WriteBufferFromFile>(metadata_file, DBMS_DEFAULT_BUFFER_SIZE, -1, 0666, nullptr, 0);

            HashingWriteBuffer hashing_out(*file_out);

            copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);

            if (blocker.isCancelled())
            {
                /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
                /// performing a poll with a not very large timeout.
                /// And now we check it only between read chunks (in the `copyData` function).
                data_part_storage_builder->removeSharedRecursive(true);
                data_part_storage_builder->commit();
                throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
            }

            MergeTreeDataPartChecksum::uint128 expected_hash;
            readPODBinary(expected_hash, in);

            if (expected_hash != hashing_out.getHash())
            {
                throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                    "Checksum mismatch for file {} transferred from {}",
                    metadata_file, replica_path);
            }
        }
    }

    assertEOF(in);

    data_part_storage_builder->commit();

    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, data_part_storage);
    new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
    new_data_part->is_temp = true;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);

    data.lockSharedData(*new_data_part, /* replace_existing_lock = */ true, {});

    LOG_DEBUG(log, "Download of part {} unique id {} metadata onto disk {} finished.",
        part_name, part_id, disk->getName());

    return new_data_part;
}

}

}
