#include <Storages/MergeTree/DataPartsExchange.h>

#include <DataStreams/NativeBlockOutputStream.h>
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
#include <IO/createReadBufferFromFileBase.h>
#include <common/scope_guard.h>
#include <Poco/Net/HTTPRequest.h>


namespace fs = std::filesystem;

namespace CurrentMetrics
{
    extern const Metric ReplicatedSend;
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
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_S3_COPY = 6;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION = 7;


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

    static std::atomic_uint total_sends {0};

    if ((data_settings->replicated_max_parallel_sends
            && total_sends >= data_settings->replicated_max_parallel_sends)
        || (data_settings->replicated_max_parallel_sends_for_table
            && data.current_table_sends >= data_settings->replicated_max_parallel_sends_for_table))
    {
        response.setStatus(std::to_string(HTTP_TOO_MANY_REQUESTS));
        response.setReason("Too many concurrent fetches, try again later");
        response.set("Retry-After", "10");
        response.setChunkedTransferEncoding(false);
        return;
    }

    /// We pretend to work as older server version, to be sure that client will correctly process our version
    response.addCookie({"server_protocol_version", toString(std::min(client_protocol_version, REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION))});

    ++total_sends;
    SCOPE_EXIT({--total_sends;});

    ++data.current_table_sends;
    SCOPE_EXIT({--data.current_table_sends;});

    LOG_TRACE(log, "Sending part {}", part_name);

    MergeTreeData::DataPartPtr part;

    auto report_broken_part = [&]()
    {
        if (part && part->isProjectionPart())
        {
            data.reportBrokenPart(part->getParentPart()->name);
        }
        else
        {
            data.reportBrokenPart(part_name);
        }
    };

    try
    {
        part = findPart(part_name);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

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

        bool try_use_s3_copy = false;

        if (data_settings->allow_remote_fs_zero_copy_replication
                && client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_S3_COPY)
        { /// if source and destination are in the same S3 storage we try to use S3 CopyObject request first
            int send_s3_metadata = parse<int>(params.get("send_s3_metadata", "0"));
            if (send_s3_metadata == 1)
            {
                auto disk = part->volume->getDisk();
                if (disk->getType() == DB::DiskType::Type::S3)
                {
                    try_use_s3_copy = true;
                }
            }
        }
        if (try_use_s3_copy)
        {
            response.addCookie({"send_s3_metadata", "1"});
            sendPartS3Metadata(part, out);
        }
        else if (client_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
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
            throw Exception("Projection " + name + " of part " + part->name + " is not stored in memory", ErrorCodes::LOGICAL_ERROR);

        writeStringBinary(name, out);
        projection->checksums.write(out);
        NativeBlockOutputStream block_out(out, 0, projection_sample_block);
        block_out.write(part_in_memory->block);
    }

    auto part_in_memory = asInMemoryPart(part);
    if (!part_in_memory)
        throw Exception("Part " + part->name + " is not stored in memory", ErrorCodes::LOGICAL_ERROR);

    NativeBlockOutputStream block_out(out, 0, metadata_snapshot->getSampleBlock());
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

    auto disk = part->volume->getDisk();
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

        String path = fs::path(part->getFullRelativePath()) / file_name;

        UInt64 size = disk->getFileSize(path);

        writeStringBinary(it.first, out);
        writeBinary(size, out);

        auto file_in = disk->readFile(path);
        HashingWriteBuffer hashing_out(out);
        copyDataWithThrottler(*file_in, hashing_out, blocker.getCounter(), data.getSendsThrottler());

        if (blocker.isCancelled())
            throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

        if (hashing_out.count() != size)
            throw Exception("Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

        writePODBinary(hashing_out.getHash(), out);

        if (!file_names_without_checksums.count(file_name))
            data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
    }

    part->checksums.checkEqual(data_checksums, false);
    return data_checksums;
}

void Service::sendPartS3Metadata(const MergeTreeData::DataPartPtr & part, WriteBuffer & out)
{
    /// We'll take a list of files from the list of checksums.
    MergeTreeData::DataPart::Checksums checksums = part->checksums;
    /// Add files that are not in the checksum list.
    auto file_names_without_checksums = part->getFileNamesWithoutChecksums();
    for (const auto & file_name : file_names_without_checksums)
        checksums.files[file_name] = {};

    auto disk = part->volume->getDisk();
    if (disk->getType() != DB::DiskType::Type::S3)
        throw Exception("S3 disk is not S3 anymore", ErrorCodes::LOGICAL_ERROR);

    part->storage.lockSharedData(*part);

    String part_id = part->getUniqueId();
    writeStringBinary(part_id, out);

    writeBinary(checksums.files.size(), out);
    for (const auto & it : checksums.files)
    {
        String file_name = it.first;

        String metadata_file = fs::path(disk->getPath()) / part->getFullRelativePath() / file_name;

        fs::path metadata(metadata_file);

        if (!fs::exists(metadata))
            throw Exception("S3 metadata '" + file_name + "' is not exists", ErrorCodes::CORRUPTED_DATA);
        if (!fs::is_regular_file(metadata))
            throw Exception("S3 metadata '" + file_name + "' is not a file", ErrorCodes::CORRUPTED_DATA);
        UInt64 file_size = fs::file_size(metadata);

        writeStringBinary(it.first, out);
        writeBinary(file_size, out);

        auto file_in = createReadBufferFromFileBase(metadata_file, 0, 0, 0, nullptr, DBMS_DEFAULT_BUFFER_SIZE);
        HashingWriteBuffer hashing_out(out);
        copyDataWithThrottler(*file_in, hashing_out, blocker.getCounter(), data.getSendsThrottler());
        if (blocker.isCancelled())
            throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

        if (hashing_out.count() != file_size)
            throw Exception("Unexpected size of file " + metadata_file, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

        writePODBinary(hashing_out.getHash(), out);
    }
}

MergeTreeData::DataPartPtr Service::findPart(const String & name)
{
    /// It is important to include PreCommitted and Outdated parts here because remote replicas cannot reliably
    /// determine the local state of the part, so queries for the parts in these states are completely normal.
    auto part = data.getPartIfExists(
        name, {MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});
    if (part)
        return part;

    throw Exception("No part " + name + " in table", ErrorCodes::NO_SUCH_DATA_PART);
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
    bool try_use_s3_copy,
    const DiskPtr disk_s3)
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

    if (try_use_s3_copy && disk_s3 && disk_s3->getType() != DB::DiskType::Type::S3)
        throw Exception("Try to fetch shared s3 part on non-s3 disk", ErrorCodes::LOGICAL_ERROR);

    Disks disks_s3;

    if (!data_settings->allow_remote_fs_zero_copy_replication)
        try_use_s3_copy = false;

    if (try_use_s3_copy)
    {
        if (disk_s3)
            disks_s3.push_back(disk_s3);
        else
        {
            disks_s3 = data.getDisksByType(DiskType::Type::S3);

            if (disks_s3.empty())
                try_use_s3_copy = false;
        }
    }

    if (try_use_s3_copy)
    {
        uri.addQueryParameter("send_s3_metadata", "1");
    }

    Poco::Net::HTTPBasicCredentials creds{};
    if (!user.empty())
    {
        creds.setUsername(user);
        creds.setPassword(password);
    }

    PooledReadWriteBufferFromHTTP in{
        uri,
        Poco::Net::HTTPRequest::HTTP_POST,
        {},
        timeouts,
        creds,
        DBMS_DEFAULT_BUFFER_SIZE,
        0, /* no redirects */
        data_settings->replicated_max_parallel_fetches_for_host
    };

    int server_protocol_version = parse<int>(in.getResponseCookie("server_protocol_version", "0"));

    int send_s3 = parse<int>(in.getResponseCookie("send_s3_metadata", "0"));

    if (send_s3 == 1)
    {
        if (server_protocol_version < REPLICATION_PROTOCOL_VERSION_WITH_PARTS_S3_COPY)
            throw Exception("Got 'send_s3_metadata' cookie with old protocol version", ErrorCodes::LOGICAL_ERROR);
        if (!try_use_s3_copy)
            throw Exception("Got 'send_s3_metadata' cookie when was not requested", ErrorCodes::LOGICAL_ERROR);

        size_t sum_files_size = 0;
        readBinary(sum_files_size, in);
        IMergeTreeDataPart::TTLInfos ttl_infos;
        String ttl_infos_string;
        readBinary(ttl_infos_string, in);
        ReadBufferFromString ttl_infos_buffer(ttl_infos_string);
        assertString("ttl format version: 1\n", ttl_infos_buffer);
        ttl_infos.read(ttl_infos_buffer);

        ReservationPtr reservation
            = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, &ttl_infos, true);
        if (!reservation)
            reservation
                = data.reserveSpacePreferringTTLRules(metadata_snapshot, sum_files_size, ttl_infos, std::time(nullptr), 0, true);
        if (reservation)
        {
            /// When we have multi-volume storage, one of them was chosen, depends on TTL, free space, etc.
            /// Chosen one may be S3 or not.
            DiskPtr disk = reservation->getDisk();
            if (disk && disk->getType() == DiskType::Type::S3)
            {
                for (const auto & d : disks_s3)
                {
                    if (d->getPath() == disk->getPath())
                    {
                        Disks disks_tmp = { disk };
                        disks_s3.swap(disks_tmp);
                        break;
                    }
                }
            }
        }

        String part_type = "Wide";
        readStringBinary(part_type, in);
        if (part_type == "InMemory")
            throw Exception("Got 'send_s3_metadata' cookie for in-memory part", ErrorCodes::INCORRECT_PART_TYPE);

        UUID part_uuid = UUIDHelpers::Nil;

        /// Always true due to values of constants. But we keep this condition just in case.
        if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID) //-V547
            readUUIDText(part_uuid, in);

        try
        {
            return downloadPartToS3(part_name, replica_path, to_detached, tmp_prefix_, std::move(disks_s3), in, throttler);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::S3_ERROR)
                throw;
            /// Try again but without S3 copy
            return fetchPart(metadata_snapshot, context, part_name, replica_path, host, port, timeouts,
                user, password, interserver_scheme, throttler, to_detached, tmp_prefix_, nullptr, false);
        }
    }

    ReservationPtr reservation;
    size_t sum_files_size = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
    {
        readBinary(sum_files_size, in);
        if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            IMergeTreeDataPart::TTLInfos ttl_infos;
            String ttl_infos_string;
            readBinary(ttl_infos_string, in);
            ReadBufferFromString ttl_infos_buffer(ttl_infos_string);
            assertString("ttl format version: 1\n", ttl_infos_buffer);
            ttl_infos.read(ttl_infos_buffer);
            reservation
                = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, &ttl_infos, true);
            if (!reservation)
                reservation
                    = data.reserveSpacePreferringTTLRules(metadata_snapshot, sum_files_size, ttl_infos, std::time(nullptr), 0, true);
        }
        else
        {
            reservation = data.balancedReservation(metadata_snapshot, sum_files_size, 0, part_name, part_info, {}, tagger_ptr, nullptr);
            if (!reservation)
                reservation = data.reserveSpace(sum_files_size);
        }
    }
    else
    {
        /// We don't know real size of part because sender server version is too old
        reservation = data.makeEmptyReservationOnLargestDisk();
    }

    bool sync = (data_settings->min_compressed_bytes_to_fsync_after_fetch
                    && sum_files_size >= data_settings->min_compressed_bytes_to_fsync_after_fetch);

    String part_type = "Wide";
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
        readStringBinary(part_type, in);

    UUID part_uuid = UUIDHelpers::Nil;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_UUID)
        readUUIDText(part_uuid, in);

    auto storage_id = data.getStorageID();
    String new_part_path = part_type == "InMemory" ? "memory" : fs::path(data.getFullPathOnDisk(reservation->getDisk())) / part_name / "";
    auto entry = data.getContext()->getReplicatedFetchList().insert(
        storage_id.getDatabaseName(), storage_id.getTableName(),
        part_info.partition_id, part_name, new_part_path,
        replica_path, uri, to_detached, sum_files_size);

    in.setNextCallback(ReplicatedFetchReadCallback(*entry));


    size_t projections = 0;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_PROJECTION)
        readBinary(projections, in);

    MergeTreeData::DataPart::Checksums checksums;
    return part_type == "InMemory"
        ? downloadPartToMemory(part_name, part_uuid, metadata_snapshot, context, std::move(reservation), in, projections, throttler)
        : downloadPartToDisk(part_name, replica_path, to_detached, tmp_prefix_, sync, reservation->getDisk(), in, projections, checksums, throttler);
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToMemory(
    const String & part_name,
    const UUID & part_uuid,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    ReservationPtr reservation,
    PooledReadWriteBufferFromHTTP & in,
    size_t projections,
    ThrottlerPtr throttler)
{
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, reservation->getDisk(), 0);
    MergeTreeData::MutableDataPartPtr new_data_part =
        std::make_shared<MergeTreeDataPartInMemory>(data, part_name, volume);

    for (auto i = 0ul; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);
        MergeTreeData::DataPart::Checksums checksums;
        if (!checksums.read(in))
            throw Exception("Cannot deserialize checksums", ErrorCodes::CORRUPTED_DATA);

        NativeBlockInputStream block_in(in, 0);
        auto block = block_in.read();
        throttler->add(block.bytes());

        MergeTreePartInfo new_part_info("all", 0, 0, 0);
        MergeTreeData::MutableDataPartPtr new_projection_part =
            std::make_shared<MergeTreeDataPartInMemory>(data, projection_name, new_part_info, volume, projection_name, new_data_part.get());

        new_projection_part->is_temp = false;
        new_projection_part->setColumns(block.getNamesAndTypesList());
        MergeTreePartition partition{};
        IMergeTreeDataPart::MinMaxIndex minmax_idx{};
        new_projection_part->partition = std::move(partition);
        new_projection_part->minmax_idx = std::move(minmax_idx);

        MergedBlockOutputStream part_out(
            new_projection_part,
            metadata_snapshot->projections.get(projection_name).metadata,
            block.getNamesAndTypesList(),
            {},
            CompressionCodecFactory::instance().get("NONE", {}));
        part_out.writePrefix();
        part_out.write(block);
        part_out.writeSuffixAndFinalizePart(new_projection_part);
        new_projection_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);
        new_data_part->addProjectionPart(projection_name, std::move(new_projection_part));
    }

    MergeTreeData::DataPart::Checksums checksums;
    if (!checksums.read(in))
        throw Exception("Cannot deserialize checksums", ErrorCodes::CORRUPTED_DATA);

    NativeBlockInputStream block_in(in, 0);
    auto block = block_in.read();
    throttler->add(block.bytes());

    new_data_part->uuid = part_uuid;
    new_data_part->is_temp = true;
    new_data_part->setColumns(block.getNamesAndTypesList());
    new_data_part->minmax_idx.update(block, data.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));
    new_data_part->partition.create(metadata_snapshot, block, 0, context);

    MergedBlockOutputStream part_out(
        new_data_part, metadata_snapshot, block.getNamesAndTypesList(), {}, CompressionCodecFactory::instance().get("NONE", {}));
    part_out.writePrefix();
    part_out.write(block);
    part_out.writeSuffixAndFinalizePart(new_data_part);
    new_data_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);

    return new_data_part;
}

void Fetcher::downloadBaseOrProjectionPartToDisk(
    const String & replica_path,
    const String & part_download_path,
    bool sync,
    DiskPtr disk,
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
        String absolute_file_path = fs::weakly_canonical(fs::path(part_download_path) / file_name);
        if (!startsWith(absolute_file_path, fs::weakly_canonical(part_download_path).string()))
            throw Exception("File path (" + absolute_file_path + ") doesn't appear to be inside part path (" + part_download_path + ")."
                " This may happen if we are trying to download part from malicious replica or logical error.",
                ErrorCodes::INSECURE_PATH);

        auto file_out = disk->writeFile(fs::path(part_download_path) / file_name);
        HashingWriteBuffer hashing_out(*file_out);
        copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);

        if (blocker.isCancelled())
        {
            /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
            /// performing a poll with a not very large timeout.
            /// And now we check it only between read chunks (in the `copyData` function).
            disk->removeRecursive(part_download_path);
            throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
        }

        MergeTreeDataPartChecksum::uint128 expected_hash;
        readPODBinary(expected_hash, in);

        if (expected_hash != hashing_out.getHash())
            throw Exception("Checksum mismatch for file " + fullPath(disk, (fs::path(part_download_path) / file_name).string()) + " transferred from " + replica_path,
                ErrorCodes::CHECKSUM_DOESNT_MATCH);

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

    String part_relative_path = String(to_detached ? "detached/" : "") + tmp_prefix + part_name;
    String part_download_path = data.getRelativeDataPath() + part_relative_path + "/";

    if (disk->exists(part_download_path))
    {
        LOG_WARNING(log, "Directory {} already exists, probably result of a failed fetch. Will remove it before fetching part.",
            fullPath(disk, part_download_path));
        disk->removeRecursive(part_download_path);
    }

    disk->createDirectories(part_download_path);

    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(part_download_path);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    for (auto i = 0ul; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);
        MergeTreeData::DataPart::Checksums projection_checksum;
        disk->createDirectories(part_download_path + projection_name + ".proj/");
        downloadBaseOrProjectionPartToDisk(
            replica_path, part_download_path + projection_name + ".proj/", sync, disk, in, projection_checksum, throttler);
        checksums.addFile(
            projection_name + ".proj", projection_checksum.getTotalSizeOnDisk(), projection_checksum.getTotalChecksumUInt128());
    }

    // Download the base part
    downloadBaseOrProjectionPartToDisk(replica_path, part_download_path, sync, disk, in, checksums, throttler);

    assertEOF(in);
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, volume, part_relative_path);
    new_data_part->is_temp = true;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);
    new_data_part->checksums.checkEqual(checksums, false);
    return new_data_part;
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToS3(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix_,
    const Disks & disks_s3,
    PooledReadWriteBufferFromHTTP & in,
    ThrottlerPtr throttler)
{
    if (disks_s3.empty())
        throw Exception("No S3 disks anymore", ErrorCodes::LOGICAL_ERROR);

    String part_id;
    readStringBinary(part_id, in);

    DiskPtr disk = disks_s3[0];

    for (const auto & disk_s3 : disks_s3)
    {
        if (disk_s3->checkUniqueId(part_id))
        {
            disk = disk_s3;
            break;
        }
    }

    static const String TMP_PREFIX = "tmp-fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;

    String part_relative_path = String(to_detached ? "detached/" : "") + tmp_prefix + part_name;
    String part_download_path = fs::path(data.getRelativeDataPath()) / part_relative_path / "";

    if (disk->exists(part_download_path))
        throw Exception("Directory " + fullPath(disk, part_download_path) + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    disk->createDirectories(part_download_path);

    size_t files;
    readBinary(files, in);

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);

    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        String data_path = fs::path(part_download_path) / file_name;
        String metadata_file = fullPath(disk, data_path);

        {
            auto file_out = std::make_unique<WriteBufferFromFile>(metadata_file, DBMS_DEFAULT_BUFFER_SIZE, -1, 0666, nullptr, 0);

            HashingWriteBuffer hashing_out(*file_out);

            copyDataWithThrottler(in, hashing_out, file_size, blocker.getCounter(), throttler);

            if (blocker.isCancelled())
            {
                /// NOTE The is_cancelled flag also makes sense to check every time you read over the network,
                /// performing a poll with a not very large timeout.
                /// And now we check it only between read chunks (in the `copyData` function).
                disk->removeSharedRecursive(part_download_path, true);
                throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
            }

            MergeTreeDataPartChecksum::uint128 expected_hash;
            readPODBinary(expected_hash, in);

            if (expected_hash != hashing_out.getHash())
            {
                throw Exception("Checksum mismatch for file " + metadata_file + " transferred from " + replica_path,
                    ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }
    }

    assertEOF(in);

    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, volume, part_relative_path);
    new_data_part->is_temp = true;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);

    new_data_part->storage.lockSharedData(*new_data_part);

    return new_data_part;
}

}

}
