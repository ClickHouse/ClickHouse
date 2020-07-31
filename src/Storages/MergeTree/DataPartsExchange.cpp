#include <Storages/MergeTree/DataPartsExchange.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Disks/createVolume.h>
#include <Disks/SingleDiskVolume.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/HTTPCommon.h>
#include <ext/scope_guard.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPRequest.h>


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
}

namespace DataPartsExchange
{

namespace
{
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE = 1;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS = 2;
constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE = 3;


std::string getEndpointId(const std::string & node_id)
{
    return "DataPartsExchange:" + node_id;
}

}

std::string Service::getId(const std::string & node_id) const
{
    return getEndpointId(node_id);
}

void Service::processQuery(const Poco::Net::HTMLForm & params, ReadBuffer & /*body*/, WriteBuffer & out, Poco::Net::HTTPServerResponse & response)
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
    response.addCookie({"server_protocol_version", toString(std::min(client_protocol_version, REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE))});

    ++total_sends;
    SCOPE_EXIT({--total_sends;});

    ++data.current_table_sends;
    SCOPE_EXIT({--data.current_table_sends;});

    LOG_TRACE(log, "Sending part {}", part_name);

    try
    {
        MergeTreeData::DataPartPtr part = findPart(part_name);

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

        if (isInMemoryPart(part))
            sendPartFromMemory(part, out);
        else
            sendPartFromDisk(part, out);
    }
    catch (const NetException &)
    {
        /// Network error or error on remote side. No need to enqueue part for check.
        throw;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::ABORTED && e.code() != ErrorCodes::CANNOT_WRITE_TO_OSTREAM)
            data.reportBrokenPart(part_name);
        throw;
    }
    catch (...)
    {
        data.reportBrokenPart(part_name);
        throw;
    }
}

void Service::sendPartFromMemory(const MergeTreeData::DataPartPtr & part, WriteBuffer & out)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr();
    auto part_in_memory = asInMemoryPart(part);
    if (!part_in_memory)
        throw Exception("Part " + part->name + " is not stored in memory", ErrorCodes::LOGICAL_ERROR);

    NativeBlockOutputStream block_out(out, 0, metadata_snapshot->getSampleBlock());
    part->checksums.write(out);
    block_out.write(part_in_memory->block);
}

void Service::sendPartFromDisk(const MergeTreeData::DataPartPtr & part, WriteBuffer & out)
{
    /// We'll take a list of files from the list of checksums.
    MergeTreeData::DataPart::Checksums checksums = part->checksums;
    /// Add files that are not in the checksum list.
    checksums.files["checksums.txt"] = {};
    checksums.files["columns.txt"] = {};

    MergeTreeData::DataPart::Checksums data_checksums;

    writeBinary(checksums.files.size(), out);
    for (const auto & it : checksums.files)
    {
        String file_name = it.first;

        auto disk = part->volume->getDisk();
        String path = part->getFullRelativePath() + file_name;

        UInt64 size = disk->getFileSize(path);

        writeStringBinary(it.first, out);
        writeBinary(size, out);

        auto file_in = disk->readFile(path);
        HashingWriteBuffer hashing_out(out);
        copyData(*file_in, hashing_out, blocker.getCounter());

        if (blocker.isCancelled())
            throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

        if (hashing_out.count() != size)
            throw Exception("Unexpected size of file " + path, ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART);

        writePODBinary(hashing_out.getHash(), out);

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt")
            data_checksums.addFile(file_name, hashing_out.count(), hashing_out.getHash());
    }

    part->checksums.checkEqual(data_checksums, false);
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
    const String & part_name,
    const String & replica_path,
    const String & host,
    int port,
    const ConnectionTimeouts & timeouts,
    const String & user,
    const String & password,
    const String & interserver_scheme,
    bool to_detached,
    const String & tmp_prefix_)
{
    if (blocker.isCancelled())
        throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);

    /// Validation of the input that may come from malicious replica.
    MergeTreePartInfo::fromPartName(part_name, data.format_version);
    const auto data_settings = data.getSettings();

    Poco::URI uri;
    uri.setScheme(interserver_scheme);
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
    {
        {"endpoint",                getEndpointId(replica_path)},
        {"part",                    part_name},
        {"client_protocol_version", toString(REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)},
        {"compress",                "false"}
    });

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

    ReservationPtr reservation;
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE)
    {
        size_t sum_files_size;
        readBinary(sum_files_size, in);
        if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            IMergeTreeDataPart::TTLInfos ttl_infos;
            String ttl_infos_string;
            readBinary(ttl_infos_string, in);
            ReadBufferFromString ttl_infos_buffer(ttl_infos_string);
            assertString("ttl format version: 1\n", ttl_infos_buffer);
            ttl_infos.read(ttl_infos_buffer);
            reservation = data.reserveSpacePreferringTTLRules(sum_files_size, ttl_infos, std::time(nullptr));
        }
        else
            reservation = data.reserveSpace(sum_files_size);
    }
    else
    {
        /// We don't know real size of part because sender server version is too old
        reservation = data.makeEmptyReservationOnLargestDisk();
    }

    String part_type = "Wide";
    if (server_protocol_version >= REPLICATION_PROTOCOL_VERSION_WITH_PARTS_TYPE)
        readStringBinary(part_type, in);

    return part_type == "InMemory" ? downloadPartToMemory(part_name, metadata_snapshot, std::move(reservation), in)
        : downloadPartToDisk(part_name, replica_path, to_detached, tmp_prefix_, std::move(reservation), in);
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToMemory(
    const String & part_name,
    const StorageMetadataPtr & metadata_snapshot,
    ReservationPtr reservation,
    PooledReadWriteBufferFromHTTP & in)
{
    MergeTreeData::DataPart::Checksums checksums;
    if (!checksums.read(in))
        throw Exception("Cannot deserialize checksums", ErrorCodes::CORRUPTED_DATA);

    NativeBlockInputStream block_in(in, 0);
    auto block = block_in.read();

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, reservation->getDisk());
    MergeTreeData::MutableDataPartPtr new_data_part =
        std::make_shared<MergeTreeDataPartInMemory>(data, part_name, volume);

    new_data_part->is_temp = true;
    new_data_part->setColumns(block.getNamesAndTypesList());
    new_data_part->minmax_idx.update(block, data.minmax_idx_columns);
    new_data_part->partition.create(metadata_snapshot, block, 0);

    MergedBlockOutputStream part_out(new_data_part, metadata_snapshot, block.getNamesAndTypesList(), {}, nullptr);
    part_out.writePrefix();
    part_out.write(block);
    part_out.writeSuffixAndFinalizePart(new_data_part);
    new_data_part->checksums.checkEqual(checksums, /* have_uncompressed = */ true);

    return new_data_part;
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPartToDisk(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix_,
    const ReservationPtr reservation,
    PooledReadWriteBufferFromHTTP & in)
{
    size_t files;
    readBinary(files, in);

    auto disk = reservation->getDisk();

    static const String TMP_PREFIX = "tmp_fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;

    String part_relative_path = String(to_detached ? "detached/" : "") + tmp_prefix + part_name;
    String part_download_path = data.getRelativeDataPath() + part_relative_path + "/";

    if (disk->exists(part_download_path))
        throw Exception("Directory " + fullPath(disk, part_download_path) + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    disk->createDirectories(part_download_path);

    MergeTreeData::DataPart::Checksums checksums;
    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        /// File must be inside "absolute_part_path" directory.
        /// Otherwise malicious ClickHouse replica may force us to write to arbitrary path.
        String absolute_file_path = Poco::Path(part_download_path + file_name).absolute().toString();
        if (!startsWith(absolute_file_path, Poco::Path(part_download_path).absolute().toString()))
            throw Exception("File path (" + absolute_file_path + ") doesn't appear to be inside part path (" + part_download_path + ")."
                " This may happen if we are trying to download part from malicious replica or logical error.",
                ErrorCodes::INSECURE_PATH);

        auto file_out = disk->writeFile(part_download_path + file_name);
        HashingWriteBuffer hashing_out(*file_out);
        copyData(in, hashing_out, file_size, blocker.getCounter());

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
            throw Exception("Checksum mismatch for file " + fullPath(disk, part_download_path + file_name) + " transferred from " + replica_path,
                ErrorCodes::CHECKSUM_DOESNT_MATCH);

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt")
            checksums.addFile(file_name, file_size, expected_hash);
    }

    assertEOF(in);

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, volume, part_relative_path);
    new_data_part->is_temp = true;
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);
    new_data_part->checksums.checkEqual(checksums, false);

    return new_data_part;
}

}

}
