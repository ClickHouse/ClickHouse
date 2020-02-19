#include <Storages/MergeTree/DataPartsExchange.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
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
    extern const int ABORTED;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_PROTOCOL;
    extern const int INSECURE_PATH;
}

namespace DataPartsExchange
{

namespace
{

static constexpr auto REPLICATION_PROTOCOL_VERSION_WITHOUT_PARTS_SIZE = "0";
static constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE = "1";
static constexpr auto REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS = "2";


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
    String client_protocol_version = params.get("client_protocol_version", REPLICATION_PROTOCOL_VERSION_WITHOUT_PARTS_SIZE);

    String part_name = params.get("part");

    if (client_protocol_version != REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS
        && client_protocol_version != REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE
        && client_protocol_version != REPLICATION_PROTOCOL_VERSION_WITHOUT_PARTS_SIZE)
        throw Exception("Unsupported fetch protocol version", ErrorCodes::UNKNOWN_PROTOCOL);

    const auto data_settings = data.getSettings();

    /// Validation of the input that may come from malicious replica.
    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    static std::atomic_uint total_sends {0};

    if ((data_settings->replicated_max_parallel_sends && total_sends >= data_settings->replicated_max_parallel_sends)
        || (data_settings->replicated_max_parallel_sends_for_table && data.current_table_sends >= data_settings->replicated_max_parallel_sends_for_table))
    {
        response.setStatus(std::to_string(HTTP_TOO_MANY_REQUESTS));
        response.setReason("Too many concurrent fetches, try again later");
        response.set("Retry-After", "10");
        response.setChunkedTransferEncoding(false);
        return;
    }
    response.addCookie({"server_protocol_version", REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS});

    ++total_sends;
    SCOPE_EXIT({--total_sends;});

    ++data.current_table_sends;
    SCOPE_EXIT({--data.current_table_sends;});

    LOG_TRACE(log, "Sending part " << part_name);

    try
    {
        auto storage_lock = data.lockStructureForShare(false, RWLockImpl::NO_QUERY);

        MergeTreeData::DataPartPtr part = findPart(part_name);

        std::shared_lock<std::shared_mutex> part_lock(part->columns_lock);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

        /// We'll take a list of files from the list of checksums.
        MergeTreeData::DataPart::Checksums checksums = part->checksums;
        /// Add files that are not in the checksum list.
        checksums.files["checksums.txt"];
        checksums.files["columns.txt"];

        MergeTreeData::DataPart::Checksums data_checksums;

        if (client_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE || client_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
            writeBinary(checksums.getTotalSizeOnDisk(), out);

        if (client_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            WriteBufferFromOwnString ttl_infos_buffer;
            part->ttl_infos.write(ttl_infos_buffer);
            writeBinary(ttl_infos_buffer.str(), out);
        }

        writeBinary(checksums.files.size(), out);
        for (const auto & it : checksums.files)
        {
            String file_name = it.first;

            String path = part->getFullPath() + file_name;

            UInt64 size = Poco::File(path).getSize();

            writeStringBinary(it.first, out);
            writeBinary(size, out);

            ReadBufferFromFile file_in(path);
            HashingWriteBuffer hashing_out(out);
            copyData(file_in, hashing_out, blocker.getCounter());

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
        {"client_protocol_version", REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS},
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

    auto server_protocol_version = in.getResponseCookie("server_protocol_version", REPLICATION_PROTOCOL_VERSION_WITHOUT_PARTS_SIZE);


    ReservationPtr reservation;
    if (server_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE || server_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
    {
        size_t sum_files_size;
        readBinary(sum_files_size, in);
        if (server_protocol_version == REPLICATION_PROTOCOL_VERSION_WITH_PARTS_SIZE_AND_TTL_INFOS)
        {
            MergeTreeDataPart::TTLInfos ttl_infos;
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

    return downloadPart(part_name, replica_path, to_detached, tmp_prefix_, std::move(reservation), in);
}

MergeTreeData::MutableDataPartPtr Fetcher::downloadPart(
    const String & part_name,
    const String & replica_path,
    bool to_detached,
    const String & tmp_prefix_,
    const ReservationPtr reservation,
    PooledReadWriteBufferFromHTTP & in)
{

    size_t files;
    readBinary(files, in);

    static const String TMP_PREFIX = "tmp_fetch_";
    String tmp_prefix = tmp_prefix_.empty() ? TMP_PREFIX : tmp_prefix_;

    String relative_part_path = String(to_detached ? "detached/" : "") + tmp_prefix + part_name;
    String absolute_part_path = Poco::Path(data.getFullPathOnDisk(reservation->getDisk()) + relative_part_path + "/").absolute().toString();
    Poco::File part_file(absolute_part_path);

    if (part_file.exists())
        throw Exception("Directory " + absolute_part_path + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    part_file.createDirectory();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data, reservation->getDisk(), part_name);
    new_data_part->relative_path = relative_part_path;
    new_data_part->is_temp = true;


    MergeTreeData::DataPart::Checksums checksums;
    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        /// File must be inside "absolute_part_path" directory.
        /// Otherwise malicious ClickHouse replica may force us to write to arbitrary path.
        String absolute_file_path = Poco::Path(absolute_part_path + file_name).absolute().toString();
        if (!startsWith(absolute_file_path, absolute_part_path))
            throw Exception("File path (" + absolute_file_path + ") doesn't appear to be inside part path (" + absolute_part_path + ")."
                " This may happen if we are trying to download part from malicious replica or logical error.",
                ErrorCodes::INSECURE_PATH);

        WriteBufferFromFile file_out(absolute_file_path);
        HashingWriteBuffer hashing_out(file_out);
        copyData(in, hashing_out, file_size, blocker.getCounter());

        if (blocker.isCancelled())
        {
            /// NOTE The is_cancelled flag also makes sense to check every time you read over the network, performing a poll with a not very large timeout.
            /// And now we check it only between read chunks (in the `copyData` function).
            part_file.remove(true);
            throw Exception("Fetching of part was cancelled", ErrorCodes::ABORTED);
        }

        MergeTreeDataPartChecksum::uint128 expected_hash;
        readPODBinary(expected_hash, in);

        if (expected_hash != hashing_out.getHash())
            throw Exception("Checksum mismatch for file " + absolute_part_path + file_name + " transferred from " + replica_path,
                ErrorCodes::CHECKSUM_DOESNT_MATCH);

        if (file_name != "checksums.txt" &&
            file_name != "columns.txt")
            checksums.addFile(file_name, file_size, expected_hash);
    }

    assertEOF(in);

    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, false);
    new_data_part->checksums.checkEqual(checksums, false);

    return new_data_part;
}

}

}
