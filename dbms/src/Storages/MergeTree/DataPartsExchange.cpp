#include <Storages/MergeTree/DataPartsExchange.h>
#include <Common/CurrentMetrics.h>
#include <Common/NetException.h>
#include <Common/typeid_cast.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <ext/scope_guard.h>
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
    extern const int TOO_MUCH_SIMULTANEOUS_QUERIES;
    extern const int CANNOT_WRITE_TO_OSTREAM;
}

namespace DataPartsExchange
{

namespace
{

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
    if (blocker.isCancelled())
        throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

    String part_name = params.get("part");

    static std::atomic_uint total_sends {0};

    if ((data.settings.replicated_max_parallel_sends && total_sends >= data.settings.replicated_max_parallel_sends)
        || (data.settings.replicated_max_parallel_sends_for_table && data.current_table_sends >= data.settings.replicated_max_parallel_sends_for_table))
    {
        response.setStatus(std::to_string(HTTP_TOO_MANY_REQUESTS));
        response.setReason("Too many concurrent fetches, try again later");
        response.set("Retry-After", "10");
        response.setChunkedTransferEncoding(false);
        return;
    }
    ++total_sends;
    SCOPE_EXIT({--total_sends;});

    ++data.current_table_sends;
    SCOPE_EXIT({--data.current_table_sends;});


    LOG_TRACE(log, "Sending part " << part_name);

    try
    {
        auto storage_lock = owned_storage->lockStructure(false, __PRETTY_FUNCTION__);

        MergeTreeData::DataPartPtr part = findPart(part_name);

        std::shared_lock<std::shared_mutex> part_lock(part->columns_lock);

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedSend};

        /// We'll take a list of files from the list of checksums.
        MergeTreeData::DataPart::Checksums checksums = part->checksums;
        /// Add files that are not in the checksum list.
        checksums.files["checksums.txt"];
        checksums.files["columns.txt"];

        MergeTreeData::DataPart::Checksums data_checksums;

        writeBinary(checksums.files.size(), out);
        for (const auto & it : checksums.files)
        {
            String file_name = it.first;

            String path = data.getFullPath() + part_name + "/" + file_name;

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
    catch (const NetException & e)
    {
        /// Network error or error on remote side. No need to enquue part for check.
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
    bool to_detached)
{
    Poco::URI uri;
    uri.setScheme("http");
    uri.setHost(host);
    uri.setPort(port);
    uri.setQueryParameters(
    {
        {"endpoint", getEndpointId(replica_path)},
        {"part", part_name},
        {"compress", "false"}
    });

    ReadWriteBufferFromHTTP in{uri, Poco::Net::HTTPRequest::HTTP_POST, {}, timeouts};

    static const String TMP_PREFIX = "tmp_fetch_";
    String relative_part_path = String(to_detached ? "detached/" : "") + TMP_PREFIX + part_name;
    String absolute_part_path = data.getFullPath() + relative_part_path + "/";
    Poco::File part_file(absolute_part_path);

    if (part_file.exists())
        throw Exception("Directory " + absolute_part_path + " already exists.", ErrorCodes::DIRECTORY_ALREADY_EXISTS);

    CurrentMetrics::Increment metric_increment{CurrentMetrics::ReplicatedFetch};

    part_file.createDirectory();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data, part_name);
    new_data_part->relative_path = relative_part_path;
    new_data_part->is_temp = true;

    size_t files;
    readBinary(files, in);
    MergeTreeData::DataPart::Checksums checksums;
    for (size_t i = 0; i < files; ++i)
    {
        String file_name;
        UInt64 file_size;

        readStringBinary(file_name, in);
        readBinary(file_size, in);

        WriteBufferFromFile file_out(absolute_part_path + file_name);
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
            throw Exception("Checksum mismatch for file " + absolute_part_path + file_name + " transferred from " + replica_path);

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
