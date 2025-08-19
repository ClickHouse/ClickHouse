#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <Storages/Distributed/Defines.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Formats/NativeReader.h>
#include <Core/ProtocolDefines.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CHECKSUM_DOESNT_MATCH;
}

DistributedAsyncInsertHeader DistributedAsyncInsertHeader::read(ReadBufferFromFile & in, LoggerPtr log)
{
    DistributedAsyncInsertHeader distributed_header;

    UInt64 query_size;
    readVarUInt(query_size, in);

    if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER)
    {
        /// Read the header as a string.
        String header_data;
        readStringBinary(header_data, in);

        /// Check the checksum of the header.
        CityHash_v1_0_2::uint128 expected_checksum;
        readPODBinary(expected_checksum, in);
        CityHash_v1_0_2::uint128 calculated_checksum =
            CityHash_v1_0_2::CityHash128(header_data.data(), header_data.size());
        if (expected_checksum != calculated_checksum)
        {
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                            "Checksum of extra info doesn't match: corrupted data. Reference: {}. Actual: {}.",
                            getHexUIntLowercase(expected_checksum), getHexUIntLowercase(calculated_checksum));
        }

        /// Read the parts of the header.
        ReadBufferFromString header_buf(header_data);

        readVarUInt(distributed_header.revision, header_buf);
        if (DBMS_TCP_PROTOCOL_VERSION < distributed_header.revision)
        {
            LOG_WARNING(log, "ClickHouse shard version is older than ClickHouse initiator version. It may lack support for new features.");
        }

        readStringBinary(distributed_header.insert_query, header_buf);
        distributed_header.insert_settings.read(header_buf);

        if (header_buf.hasPendingData())
            distributed_header.client_info.read(header_buf, distributed_header.revision);

        if (header_buf.hasPendingData())
        {
            readVarUInt(distributed_header.rows, header_buf);
            readVarUInt(distributed_header.bytes, header_buf);
            readStringBinary(distributed_header.block_header_string, header_buf);
        }

        if (header_buf.hasPendingData())
        {
            NativeReader header_block_in(header_buf, distributed_header.revision);
            distributed_header.block_header = header_block_in.read();
            if (!distributed_header.block_header)
                throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                    "Cannot read header from the {} batch. Data was written with protocol version {}, current version: {}",
                        in.getFileName(), distributed_header.revision, DBMS_TCP_PROTOCOL_VERSION);
        }

        if (header_buf.hasPendingData())
        {
            readVarUInt(distributed_header.shard_num, header_buf);
            readStringBinary(distributed_header.cluster, header_buf);
            readStringBinary(distributed_header.distributed_table, header_buf);
            readStringBinary(distributed_header.remote_table, header_buf);
        }

        /// Add handling new data here, for example:
        ///
        /// if (header_buf.hasPendingData())
        ///     readVarUInt(my_new_data, header_buf);
        ///
        /// And note that it is safe, because we have checksum and size for header.

        return distributed_header;
    }

    if (query_size == DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT)
    {
        distributed_header.insert_settings.read(in, SettingsWriteFormat::BINARY);
        readStringBinary(distributed_header.insert_query, in);
        return distributed_header;
    }

    distributed_header.insert_query.resize(query_size);
    in.readStrict(distributed_header.insert_query.data(), query_size);

    return distributed_header;
}

OpenTelemetry::TracingContextHolderPtr DistributedAsyncInsertHeader::createTracingContextHolder(const char * function, std::shared_ptr<OpenTelemetrySpanLog> open_telemetry_span_log) const
{
    OpenTelemetry::TracingContextHolderPtr trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>(
        function,
        client_info.client_trace_context,
        std::move(open_telemetry_span_log));
    trace_context->root_span.addAttribute("clickhouse.shard_num", shard_num);
    trace_context->root_span.addAttribute("clickhouse.cluster", cluster);
    trace_context->root_span.addAttribute("clickhouse.distributed", distributed_table);
    trace_context->root_span.addAttribute("clickhouse.remote", remote_table);
    trace_context->root_span.addAttribute("clickhouse.rows", rows);
    trace_context->root_span.addAttribute("clickhouse.bytes", bytes);
    return trace_context;
}

}
