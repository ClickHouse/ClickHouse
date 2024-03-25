#pragma once

#include <Core/Settings.h>
#include <Core/Block.h>
#include <Interpreters/ClientInfo.h>
#include <base/types.h>
#include <string>

namespace DB
{

class ReadBufferFromFile;

namespace OpenTelemetry
{
struct TracingContextHolder;
using TracingContextHolderPtr = std::unique_ptr<TracingContextHolder>;
}

/// Header for the binary files that are stored on disk for async INSERT into Distributed.
struct DistributedAsyncInsertHeader
{
    UInt64 revision = 0;
    Settings insert_settings;
    std::string insert_query;
    ClientInfo client_info;

    /// .bin file cannot have zero rows/bytes.
    size_t rows = 0;
    size_t bytes = 0;

    UInt32 shard_num = 0;
    std::string cluster;
    std::string distributed_table;
    std::string remote_table;

    /// dumpStructure() of the header -- obsolete
    std::string block_header_string;
    Block block_header;

    static DistributedAsyncInsertHeader read(ReadBufferFromFile & in, LoggerPtr log);
    OpenTelemetry::TracingContextHolderPtr createTracingContextHolder(const char * function, std::shared_ptr<OpenTelemetrySpanLog> open_telemetry_span_log) const;
};

}
