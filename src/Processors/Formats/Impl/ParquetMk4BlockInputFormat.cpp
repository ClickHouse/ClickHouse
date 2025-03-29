#include "ParquetMk4BlockInputFormat.h"

#include <Common/ThreadPool.h>

#if USE_PARQUET

namespace ProfileEvents
{
}

namespace CurrentMetrics
{
    extern const Metric ParquetDecoderThreads;
    extern const Metric ParquetDecoderThreadsActive;
    extern const Metric ParquetDecoderThreadsScheduled;

    extern const Metric ParquetDecoderIOThreads;
    extern const Metric ParquetDecoderIOThreadsActive;
    extern const Metric ParquetDecoderIOThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
}

ParquetMk4BlockInputFormat::ParquetMk4BlockInputFormat(
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings & format_settings_,
    Parquet::SharedParsingThreadPoolPtr thread_pool_,
    size_t min_bytes_for_seek)
    : IInputFormat(header_, &buf)
    , format_settings(format_settings_)
    , thread_pool(thread_pool_)
{
    read_options.min_bytes_for_seek = min_bytes_for_seek;
    //TODO: fill out the rest of read_options
}

void ParquetMk4BlockInputFormat::initializeIfNeeded()
{
    if (!reader)
        reader.emplace(in, read_options, thread_pool);
}

ParquetMk4BlockInputFormat::~ParquetMk4BlockInputFormat()
{
    //TODO
}

Chunk ParquetMk4BlockInputFormat::read()
{
    initializeIfNeeded();
    return {};
}

void ParquetMk4BlockInputFormat::resetParser()
{
    //TODO
    IInputFormat::resetParser();
}

}

#endif
