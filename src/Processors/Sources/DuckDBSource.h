#pragma once

#include "config.h"

#if USE_DUCKDB
#include <Core/ExternalResultDescription.h>
#include <Processors/ISource.h>

#include <duckdb.hpp>


namespace DB
{

class DuckDBSource : public ISource
{

using DuckDBPtr = std::shared_ptr<duckdb::DuckDB>;
using ResultPtr = std::unique_ptr<duckdb::QueryResult>;
using ChunkPtr = std::unique_ptr<duckdb::DataChunk>;

public:
    DuckDBSource(DuckDBPtr duckdb_instance_, const String & query_str_, const Block & sample_block, UInt64 max_block_size_);

    String getName() const override { return "DuckDB"; }

private:

    using ValueType = ExternalResultDescription::ValueType;

    Chunk generate() override;

    String query_str;
    UInt64 max_block_size;

    ExternalResultDescription description;
    DuckDBPtr duckdb_instance;
    ResultPtr result;
    ChunkPtr fetched_chunk;
    size_t fetched_chunk_pos = 0;
    bool finished_fetch = false;
};

}

#endif
