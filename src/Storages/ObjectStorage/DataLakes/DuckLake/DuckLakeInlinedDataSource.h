#pragma once

#include "config.h"

#if USE_PARQUET

#include <Processors/ISource.h>

#include <vector>

namespace DB
{

/// Emits pre-materialized chunks of DuckLake inlined data rows (rows that DuckLake stores
/// in the catalog database instead of data files). Inlined row counts are small by design
/// (bounded by the DuckLake data_inlining_row_limit), so materializing them eagerly at
/// pipeline build time is fine.
class DuckLakeInlinedDataSource final : public ISource
{
public:
    DuckLakeInlinedDataSource(SharedHeader header_, std::vector<Chunk> chunks_)
        : ISource(std::move(header_))
        , chunks(std::move(chunks_))
    {
    }

    String getName() const override { return "DuckLakeInlinedData"; }

protected:
    Chunk generate() override
    {
        if (index >= chunks.size())
            return {};
        return std::move(chunks[index++]);
    }

private:
    std::vector<Chunk> chunks;
    size_t index = 0;
};

}

#endif
