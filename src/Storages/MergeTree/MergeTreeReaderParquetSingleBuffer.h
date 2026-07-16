#pragma once
#include <memory>
#include <IO/SeekableReadBuffer.h>
#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <Processors/Formats/Impl/Parquet/Decoding.h>

#include <Storages/MergeTree/MergeTreeReaderParquet.h>

#if USE_PARQUET

#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>

namespace DB
{

class MergeTreeReaderParquetSingleBuffer : public MergeTreeReaderParquet
{
public:
    template <typename... Args>
    explicit MergeTreeReaderParquetSingleBuffer(Args &&... args)
        : MergeTreeReaderCompact{std::forward<Args>(args)...}
    {
    }

    std::unique_ptr<SeekableReadBuffer> file_buf;
};

}

#endif
