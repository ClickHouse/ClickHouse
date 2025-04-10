#pragma once
#include "config.h"
#if USE_PARQUET

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/Parquet/ReadManager.h>

namespace DB
{

class ParquetMk4BlockInputFormat : public IInputFormat
{
public:
    ParquetMk4BlockInputFormat(
        ReadBuffer & buf,
        const Block & header,
        const FormatSettings & format_settings,
        Parquet::SharedParsingThreadPoolPtr thread_pool_,
        size_t min_bytes_for_seek);

    ~ParquetMk4BlockInputFormat() override;

    void resetParser() override;

    String getName() const override { return "ParquetMk4BlockInputFormat"; }

    const BlockMissingValues * getMissingValues() const override
    {
        //TODO
        return nullptr;
    }

    size_t getApproxBytesReadForChunk() const override
    {
        //TODO
        return 0;
    }

private:
    Chunk read() override;

    void onCancel() noexcept override
    {
        //TODO
    }

    const FormatSettings format_settings;
    Parquet::ReadOptions read_options;
    Parquet::SharedParsingThreadPoolPtr thread_pool;

    std::optional<Parquet::ReadManager> reader;

    void initializeIfNeeded();
};

class NativeParquetSchemaReader : public ISchemaReader
{
public:
    NativeParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings);

    NamesAndTypesList readSchema() override;
    std::optional<size_t> readNumberOrRows() override;

private:
    void initializeIfNeeded();

    Parquet::ReadOptions read_options;
    Parquet::parq::FileMetaData file_metadata;
    bool initialized = false;
};

}

#endif
