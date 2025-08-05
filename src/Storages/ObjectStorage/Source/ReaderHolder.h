
#pragma once
#include <memory>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/Cache/SchemaCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <boost/noncopyable.hpp>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/ObjectInfo.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/CompressionMethod.h>

// src/Formats/ReadSchemaUtils.cpp


namespace DB {

struct ReaderHolder : private boost::noncopyable
{
public:
    ReaderHolder(
        ObjectInfoPtr object_info_,
        std::unique_ptr<ReadBuffer> read_buf_,
        std::shared_ptr<ISource> source_,
        std::unique_ptr<QueryPipeline> pipeline_,
        std::unique_ptr<PullingPipelineExecutor> reader_);

    ReaderHolder() = default;
    ReaderHolder(ReaderHolder && other) noexcept { *this = std::move(other); }
    ReaderHolder & operator=(ReaderHolder && other) noexcept;

    explicit operator bool() const { return reader != nullptr; }
    PullingPipelineExecutor * operator->() { return reader.get(); }
    const PullingPipelineExecutor * operator->() const { return reader.get(); }

    ObjectInfoPtr getObjectInfo() const { return object_info; }
    const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

private:
    ObjectInfoPtr object_info;
    std::unique_ptr<ReadBufferFromFileBase> read_buf;
    std::shared_ptr<ISource> source;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
};


ReaderHolder createReader(
    size_t processor,
    const std::shared_ptr<IObjectIterator> & file_iterator,
    const StorageObjectStorageConfigurationPtr & configuration,
    const ObjectStoragePtr & object_storage,
    ReadFromFormatInfo & read_from_format_info,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context_,
    SchemaCache * schema_cache,
    const LoggerPtr & log,
    size_t max_block_size,
    FormatParserSharedResourcesPtr parser_shared_resources,
    FormatFilterInfoPtr format_filter_info,
    bool need_only_count);

}
