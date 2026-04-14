#pragma once

#include <boost/noncopyable.hpp>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DB
{

/// Holds all resources needed to read from a single object storage file:
/// the read buffer, the input format source, the pipeline, and the executor.
/// Move-only. Destruction order matters: reader -> pipeline -> read_buf.
class ObjectStorageReaderHolder : private boost::noncopyable
{
public:
    ObjectStorageReaderHolder(
        ObjectInfoPtr object_info_,
        std::unique_ptr<ReadBuffer> read_buf_,
        std::shared_ptr<ISource> source_,
        std::unique_ptr<QueryPipeline> pipeline_,
        std::unique_ptr<PullingPipelineExecutor> reader_);

    ObjectStorageReaderHolder() = default;
    ObjectStorageReaderHolder(ObjectStorageReaderHolder && other) noexcept { *this = std::move(other); }
    ObjectStorageReaderHolder & operator=(ObjectStorageReaderHolder && other) noexcept;

    explicit operator bool() const { return reader != nullptr; }
    PullingPipelineExecutor * operator->() { return reader.get(); }
    const PullingPipelineExecutor * operator->() const { return reader.get(); }

    ObjectInfoPtr getObjectInfo() const { return object_info; }
    const IInputFormat * getInputFormat() const { return dynamic_cast<const IInputFormat *>(source.get()); }

private:
    ObjectInfoPtr object_info;
    std::unique_ptr<ReadBuffer> read_buf;
    std::shared_ptr<ISource> source;
    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
};

}
