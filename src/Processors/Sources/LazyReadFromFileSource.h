#pragma once

#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/LazilyReadFromFile.h>

namespace DB
{

/// Dynamically creates a reader of the deferred columns of surviving rows from local files,
/// based on FileLazyMaterializingRows. The reader cannot be created in advance because
/// the set of surviving rows becomes known only at run time, after the main branch of the query
/// (with the `LIMIT`) is fully executed. Until then, prepare() reports UpdatePipeline; the
/// pipeline executor calls updatePipeline() when the downstream LazyMaterializingTransform
/// starts pulling from this processor, which happens strictly after it has filled the shared
/// FileLazyMaterializingRows state.
class LazyReadFromFileSource final : public IProcessor
{
public:
    LazyReadFromFileSource(
        SharedHeader header,
        std::shared_ptr<StorageFile> storage_,
        ReadFromFormatInfo info_,
        ContextPtr context_,
        size_t max_block_size_,
        FileLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyReadFromFileSource"; }
    Status prepare() override;
    PipelineUpdate updatePipeline() override;

private:
    std::shared_ptr<StorageFile> storage;
    ReadFromFormatInfo info;
    ContextPtr context;
    size_t max_block_size;

    FileLazyMaterializingRowsPtr lazy_materializing_rows;
};

}
