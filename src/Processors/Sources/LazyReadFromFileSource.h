#pragma once

#include <Processors/Sources/LazyReadFromFileGroupsSource.h>
#include <Processors/QueryPlan/LazilyReadFromFile.h>

namespace DB
{

/// Dynamically creates the readers of the deferred columns of surviving rows from local files,
/// based on FileLazyMaterializingRows. The readers cannot be created in advance because
/// the set of surviving rows becomes known only at run time, after the main branch of the query
/// (with the `LIMIT`) is fully executed. Until then, prepare() reports UpdatePipeline; the
/// pipeline executor calls updatePipeline() when the downstream LazyMaterializingTransform
/// starts pulling from this processor, which happens strictly after it has filled the shared
/// FileLazyMaterializingRows state. See LazyReadFromFileGroupsSource.
class LazyReadFromFileSource final : public LazyReadFromFileGroupsSource
{
public:
    LazyReadFromFileSource(
        SharedHeader header,
        std::shared_ptr<StorageFile> storage_,
        ReadFromFormatInfo info_,
        ContextPtr context_,
        size_t max_block_size_,
        size_t max_threads_,
        size_t main_read_leaf_columns_,
        std::optional<size_t> lazy_read_leaf_columns_,
        FileLazyMaterializingRowsPtr lazy_materializing_rows_);

    String getName() const override { return "LazyReadFromFileSource"; }
    PipelineUpdate updatePipeline() override;

private:
    std::shared_ptr<StorageFile> storage;
    ReadFromFormatInfo info;
    ContextPtr context;
    size_t max_block_size;
    std::optional<size_t> lazy_read_leaf_columns;

    FileLazyMaterializingRowsPtr lazy_materializing_rows;
};

}
