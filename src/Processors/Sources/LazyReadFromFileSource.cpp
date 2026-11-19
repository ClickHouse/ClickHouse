#include <Processors/Sources/LazyReadFromFileSource.h>
#include <Formats/FormatParserSharedResources.h>
#include <Interpreters/Context.h>
#include <Storages/StorageFile.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

LazyReadFromFileSource::LazyReadFromFileSource(
    SharedHeader header,
    std::shared_ptr<StorageFile> storage_,
    ReadFromFormatInfo info_,
    ContextPtr context_,
    size_t max_block_size_,
    size_t max_threads_,
    size_t main_read_leaf_columns_,
    std::optional<size_t> lazy_read_leaf_columns_,
    FileLazyMaterializingRowsPtr lazy_materializing_rows_)
    : LazyReadFromFileGroupsSource(std::move(header), max_threads_, main_read_leaf_columns_)
    , storage(std::move(storage_))
    , info(std::move(info_))
    , context(std::move(context_))
    , max_block_size(max_block_size_)
    , lazy_read_leaf_columns(lazy_read_leaf_columns_)
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
}

IProcessor::PipelineUpdate LazyReadFromFileSource::updatePipeline()
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazyReadFromFileSource: no lazy materializing rows");

    auto rows = std::move(lazy_materializing_rows);
    lazy_materializing_rows.reset();

    auto & files = rows->rows_in_files;
    if (files.empty())
        return addReaders({});

    size_t main_read_streams = 0;
    {
        std::lock_guard lock(rows->file_registry->mutex);
        main_read_streams = std::min(rows->file_registry->main_read_streams, rows->file_registry->files.size());
    }
    const size_t num_readers = getNumReaders(main_read_streams, lazy_read_leaf_columns, files.size());
    auto parser_shared_resources = std::make_shared<FormatParserSharedResources>(context->getSettingsRef(), num_readers);

    Processors readers;
    for (auto & group_files : splitIntoGroups(std::move(files), num_readers))
        readers.push_back(StorageFile::createLazyRowsSource(storage, info, context, max_block_size, std::move(group_files), parser_shared_resources));

    return addReaders(std::move(readers));
}

}
