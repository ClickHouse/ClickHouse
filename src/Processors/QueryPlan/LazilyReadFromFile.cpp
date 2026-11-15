#include <Processors/QueryPlan/LazilyReadFromFile.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Sources/LazyReadFromFileSource.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

UInt64 LazyFileRegistry::registerFile(const String & path, const String & version_token)
{
    std::lock_guard lock(mutex);
    if (files.size() >= MAX_FILES)
        throw Exception(ErrorCodes::TOO_MANY_ROWS,
            "Too many files ({}) are read by a query with lazy materialization. "
            "Disable the query_plan_optimize_lazy_materialization_for_file setting",
            files.size());
    files.push_back({path, version_token});
    return files.size() - 1;
}

FileLazyMaterializingRows::FileLazyMaterializingRows(LazyFileRegistryPtr file_registry_)
    : file_registry(std::move(file_registry_))
{
}

void FileLazyMaterializingRows::filterRangesAndFillRows(const PaddedPODArray<UInt64> & sorted_indexes)
{
    rows_in_files.clear();

    std::lock_guard lock(file_registry->mutex);

    for (size_t i = 0; i < sorted_indexes.size();)
    {
        UInt64 file_index = sorted_indexes[i] >> LazyFileRegistry::ROW_INDEX_BITS;
        if (file_index >= file_registry->files.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Global row index refers to file {} while only {} files were registered",
                file_index, file_registry->files.size());

        auto rows = std::make_shared<PaddedPODArray<UInt64>>();
        while (i < sorted_indexes.size()
            && (sorted_indexes[i] >> LazyFileRegistry::ROW_INDEX_BITS) == file_index)
        {
            rows->push_back(sorted_indexes[i] & LazyFileRegistry::ROW_INDEX_MASK);
            ++i;
        }

        rows_in_files.push_back({file_registry->files[file_index], std::move(rows)});
    }

    LOG_TRACE(getLogger("FileLazyMaterializingRows"), "Lazily reading {} rows from {} files",
        sorted_indexes.size(), rows_in_files.size());
}

LazilyReadFromFile::LazilyReadFromFile(
    SharedHeader header,
    std::shared_ptr<StorageFile> storage_,
    ReadFromFormatInfo info_,
    ContextPtr context_,
    size_t max_block_size_)
    : ISourceStep(std::move(header))
    , storage(std::move(storage_))
    , info(std::move(info_))
    , context(std::move(context_))
    , max_block_size(max_block_size_)
{
}

void LazilyReadFromFile::setLazyMaterializingRows(FileLazyMaterializingRowsPtr lazy_materializing_rows_)
{
    lazy_materializing_rows = std::move(lazy_materializing_rows_);
}

void LazilyReadFromFile::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!lazy_materializing_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "LazilyReadFromFile: lazy_materializing_rows is not set");

    auto source = std::make_shared<LazyReadFromFileSource>(
        getOutputHeader(),
        storage,
        info,
        context,
        max_block_size,
        lazy_materializing_rows);

    processors.emplace_back(source);
    Pipe pipe(std::move(source));
    pipeline.init(std::move(pipe));
}

void LazilyReadFromFile::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    settings.out << prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column : *getOutputHeader())
    {
        if (!first)
            settings.out << ", ";
        first = false;

        settings.out << column.name;
    }

    settings.out << '\n';
}

void LazilyReadFromFile::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & column : *getOutputHeader())
        json_array->add(column.name);

    map.add("Lazily read columns", std::move(json_array));
}

}
