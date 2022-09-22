#include <Interpreters/InterpreterDescribeCacheQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Access/Common/AccessFlags.h>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static Block getSampleBlock()
{
    ColumnsWithTypeAndName columns{
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_elements"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_file_segment_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "cache_on_write_operations"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "enable_cache_hits_threshold"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_elements"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "path"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "do_not_evict_index_and_mark_files"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "background_download_max_memory_usage"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "background_download_current_memory_usage"}
    };
    return Block(columns);
}

BlockIO InterpreterDescribeCacheQuery::execute()
{
    getContext()->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

    const auto & ast = query_ptr->as<ASTDescribeCacheQuery &>();
    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    FileCacheFactory::FileCacheData cache_data;
    bool found = FileCacheFactory::instance().tryGetByName(cache_data, ast.cache_name);
    if (!found)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find cache identified by {}", ast.cache_name);
    const auto & settings = cache_data.settings;
    const auto & cache = cache_data.cache;

    res_columns[0]->insert(settings.max_size);
    res_columns[1]->insert(settings.max_elements);
    res_columns[2]->insert(settings.max_file_segment_size);
    res_columns[3]->insert(settings.cache_on_write_operations);
    res_columns[4]->insert(settings.enable_cache_hits_threshold);
    res_columns[5]->insert(cache->getUsedCacheSize());
    res_columns[6]->insert(cache->getFileSegmentsNum());
    res_columns[7]->insert(cache->getBasePath());
    res_columns[8]->insert(settings.do_not_evict_index_and_mark_files);
    res_columns[9]->insert(settings.background_download_max_memory_usage);
    res_columns[10]->insert(cache->getCurrentMemoryUsageOfBackgroundDownload());

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

}
