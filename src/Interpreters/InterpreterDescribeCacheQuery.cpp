#include <Interpreters/InterpreterFactory.h>
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

static Block getSampleBlock()
{
    ColumnsWithTypeAndName columns{
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_elements"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "max_file_segment_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt8>(), "is_initialized"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "boundary_alignment"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "cache_on_write_operations"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "cache_hits_threshold"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_elements"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "path"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt64>>(), "background_download_threads"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt64>>(), "background_download_queue_size_limit"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt64>>(), "enable_bypass_cache_with_threshold"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt64>>(), "load_metadata_threads"},
    };
    return Block(columns);
}

BlockIO InterpreterDescribeCacheQuery::execute()
{
    getContext()->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

    const auto & ast = query_ptr->as<ASTDescribeCacheQuery &>();
    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    auto cache_data = FileCacheFactory::instance().getByName(ast.cache_name);
    auto settings = cache_data->getSettings();
    const auto & cache = cache_data->cache;

    size_t i = 0;
    res_columns[i++]->insert(settings.max_size);
    res_columns[i++]->insert(settings.max_elements);
    res_columns[i++]->insert(settings.max_file_segment_size);
    res_columns[i++]->insert(cache->isInitialized());
    res_columns[i++]->insert(settings.boundary_alignment);
    res_columns[i++]->insert(settings.cache_on_write_operations);
    res_columns[i++]->insert(settings.cache_hits_threshold);
    res_columns[i++]->insert(cache->getUsedCacheSize());
    res_columns[i++]->insert(cache->getFileSegmentsNum());
    res_columns[i++]->insert(cache->getBasePath());
    res_columns[i++]->insert(settings.background_download_threads);
    res_columns[i++]->insert(settings.background_download_queue_size_limit);
    res_columns[i++]->insert(settings.enable_bypass_cache_with_threshold);
    res_columns[i++]->insert(settings.load_metadata_threads);

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

void registerInterpreterDescribeCacheQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDescribeCacheQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDescribeCacheQuery", create_fn);
}

}
