#include <Interpreters/InterpreterDescribeCacheQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Common/FileCacheFactory.h>
#include <Common/FileCache.h>
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
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "cache_on_write_operations"},
        ColumnWithTypeAndName{std::make_shared<DataTypeNumber<UInt8>>(), "enable_cache_hits_threshold"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_size"},
        ColumnWithTypeAndName{std::make_shared<DataTypeUInt64>(), "current_elements"},
        ColumnWithTypeAndName{std::make_shared<DataTypeString>(), "path"}
    };
    return Block(columns);
}

BlockIO InterpreterDescribeCacheQuery::execute()
{
    getContext()->checkAccess(AccessType::SHOW_CACHES);

    const auto & ast = query_ptr->as<ASTDescribeCacheQuery &>();
    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    auto cache_data = FileCacheFactory::instance().getByName(ast.cache_name);
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

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));

    return res;
}

}
