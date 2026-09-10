#include <Columns/IColumn.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDescribeCacheQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTDescribeCacheQuery.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/System/MutableColumnsAndConstraints.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Access/Common/AccessFlags.h>
#include <Core/Block.h>

namespace DB
{

static Block getSampleBlock()
{
    ColumnsWithTypeAndName columns;
    for (const auto & desc : FileCacheSettings::getColumnsDescription())
    {
        columns.push_back(ColumnWithTypeAndName(desc.type, desc.name));
    }
    return Block(columns);
}

BlockIO InterpreterDescribeCacheQuery::execute()
{
    getContext()->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();
    auto constraints_and_current_profiles = getContext()->getSettingsConstraintsAndCurrentProfiles();
    const auto & constraints = constraints_and_current_profiles->constraints;

    const auto & ast = query_ptr->as<ASTDescribeCacheQuery &>();
    auto cache_data = FileCacheFactory::instance().getByName(ast.cache_name);
    auto settings = cache_data->getSettings();
    MutableColumnsAndConstraints params(res_columns, constraints);
    settings.dumpToSystemSettingsColumns(params, ast.cache_name, cache_data->cache);

    BlockIO res;
    size_t num_rows = res_columns[0]->size();
    auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample_block)), Chunk(std::move(res_columns), num_rows));
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
