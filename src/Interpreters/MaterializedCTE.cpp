#include <Interpreters/MaterializedCTE.h>

#include <Common/thread_local_rng.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/StorageMemory.h>

namespace DB
{

MaterializedCTE::MaterializedCTE(const std::string & cte_name_)
    : cte_name(cte_name_)
    , temporary_table_name(fmt::format("_materialized_cte_{}_{}", cte_name, thread_local_rng()))
{}

MaterializedCTE::~MaterializedCTE() noexcept = default;

void MaterializedCTE::initializeStorage(const NamesAndTypesList & columns, const ContextPtr & context)
{
    chassert(!isStorageInitialized());
    table_holder.emplace(
        context->getQueryContext(), ColumnsDescription(columns, false), ConstraintsDescription{},
        nullptr /*query*/, true /*create_for_global_subquery*/);
    storage = table_holder->getTable();
    typeid_cast<StorageMemory &>(*storage).setMaterializedCTE(shared_from_this());
}

void MaterializedCTE::registerInQueryContext(const ContextPtr & context)
{
    if (table_holder)
        context->getQueryContext()->addExternalTable(temporary_table_name, extractTableHolder());
}

std::shared_ptr<MaterializedCTE> MaterializedCTE::materialize(
    const std::string & cte_name, Pipe source, const ContextPtr & context)
{
    auto cte = std::make_shared<MaterializedCTE>(cte_name);
    cte->initializeStorage(source.getHeader().getNamesAndTypesList(), context);
    cte->is_materialization_planned = true;

    QueryPipelineBuilder builder;
    builder.init(std::move(source));
    builder.addMaterializingCTETransform(std::make_shared<const Block>(), cte);
    builder.setSinks([](const SharedHeader & header, Pipe::StreamType) { return std::make_shared<EmptySink>(header); });
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(builder));
    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    cte->registerInQueryContext(context);
    return cte;
}

}
