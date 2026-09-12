#pragma once
#include <Core/QueryProcessingStage.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

    class ReadFromLoopStep final : public SourceStepWithFilter
    {
    public:
        ReadFromLoopStep(
                const Names & column_names_,
                const SelectQueryInfo & query_info_,
                const StorageSnapshotPtr & storage_snapshot_,
                const ContextPtr & context_,
                const StorageID & inner_table_id_,
                ASTPtr inner_table_function_ast_);

        String getName() const override { return "ReadFromLoop"; }

        void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    private:

        Pipe makePipe();

        const Names column_names;
        StorageID inner_table_id;
        ASTPtr inner_table_function_ast;
    };
}
