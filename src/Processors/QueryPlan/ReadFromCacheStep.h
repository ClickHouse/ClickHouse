#pragma once
#include <unordered_map>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Transforms/ReadFromCacheTransform.h>


namespace DB
{

class ReadFromCacheStep : public ITransformingStep
{
public:
    ReadFromCacheStep(const DataStream & input_stream_, std::unordered_map<IAST::Hash, Data> & cache, ASTPtr query_ptr_);
    String getName() const override { return "Caching"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::unordered_map<IAST::Hash, Data> & cache;
    ASTPtr query_ptr;
};

}
