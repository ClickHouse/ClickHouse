#pragma once
#include <unordered_map>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>


namespace DB
{

class CachingStep : public ITransformingStep
{
public:
    CachingStep(const DataStream & input_stream_, std::unordered_map<IAST::Hash, Data, ASTHash> & cache, ASTPtr query_ptr_);

    String getName() const override { return "Caching"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    std::unordered_map<IAST::Hash, Data, ASTHash> & cache;
    ASTPtr query_ptr;
};

}
