#pragma once
#include <unordered_map>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Common/LRUCache.h>


namespace DB
{

class CachingStep : public ITransformingStep
{
public:
    CachingStep(const DataStream & input_stream_, LRUCache<CacheKey, Data, CacheKeyHasher> & cache_, ASTPtr query_ptr_);

    String getName() const override { return "Caching"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    LRUCache<CacheKey, Data, CacheKeyHasher> & cache;
    ASTPtr query_ptr;
};

}
