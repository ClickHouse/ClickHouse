#pragma once
#include <memory>
#include <Processors/ISimpleTransform.h>
#include <Interpreters/BloomFilter.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;
class QueryConditionCache;

/// Implements building a Bloom Filter from all values of the specified column. When builidng is finished the filter is saved into
/// per-query filter map under the specified name. This allows to find the filter by name and use it in Expressions with the help of
/// a special function 'filterContains'
class BuildRuntimeFilterTransform : public ISimpleTransform
{
public:
    BuildRuntimeFilterTransform(SharedHeader header_, String filter_column_name_, String filter_name_)
        : ISimpleTransform(header_, header_, true)
        , filter_column_name(filter_column_name_)
        , filter_name(filter_name_)
        , filter_column_position(header_->getPositionByName(filter_column_name))
        , built_filter(std::make_shared<BloomFilter>(10*1024, 4, 42))
    {}

    String getName() const override { return "BuildRuntimeFilterTransform"; }

    Status prepare() override;

    void transform(Chunk & chunk) override;

private:
    String filter_column_name;
    String filter_name;
    size_t filter_column_position = -1;

    std::shared_ptr<BloomFilter> built_filter;

    void doTransform(Chunk & chunk);

    void finish();
};

}
