#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

/// Implements WHERE, HAVING operations. See FilterTransform.
class FilterStep : public ITransformingStep
{
public:
    FilterStep(
        const DataStream & input_stream_,
        ActionsDAGPtr actions_dag_,
        String filter_column_name_,
        bool remove_filter_column_);

    String getName() const override { return "Filter"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }
    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

private:
    ActionsDAGPtr actions_dag;
    String filter_column_name;
    bool remove_filter_column;
};

}
