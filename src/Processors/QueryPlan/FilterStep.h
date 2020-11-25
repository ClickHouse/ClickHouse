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
        String filter_column_name_,
        bool remove_filter_column_);

    String getName() const override { return "Filter"; }
    void transformPipeline(QueryPipeline & pipeline) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const String & getFilterColumnName() const { return filter_column_name; }
    bool removesFilterColumn() const { return remove_filter_column; }

private:
    String filter_column_name;
    bool remove_filter_column;
};

}
