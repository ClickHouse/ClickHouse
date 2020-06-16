#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class FilterStep : public ITransformingStep
{
public:
    FilterStep(
        const DataStream & input_stream_,
        ExpressionActionsPtr expression_,
        String filter_column_name_,
        bool remove_filter_column_);

    String getName() const override { return "Filter"; }
    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ExpressionActionsPtr expression;
    String filter_column_name;
    bool remove_filter_column;
};

}
