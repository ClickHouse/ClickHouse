#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoin.h>

#include <optional>

namespace DB
{

class ArrayJoinAction;
using ArrayJoinActionPtr = std::shared_ptr<ArrayJoinAction>;

class ArrayJoinStep : public ITransformingStep
{
public:
    ArrayJoinStep(const SharedHeader & input_header_, ArrayJoin array_join_, bool is_unaligned_, size_t max_block_size_, bool enable_lazy_columns_replication_);

    ArrayJoinStep(const ArrayJoinStep & other)
        : ITransformingStep(other)
        , array_join(other.array_join)
        , is_unaligned(other.is_unaligned)
        , max_block_size(other.max_block_size)
        , enable_lazy_columns_replication(other.enable_lazy_columns_replication)
        , element_filter(other.element_filter ? std::optional<ActionsDAG>(other.element_filter->clone()) : std::nullopt)
        , element_filter_column_name(other.element_filter_column_name)
        , remove_element_filter_column(other.remove_element_filter_column)
    {}

    String getName() const override { return "ArrayJoin"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    const Names & getColumns() const { return array_join.columns; }
    bool isLeft() const { return array_join.is_left; }

    /// Attach an element-space filter (the fuse-filter pass sets this); the DAG references only joined columns
    void setElementFilter(ActionsDAG filter_dag, String filter_column_name, bool remove_filter_column);
    bool hasElementFilter() const { return element_filter.has_value(); }

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    ArrayJoin array_join;
    bool is_unaligned = false;
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
    bool enable_lazy_columns_replication = false;

    std::optional<ActionsDAG> element_filter;
    String element_filter_column_name;
    bool remove_element_filter_column = false;
};

}
