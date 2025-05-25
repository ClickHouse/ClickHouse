#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

class MergeTreeLazilyReader;
using MergeTreeLazilyReaderPtr = std::unique_ptr<MergeTreeLazilyReader>;

class LazilyReadStep : public ITransformingStep
{
public:
    LazilyReadStep(const Header & input_header_, const LazilyReadInfoPtr & lazily_read_info_, MergeTreeLazilyReaderPtr lazy_column_reader_);

    String getName() const override { return "LazilyRead"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    LazilyReadInfoPtr lazily_read_info;
    MergeTreeLazilyReaderPtr lazy_column_reader;
};

}
