#include <Processors/QueryPlan/LazilyReadStep.h>

#include <Common/JSONBuilder.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

LazilyReadStep::LazilyReadStep(
    const DataStream & input_stream_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    const LazilyReadInfoPtr & lazily_read_info_,
    const ContextPtr & context_,
    const AliasToNamePtr & alias_index_)
    : ITransformingStep(
        input_stream_,
        MergeTreeTransform::transformHeader(input_stream_.header),
        getTraits())
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , lazily_read_info(lazily_read_info_)
    , context(context_)
    , alias_index(alias_index_)
{
}

void LazilyReadStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<MergeTreeTransform>(header, storage, storage_snapshot, lazily_read_info, context, alias_index);
    });
}

void LazilyReadStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    settings.out << prefix << "Lazily read columns: ";

    bool first = true;
    for (const auto & column_name : lazily_read_info->lazily_read_columns_names)
    {
        if (!first)
            settings.out << ", ";
        first = false;

        settings.out << column_name;
    }

    settings.out << '\n';
}

void LazilyReadStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & column_name : lazily_read_info->lazily_read_columns_names)
        json_array->add(column_name);

    map.add("Lazily read columns", std::move(json_array));
}

void LazilyReadStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(),
        MergeTreeTransform::transformHeader(input_streams.front().header),
        getDataStreamTraits());
}

}
