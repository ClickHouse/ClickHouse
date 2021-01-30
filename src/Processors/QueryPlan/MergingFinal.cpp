#include <Processors/QueryPlan/MergingFinal.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Transforms/AddingSelectorTransform.h>
#include <Processors/Transforms/CopyTransform.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = true,
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

MergingFinal::MergingFinal(
    const DataStream & input_stream,
    size_t num_output_streams_,
    SortDescription sort_description_,
    MergeTreeData::MergingParams params_,
    Names partition_key_columns_,
    size_t max_block_size_)
    : ITransformingStep(input_stream, input_stream.header, getTraits())
    , num_output_streams(num_output_streams_)
    , sort_description(std::move(sort_description_))
    , merging_params(std::move(params_))
    , partition_key_columns(std::move(partition_key_columns_))
    , max_block_size(max_block_size_)
{
    /// TODO: check input_stream is partially sorted (each port) by the same description.
//    output_stream->sort_description = sort_description;
//    output_stream->sort_mode = DataStream::SortMode::Stream;
}

void MergingFinal::transformPipeline(QueryPipeline & pipeline)
{
    const auto & header = pipeline.getHeader();
    size_t num_outputs = pipeline.getNumStreams();

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
            {
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);
            }

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, true, max_block_size);

            case MergeTreeData::MergingParams::Summing:
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.columns_to_sum, partition_key_columns, max_block_size);

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                           sort_description, max_block_size);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                           sort_description, merging_params.version_column, max_block_size);

            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                           sort_description, merging_params.sign_column, max_block_size);

            case MergeTreeData::MergingParams::Graphite:
                throw Exception("GraphiteMergeTree doesn't support FINAL", ErrorCodes::LOGICAL_ERROR);
        }

        __builtin_unreachable();
    };

    if (num_output_streams <= 1 || sort_description.empty())
    {
        pipeline.addTransform(get_merging_processor());
        return;
    }

    ColumnNumbers key_columns;
    key_columns.reserve(sort_description.size());

    for (auto & desc : sort_description)
    {
        if (!desc.column_name.empty())
            key_columns.push_back(header.getPositionByName(desc.column_name));
        else
            key_columns.emplace_back(desc.column_number);
    }

    pipeline.addSimpleTransform([&](const Block & stream_header)
    {
        return std::make_shared<AddingSelectorTransform>(stream_header, num_output_streams, key_columns);
    });

    pipeline.transform([&](OutputPortRawPtrs ports)
    {
        Processors transforms;
        std::vector<OutputPorts::iterator> output_ports;
        transforms.reserve(ports.size() + num_output_streams);
        output_ports.reserve(ports.size());

        for (auto & port : ports)
        {
            auto copier = std::make_shared<CopyTransform>(header, num_output_streams);
            connect(*port, copier->getInputPort());
            output_ports.emplace_back(copier->getOutputs().begin());
            transforms.emplace_back(std::move(copier));
        }

        for (size_t i = 0; i < num_output_streams; ++i)
        {
            auto merge = get_merging_processor();
            merge->setSelectorPosition(i);
            auto input = merge->getInputs().begin();

            /// Connect i-th merge with i-th input port of every copier.
            for (size_t j = 0; j < ports.size(); ++j)
            {
                connect(*output_ports[j], *input);
                ++output_ports[j];
                ++input;
            }

            transforms.emplace_back(std::move(merge));
        }

        return transforms;
    });
}

void MergingFinal::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Sort description: ";
    dumpSortDescription(sort_description, input_streams.front().header, settings.out);
    settings.out << '\n';
}

}
