#include <Processors/QueryPlan/MultiwayMergeJoinStep.h>

#include <Core/Block.h>
#include <Core/Joins.h>
#include <IO/Operators.h>
#include <Interpreters/QueryExecutionCounters.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MultiwayMergeJoinStep::MultiwayMergeJoinStep(
    SharedHeaders input_headers_,
    VectorWithMemoryTracking<Names> key_names_,
    SharedHeader output_header_,
    size_t max_block_size_,
    size_t max_streams_,
    int null_direction_)
    : key_names(std::move(key_names_))
    , result_header(std::move(output_header_))
    , max_block_size(max_block_size_)
    , max_streams(max_streams_)
    , null_direction(null_direction_)
{
    updateInputHeaders(std::move(input_headers_));
}

void MultiwayMergeJoinStep::updateOutputHeader()
{
    if (input_headers.size() != key_names.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiwayMergeJoinStep got {} inputs and {} keys", input_headers.size(), key_names.size());

    output_columns.clear();
    output_columns.reserve(result_header->columns());
    for (const auto & column : *result_header)
    {
        std::optional<MultiwayMergeJoinAlgorithm::OutputColumn> source;
        for (size_t input = 0; input < input_headers.size(); ++input)
        {
            if (!input_headers[input]->has(column.name))
                continue;
            if (source)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is found in several inputs of MultiwayMergeJoinStep", column.name);
            source = MultiwayMergeJoinAlgorithm::OutputColumn{input, input_headers[input]->getPositionByName(column.name)};
        }

        if (!source)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is not found in the inputs of MultiwayMergeJoinStep", column.name);
        output_columns.push_back(*source);
    }

    output_header = result_header;
}

QueryPipelineBuilderPtr MultiwayMergeJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != input_headers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MultiwayMergeJoinStep expects {} inputs, got {}", input_headers.size(), pipelines.size());

    QueryExecutionCounters::addExecutedJoin(JoinKind::Inner, JoinStrictness::All, toString(JoinAlgorithm::FULL_SORTING_MERGE));

    bool use_sharding = !sharding.empty();
    for (const auto & pipeline : pipelines)
        use_sharding = use_sharding && pipeline->getNumStreams() == pipelines.front()->getNumStreams();

    auto create_transform = [&](const SharedHeaders & headers) -> ProcessorPtr
    {
        return std::make_shared<MultiwayMergeJoinTransform>(headers, output_header, key_names, output_columns, max_block_size, null_direction);
    };

    auto joined = QueryPipelineBuilder::joinPipelinesMultiwayMerge(
        std::move(pipelines), key_names, use_sharding, create_transform, max_block_size, this, &processors);

    if (!use_sharding)
        joined->resize(max_streams);

    return joined;
}

void MultiwayMergeJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void MultiwayMergeJoinStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Type: " << (settings.pretty ? "inner" : "INNER") << '\n';
    settings.out << prefix << "Inputs: " << input_headers.size() << '\n';
    settings.out << prefix << "Keys: ";
    for (size_t i = 0; i < key_names.size(); ++i)
    {
        if (i > 0)
            settings.out << ", ";
        if (key_names[i].size() == 1)
            settings.out << key_names[i].front();
        else
            settings.out << fmt::format("({})", fmt::join(key_names[i], ", "));
    }
    settings.out << '\n';

    if (!sharding.empty())
    {
        settings.out << prefix << "Sharding: [";
        bool first = true;
        for (const auto & [lhs, rhs] : sharding)
        {
            if (!first)
                settings.out << ", ";
            first = false;
            settings.out << "(" << lhs << " = " << rhs << ")";
        }
        settings.out << "]\n";
    }
}

void MultiwayMergeJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", "INNER");
    map.add("Inputs", input_headers.size());

    auto keys = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & names : key_names)
    {
        auto item = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & name : names)
            item->add(name);
        keys->add(std::move(item));
    }
    map.add("Keys", std::move(keys));

    if (!sharding.empty())
    {
        auto array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & [lhs, rhs] : sharding)
        {
            auto item = std::make_unique<JSONBuilder::JSONArray>();
            item->add(lhs);
            item->add(rhs);
            array->add(std::move(item));
        }
        map.add("Sharding", std::move(array));
    }
}

QueryPlanStepPtr MultiwayMergeJoinStep::clone() const
{
    return std::make_unique<MultiwayMergeJoinStep>(*this);
}

}
