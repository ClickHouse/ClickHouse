#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/JSONBuilder.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FillingStep::FillingStep(
    SharedHeader input_header_,
    SortDescription sort_description_,
    SortDescription fill_description_,
    InterpolateDescriptionPtr interpolate_description_,
    bool use_with_fill_by_sorting_prefix_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(FillingTransform::transformHeader(*input_header_, sort_description_)), getTraits())
    , sort_description(std::move(sort_description_))
    , fill_description(std::move(fill_description_))
    , interpolate_description(interpolate_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (pipeline.getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
    {
        if (stream_type == QueryPipelineBuilder::StreamType::Totals)
            return std::make_shared<FillingNoopTransform>(header, fill_description);

        return std::make_shared<FillingTransform>(
            header, sort_description, fill_description, std::move(interpolate_description),
            use_with_fill_by_sorting_prefix, settings.process_list_element);
    });
}

void FillingStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix;
    dumpSortDescription(sort_description, settings);
    settings.out << '\n';
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        if (!settings.compact)
            expression->describeActions(settings.out, prefix);
    }
}

void FillingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Sort Description", explainSortDescription(sort_description));
    if (interpolate_description)
    {
        auto expression = std::make_shared<ExpressionActions>(interpolate_description->actions.clone());
        map.add("Expression", expression->toTree());
    }
}

void FillingStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(FillingTransform::transformHeader(*input_headers.front(), sort_description));
}

QueryPlanStepPtr FillingStep::clone() const
{
    /// `interpolate_description` is shared: `transformPipeline` moves it out of the instance it runs on, so
    /// the clone and the original cannot both build a transform, and they never do - only one of the two
    /// ends up in the executed plan.
    return std::make_unique<FillingStep>(*this);
}

void FillingStep::serialize(Serialization & ctx) const
{
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serialization of FillingStep requires query plan serialization version >= {}; "
            "all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP);

    serializeSortDescription(sort_description, ctx.out, ctx.version);
    serializeSortDescription(fill_description, ctx.out, ctx.version);

    UInt8 flags = 0;
    if (use_with_fill_by_sorting_prefix)
        flags |= 1;
    if (interpolate_description)
        flags |= 2;
    writeIntBinary(flags, ctx.out);

    if (!interpolate_description)
        return;

    interpolate_description->actions.serialize(ctx.out, ctx.registry);

    /// `required_columns_map` keys come from the query's aliases, which are not part of the plan, so the
    /// map travels as data. Sorted, because the map itself has no stable order and the serialized bytes
    /// are also used as a plan cache key. `result_columns_set` is the membership view of
    /// `result_columns_order` and is rebuilt from it.
    std::vector<std::string_view> required_column_keys;
    required_column_keys.reserve(interpolate_description->required_columns_map.size());
    for (const auto & [key, _] : interpolate_description->required_columns_map)
        required_column_keys.push_back(key);
    std::sort(required_column_keys.begin(), required_column_keys.end());

    writeVarUInt(required_column_keys.size(), ctx.out);
    for (const auto & key : required_column_keys)
    {
        const auto & name_and_type = interpolate_description->required_columns_map.at(std::string(key));
        writeStringBinary(key, ctx.out);
        writeStringBinary(name_and_type.name, ctx.out);
        encodeDataType(name_and_type.type, ctx.out);
    }

    writeVarUInt(interpolate_description->result_columns_order.size(), ctx.out);
    for (const auto & name : interpolate_description->result_columns_order)
        writeStringBinary(name, ctx.out);
}

QueryPlanStepPtr FillingStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FillingStep must have one input stream");

    SortDescription sort_description;
    deserializeSortDescription(sort_description, ctx.in, ctx.version, ctx.max_type_complexity);

    SortDescription fill_description;
    deserializeSortDescription(fill_description, ctx.in, ctx.version, ctx.max_type_complexity);

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);
    /// Fail closed on a flag bit this version does not know, rather than desynchronizing the stream.
    if (flags & ~UInt8(0x03))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "FillingStep: unsupported flags {0:#04x}", UInt64(flags));

    InterpolateDescriptionPtr interpolate_description;
    if (flags & 2)
    {
        auto actions = ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context, ctx.max_type_complexity);

        UnorderedMapWithMemoryTracking<std::string, NameAndTypePair> required_columns_map;
        size_t num_required_columns = 0;
        readVarUInt(num_required_columns, ctx.in);
        for (size_t i = 0; i < num_required_columns; ++i)
        {
            String key;
            String name;
            readStringBinary(key, ctx.in);
            readStringBinary(name, ctx.in);
            auto type = decodeDataType(ctx.in, ctx.max_type_complexity);
            required_columns_map[key] = NameAndTypePair(name, type);
        }

        VectorWithMemoryTracking<std::string> result_columns_order;
        size_t num_result_columns = 0;
        readVarUInt(num_result_columns, ctx.in);
        result_columns_order.reserve(num_result_columns);
        for (size_t i = 0; i < num_result_columns; ++i)
        {
            String name;
            readStringBinary(name, ctx.in);
            result_columns_order.push_back(name);
        }

        interpolate_description = std::make_shared<InterpolateDescription>(
            std::move(actions), std::move(required_columns_map), std::move(result_columns_order));
    }

    return std::make_unique<FillingStep>(
        ctx.input_headers.front(),
        std::move(sort_description),
        std::move(fill_description),
        std::move(interpolate_description),
        (flags & 1) != 0);
}

void registerFillingStep(QueryPlanStepRegistry & registry);
void registerFillingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Filling", FillingStep::deserialize);
}
}
