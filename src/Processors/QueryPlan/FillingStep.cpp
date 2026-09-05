#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/FillingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/JSONBuilder.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <algorithm>

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

namespace
{

/// The columns to fill are exactly the `WITH FILL` elements of the `ORDER BY`, in the same order.
SortDescription extractWithFillColumns(const SortDescription & sort_description)
{
    SortDescription fill_description;
    for (const auto & description : sort_description)
        if (description.with_fill)
            fill_description.push_back(description);
    return fill_description;
}

}

FillingStep::FillingStep(
    SharedHeader input_header_,
    SortDescription sort_description_,
    InterpolateDescriptionPtr interpolate_description_,
    bool use_with_fill_by_sorting_prefix_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(FillingTransform::transformHeader(*input_header_, sort_description_)), getTraits())
    , sort_description(std::move(sort_description_))
    , interpolate_description(interpolate_description_)
    , use_with_fill_by_sorting_prefix(use_with_fill_by_sorting_prefix_)
{
}

void FillingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (pipeline.getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FillingStep expects single input");

    const auto fill_description = extractWithFillColumns(sort_description);

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

    /// The fill columns are derived from the sort description, so only the latter travels.
    serializeSortDescription(sort_description, ctx.out, ctx.version);

    UInt8 flags = 0;
    if (use_with_fill_by_sorting_prefix)
        flags |= 1;
    if (interpolate_description)
        flags |= 2;
    writeIntBinary(flags, ctx.out);

    if (!interpolate_description)
        return;

    interpolate_description->actions.serialize(ctx.out, ctx.registry);

    /// The reader rebuilds `result_columns_order` from the DAG's outputs, which is exactly how
    /// `InterpolateDescription` built it - unless the query's aliases renamed an output. Only the analyzer
    /// path is ever serialized and it builds the description with no aliases, so the two agree; refuse to
    /// ship a renamed description rather than let the reader rebuild a different one.
    const auto result_columns = interpolate_description->actions.getResultColumns();
    const auto & result_columns_order = interpolate_description->result_columns_order;
    bool derivable_from_actions = result_columns.size() == result_columns_order.size();
    for (size_t i = 0; derivable_from_actions && i < result_columns.size(); ++i)
        derivable_from_actions = result_columns[i].name == result_columns_order[i];

    if (!derivable_from_actions)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serialization of an INTERPOLATE description whose result columns do not match its expression "
            "outputs (for example renamed by query aliases) is not supported");

    /// Only what the reader cannot derive from the DAG travels: the `required_columns_map` keys, which are
    /// the query's aliases (not part of the plan) that `FillingTransform` matches against header columns.
    /// Everything else - each entry's type, and `result_columns_order`/`result_columns_set` - is rebuilt
    /// from `actions` on the other side, so a stream cannot describe a fill that disagrees with its own DAG.
    /// The pairs are sorted because the map has no stable order and these bytes are also a plan cache key.
    /// Keys are unique, so ordering the pairs orders them by key.
    std::vector<std::pair<std::string_view, std::string_view>> required_columns;
    required_columns.reserve(interpolate_description->required_columns_map.size());
    for (const auto & [key, name_and_type] : interpolate_description->required_columns_map)
        required_columns.emplace_back(key, name_and_type.name);
    std::sort(required_columns.begin(), required_columns.end());

    writeVarUInt(required_columns.size(), ctx.out);
    for (const auto & [key, name] : required_columns)
    {
        writeStringBinary(key, ctx.out);
        writeStringBinary(name, ctx.out);
    }
}

QueryPlanStepPtr FillingStep::deserialize(Deserialization & ctx)
{
    /// The registry dispatches by step name, so a stream that predates this step can still name it.
    /// Fail closed at the version boundary instead of building a step the stream cannot describe.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "FillingStep is not part of query plan serialization version {}; the first version that "
            "carries it is {}", ctx.version, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FILLING_STEP);

    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "FillingStep must have one input stream");

    SortDescription sort_description;
    deserializeSortDescription(sort_description, ctx.in, ctx.version, ctx.max_type_complexity);

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

        /// The plan may come from an untrusted client (`TCPHandler::receiveQueryPlan`), so the description
        /// is rebuilt from the DAG rather than taken from the wire. `FillingTransform` pairs the executed
        /// DAG's output columns with `result_columns_order` by position and stages each required column in
        /// a column of the type stored here, so a description that disagrees with its DAG would index past
        /// `interpolate_column_positions` or insert into a column of another type.
        const auto & input_header = *ctx.input_headers.front();
        const auto dag_required_columns = actions.getRequiredColumns();

        UnorderedMapWithMemoryTracking<std::string, NameAndTypePair> required_columns_map;
        size_t num_required_columns = 0;
        readVarUInt(num_required_columns, ctx.in);
        for (size_t i = 0; i < num_required_columns; ++i)
        {
            String key;
            String name;
            readStringBinary(key, ctx.in);
            readStringBinary(name, ctx.in);

            /// The key is the alias under which the transform looks the column up in its input header.
            if (!input_header.has(key))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FillingStep: INTERPOLATE source column '{}' is not present in the input header", key);

            auto name_and_type = dag_required_columns.tryGetByName(name);
            if (!name_and_type)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FillingStep: INTERPOLATE source column '{}' is not required by the interpolate expression", name);

            /// The transform stages the header column in a column of the expression's input type and
            /// `insertFrom`s one into the other, which is only defined when the two types are the same.
            /// The planner builds the expression's inputs from this very header, so they always are.
            const auto & header_type = input_header.getByName(key).type;
            if (!header_type->equals(*name_and_type->type))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FillingStep: INTERPOLATE source column '{}' has type {} in the input header, but the "
                    "interpolate expression expects {}", key, header_type->getName(), name_and_type->type->getName());

            /// The type comes from the DAG, never from the wire: it decides the column the transform
            /// stages the value in before executing the expression.
            required_columns_map[key] = *name_and_type;
        }

        if (required_columns_map.size() != dag_required_columns.size())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "FillingStep: INTERPOLATE expects {} source columns, got {}",
                dag_required_columns.size(), required_columns_map.size());

        /// One entry per DAG output, in output order - the pairing `FillingTransform` relies on.
        VectorWithMemoryTracking<std::string> result_columns_order;
        for (const auto & column : actions.getResultColumns())
        {
            if (!input_header.has(column.name))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FillingStep: INTERPOLATE column '{}' is not present in the input header", column.name);

            /// Each output is `insertFrom`-ed into the header column of the same name, so their types have
            /// to agree; the planner casts the interpolate expression to that column's type for this reason.
            const auto & header_type = input_header.getByName(column.name).type;
            if (!header_type->equals(*column.type))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "FillingStep: INTERPOLATE column '{}' has type {} in the input header, but the "
                    "interpolate expression produces {}", column.name, header_type->getName(), column.type->getName());

            result_columns_order.push_back(column.name);
        }

        interpolate_description = std::make_shared<InterpolateDescription>(
            std::move(actions), std::move(required_columns_map), std::move(result_columns_order));
    }

    return std::make_unique<FillingStep>(
        ctx.input_headers.front(),
        std::move(sort_description),
        std::move(interpolate_description),
        (flags & 1) != 0);
}

void registerFillingStep(QueryPlanStepRegistry & registry);
void registerFillingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Filling", FillingStep::deserialize);
}
}
