#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event RuntimeFilterStatesSent;
extern const Event RuntimeFilterStateBytesSent;
extern const Event RuntimeFilterStatesReceived;
extern const Event RuntimeFilterStateBytesReceived;
extern const Event RuntimeFilterOversizedStatesRejected;
}

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char distributed_plan_runtime_filter_merge_pause_before_finalize[];
extern const char distributed_plan_runtime_filter_merge_fails_before_finalize[];
}

SharedHeader runtimeFilterPartialsHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "partial")});
}

Chunk makeRuntimeFilterPartialsChunk(const String & state, size_t num_destinations)
{
    ProfileEvents::increment(ProfileEvents::RuntimeFilterStatesSent, num_destinations);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterStateBytesSent, state.size() * num_destinations);
    auto column = ColumnString::create();
    column->insertData(state.data(), state.size());
    Columns columns;
    columns.emplace_back(std::move(column));
    return Chunk(std::move(columns), 1);
}

void registerCompleteRuntimeFilter(
    IRuntimeFilterLookup & lookup,
    const String & filter_key,
    const String & filter_name,
    const RuntimeFilterGeometry & geometry,
    AdaptiveSetRuntimeFilter && filter,
    bool enable_index_analysis)
{
    const RuntimeFilterConfig config{geometry.pass_ratio_threshold_for_disabling, geometry.blocks_to_skip_before_reenabling};
    auto complete_filter = std::make_unique<RuntimeFilter>(/*filters_to_merge_=*/0, config, std::move(filter));
    if (enable_index_analysis)
        complete_filter->enableIndexAnalysis();
    lookup.add(filter_key, filter_name, std::move(complete_filter));
}

MergeRuntimeFiltersTransform::MergeRuntimeFiltersTransform(
    SharedHeader partials_header,
    size_t num_inputs,
    Mode mode_,
    String filter_name_,
    String filter_key_,
    const DataTypePtr & filter_column_target_type_,
    const RuntimeFilterGeometry & geometry_,
    RuntimeFilterLookupPtr filter_lookup_,
    size_t num_forward_destinations_,
    UInt64 max_received_state_bytes_)
    : IProcessor(InputPorts(num_inputs, partials_header), {partials_header})
    , mode(mode_)
    , filter_name(std::move(filter_name_))
    , filter_key(std::move(filter_key_))
    , filter_column_target_type(filter_column_target_type_)
    , geometry(geometry_)
    , filter_lookup(std::move(filter_lookup_))
    , num_forward_destinations(num_forward_destinations_)
    , max_received_state_bytes(max_received_state_bytes_)
    , received(num_inputs, false)
{
    chassert(num_inputs > 0);
    if (mode == Mode::RegisterUnion && !filter_lookup)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeRuntimeFiltersTransform in register mode requires a filter lookup");
}

IProcessor::Status MergeRuntimeFiltersTransform::prepare()
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (has_output_chunk)
    {
        if (!output.canPush())
            return Status::PortFull;
        output.push(std::move(output_chunk));
        has_output_chunk = false;
        output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// An oversized state was rejected: stop pulling, publish nothing, fail open.
    if (skipped)
    {
        for (auto & input : inputs)
            input.close();
        output.finish();
        return Status::Finished;
    }

    bool all_finished = true;
    size_t input_index = 0;
    for (auto & input : inputs)
    {
        if (!input.isFinished())
        {
            input.setNeeded();
            if (input.hasData())
            {
                current_chunk = input.pull(true);
                current_input = input_index;
                has_current_chunk = true;
                return Status::Ready;
            }
            all_finished = false;
        }
        ++input_index;
    }

    if (!all_finished)
        return Status::NeedData;

    if (!finalized)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

void MergeRuntimeFiltersTransform::work()
{
    if (has_current_chunk)
        consume();
    else
        finalize();
}

void MergeRuntimeFiltersTransform::consume()
{
    const auto & column = assert_cast<const ColumnString &>(*current_chunk.getColumns().front());
    for (size_t row = 0; row < column.size() && !skipped; ++row)
    {
        /// Each stream carries one state and is never replayed, so a second state means a bug.
        if (received[current_input])
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Received more than one partial runtime filter '{}' from the same source", filter_name);
        received[current_input] = true;
        ++states_received;

        /// Points into `current_chunk`; parsed in place.
        const std::string_view state = column.getDataAt(row);

        ProfileEvents::increment(ProfileEvents::RuntimeFilterStatesReceived);
        ProfileEvents::increment(ProfileEvents::RuntimeFilterStateBytesReceived, state.size());

        /// Reject an oversized state before parsing it (and before the decoded set or bloom state
        /// is allocated). The whole filter is then unusable: publish nothing, fail open.
        if (state.size() > max_received_state_bytes)
        {
            skipped = true;
            accumulated.reset();
            ProfileEvents::increment(ProfileEvents::RuntimeFilterOversizedStatesRejected);
            LOG_WARNING(
                getLogger("RuntimeFilter"),
                "Skipping runtime filter '{}': received state of {} bytes exceeds the limit of {} bytes; rows will pass unfiltered",
                filter_name,
                state.size(),
                max_received_state_bytes);
            break;
        }

        ReadBufferFromMemory in(state.data(), state.size());
        if (!accumulated)
        {
            accumulated = std::make_unique<AdaptiveSetRuntimeFilter>(
                AdaptiveSetRuntimeFilter::deserialize(in, filter_column_target_type, geometry));
        }
        else
        {
            accumulated->mergeFrom(AdaptiveSetRuntimeFilter::deserialize(in, filter_column_target_type, geometry));
        }
    }
    current_chunk.clear();
    has_current_chunk = false;
}

void MergeRuntimeFiltersTransform::finalize()
{
    finalized = true;

    if (mode == Mode::ForwardUnion)
    {
        FailPointInjection::pauseFailPoint(FailPoints::distributed_plan_runtime_filter_merge_pause_before_finalize);
        fiu_do_on(FailPoints::distributed_plan_runtime_filter_merge_fails_before_finalize,
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Injected runtime filter merge failure");
        });
    }

    /// A missing state (e.g. a cancelled stream, or one whose producer was lost) means the union
    /// would be incomplete and must not be used for filtering: without it rows keep passing
    /// unfiltered, which is always correct.
    if (states_received != inputs.size())
    {
        accumulated.reset();
        return;
    }

    chassert(accumulated);

    if (mode == Mode::RegisterUnion)
    {
        /// Every input has delivered, so the union is complete.
        registerCompleteRuntimeFilter(*filter_lookup, filter_key, filter_name, geometry, std::move(*accumulated), enable_index_analysis);
        accumulated.reset();
        return;
    }

    WriteBufferFromOwnString out;
    accumulated->serialize(out);
    accumulated.reset();

    /// Cannot trigger with a valid geometry; checks for a sizing bug.
    if (out.str().size() > max_received_state_bytes)
    {
        skipped = true;
        ProfileEvents::increment(ProfileEvents::RuntimeFilterOversizedStatesRejected);
        LOG_WARNING(
            getLogger("RuntimeFilter"),
            "Skipping runtime filter '{}': merged state of {} bytes exceeds the limit of {} bytes; rows will pass unfiltered",
            filter_name,
            out.str().size(),
            max_received_state_bytes);
        return;
    }

    output_chunk = makeRuntimeFilterPartialsChunk(out.str(), num_forward_destinations);
    has_output_chunk = true;
}

}
