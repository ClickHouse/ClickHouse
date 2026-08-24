#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
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

SharedHeader runtimeFilterPartialsHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "partial")});
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
        /// Failing closed on a duplicate is sound because the exchange layer delivers each stream
        /// at most once: a producer emits exactly one state row per stream, a build task is never
        /// re-run (task starts are not retried -- `sendTask` in `StatelessWorkerClient.cpp` -- and
        /// the worker ignores a duplicate start, `StatelessTaskExecutor::startTask`), a stream
        /// pairs one producer with one consumer (`ExchangeConnections` drops a duplicate producer
        /// and refuses a duplicate consumer), and a broken stream fails the whole query instead of
        /// reconnecting and replaying (`StreamingExchangeSource`). A second state from the same
        /// source therefore indicates a bug, not a benign redelivery.
        if (received[current_input])
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Received more than one partial runtime filter '{}' from the same source", filter_name);
        received[current_input] = true;
        ++states_received;

        /// A view into the chunk's column; valid until `current_chunk` is released below. The state
        /// is parsed straight from it, without materializing an intermediate copy.
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
            accumulated = ApproximateRuntimeFilter::deserialize(in, inputs.size() - 1, filter_column_target_type, geometry);
        }
        else
        {
            /// Deserialize-and-merge immediately; the decoded state dies at the end of this block.
            auto arrived = ApproximateRuntimeFilter::deserialize(in, 0, filter_column_target_type, geometry);
            accumulated->merge(arrived.get());
        }
    }
    current_chunk.clear();
    has_current_chunk = false;
}

void MergeRuntimeFiltersTransform::finalize()
{
    finalized = true;

    /// A missing state (e.g. a cancelled stream) means the union would be incomplete and must not
    /// be used for filtering: without it rows keep passing unfiltered, which is always correct.
    if (states_received != inputs.size())
    {
        accumulated.reset();
        return;
    }

    chassert(accumulated);

    if (mode == Mode::RegisterUnion)
    {
        filter_lookup->add(filter_key, filter_name, std::move(accumulated));
        return;
    }

    WriteBufferFromOwnString out;
    accumulated->serialize(out);
    accumulated.reset();

    /// The merged union must respect the same limit its receivers enforce; with a valid geometry
    /// this cannot trigger, so it is pure defense in depth against a sizing bug.
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

    ProfileEvents::increment(ProfileEvents::RuntimeFilterStatesSent, num_forward_destinations);
    ProfileEvents::increment(ProfileEvents::RuntimeFilterStateBytesSent, out.str().size() * num_forward_destinations);

    auto column = ColumnString::create();
    column->insertData(out.str().data(), out.str().size());
    Columns columns;
    columns.emplace_back(std::move(column));
    output_chunk = Chunk(std::move(columns), 1);
    has_output_chunk = true;
}

}
