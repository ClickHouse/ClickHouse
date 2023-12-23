#include <ranges>

#include <base/defines.h>

#include <Common/logger_useful.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/StreamingAdapter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StreamingAdapter::StreamingAdapter(const Block & header_, size_t num_streams, Block sample, SubscriberPtr sub)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , storage_sample(std::move(sample))
    , subscriber(std::move(sub))
{
    ports_data.resize(num_streams);

    for (const auto [data, input, output] : std::views::zip(ports_data, inputs, outputs))
    {
        data.input_port = &input;
        data.output_port = &output;
    }

    fd = subscriber->fd();
}

IProcessor::Status StreamingAdapter::prepare()
{
    if (isCancelled())
    {
        LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "cancelling processor");

        for (const auto & data : ports_data)
        {
            data.input_port->close();
            data.output_port->finish();
        }

        return Status::Finished;
    }

    size_t ports_count = ports_data.size();

    size_t reading_from_storage_count = 0;
    size_t reading_from_subscription_count = 0;
    size_t finished_count = 0;

    for (auto & data : ports_data)
    {
        updateState(data);

        switch (data.state)
        {
            case PortsDataState::ReadingFromStorage:
                reading_from_storage_count += 1;
                break;
            case PortsDataState::ReadingFromSubscription:
                reading_from_subscription_count += 1;
                break;
            case PortsDataState::Finished:
                finished_count += 1;
                break;
        }
    }

    LOG_DEBUG(
        &Poco::Logger::get("StreamingAdapter"),
        "storage_readers: {}, subscription_readers: {}, finished: {}",
        reading_from_storage_count,
        reading_from_subscription_count,
        finished_count);

    chassert(finished_count + reading_from_storage_count + reading_from_subscription_count == ports_count);

    if (finished_count == ports_count)
        return Status::Finished;

    bool has_full_port = false;

    if (reading_from_storage_count > 0)
    {
        for (auto & data : ports_data)
        {
            if (data.state != PortsDataState::ReadingFromStorage)
                continue;

            Status status = prepareStoragePair(data);

            switch (status)
            {
                case IProcessor::Status::PortFull:
                    has_full_port = true;
                    break;
                case IProcessor::Status::NeedData:
                    break;
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status for StreamingAdapter::prepareStoragePair : {}",
                        IProcessor::statusToName(status));
            }
        }

        if (has_full_port)
            return Status::PortFull;

        return Status::NeedData;
    }
    else
    {
        for (auto & data : ports_data)
        {
            if (data.state != PortsDataState::ReadingFromSubscription)
                continue;

            Status status = prepareSubscriptionPair(data);

            switch (status)
            {
                case IProcessor::Status::PortFull:
                    has_full_port = true;
                    break;
                case IProcessor::Status::Async:
                    break;
                default:
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected status for StreamingAdapter::prepareSubscriptionPair : {}",
                        IProcessor::statusToName(status));
            }
        }

        if (has_full_port)
            return Status::PortFull;

        if (subscriber_chunks.empty() && fd.has_value())
            return Status::Async;
        else
            return Status::Ready;
    }
}


void StreamingAdapter::updateState(PortsData & data)
{
    if (data.input_port->isFinished() && data.output_port->isFinished())
    {
        data.state = PortsDataState::Finished;
        return;
    }

    if (data.output_port->isFinished())
    {
        data.input_port->close();
        data.state = PortsDataState::Finished;
        return;
    }

    if (data.input_port->isFinished())
    {
        chassert(data.state != PortsDataState::Finished);
        data.state = PortsDataState::ReadingFromSubscription;
        return;
    }

    chassert(data.state == PortsDataState::ReadingFromStorage);
}


IProcessor::Status StreamingAdapter::prepareStoragePair(PortsData & data)
{
    /// Check can output.
    if (!data.output_port->canPush())
    {
        data.input_port->setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.
    if (!data.input_port->hasData())
    {
        data.input_port->setNeeded();
        return Status::NeedData;
    }

    /// push generated chunk to output
    Chunk chunk = data.input_port->pull(true);
    data.output_port->push(std::move(chunk));

    return Status::PortFull;
}

IProcessor::Status StreamingAdapter::prepareSubscriptionPair(PortsData & data)
{
    /// Check can output.
    if (!data.output_port->canPush())
    {
        data.input_port->setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.
    if (!data.subscriber_chunk.has_value())
    {
        data.need_next_subscriber_chunk = true;
        return Status::Async;
    }

    chassert(data.need_next_subscriber_chunk == false);

    /// push generated chunk to output
    data.output_port->push(std::move(data.subscriber_chunk.value()));
    data.subscriber_chunk = std::nullopt;

    return Status::PortFull;
}

Chunk StreamingAdapter::FilterStorageChunk(Chunk chunk, const Block & header)
{
    // chunk was inserted into storage -> it must have the same structure as in the storage sample
    Block storage_block = storage_sample.cloneWithColumns(chunk.detachColumns());

    Block result;
    for (const auto & column_name : header.getNames())
    {
        auto column = storage_block.getByName(column_name);
        result.insert(std::move(column));
    }

    return Chunk(result.getColumns(), result.rows());
}

void StreamingAdapter::work()
{
    if (isCancelled())
        return;

    if (subscriber_chunks.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "extracting new chunk batch");
        auto new_chunks = subscriber->extractAll();
        subscriber_chunks.splice(subscriber_chunks.end(), new_chunks);
    }

    LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "processing subscriber chunks, count: {}", subscriber_chunks.size());

    for (auto & data : ports_data)
    {
        chassert(data.state != PortsDataState::ReadingFromStorage);

        if (data.state == PortsDataState::Finished)
            continue;

        if (subscriber_chunks.empty())
            return;

        if (data.need_next_subscriber_chunk)
        {
            Chunk new_chunk = std::move(subscriber_chunks.front());
            data.subscriber_chunk = FilterStorageChunk(std::move(new_chunk), data.output_port->getHeader());
            subscriber_chunks.pop_front();
            data.need_next_subscriber_chunk = false;
        }
    }
}

int StreamingAdapter::schedule()
{
    chassert(fd.has_value());
    LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "waiting on descriptor: {}", fd.value());
    return fd.value();
}

void StreamingAdapter::onCancel()
{
    LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "cancelling subscription");
    subscriber->cancel();
}

}
