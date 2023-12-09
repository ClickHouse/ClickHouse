#include <cstddef>
#include <Processors/StreamingAdapter.h>
#include "Interpreters/Context.h"
#include "Processors/Port.h"
#include "base/defines.h"

namespace DB
{

StreamingAdapter::StreamingAdapter(const Block & header_, size_t num_streams, SubscriberPtr sub)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , subscriber(std::move(sub))
{
    ports_data.resize(num_streams);

    size_t cur_stream = 0;
    for (auto & input : inputs)
    {
        ports_data[cur_stream].input_port = &input;
        ++cur_stream;
    }

    cur_stream = 0;
    for (auto & output : outputs)
    {
        ports_data[cur_stream].output_port = &output;
        ++cur_stream;
    }
}

IProcessor::Status StreamingAdapter::prepare() {
  size_t ports_count = ports_data.size();

  size_t reading_from_storage_count = 0;
  size_t reading_from_subscription_count = 0;
  size_t finished_count = 0;

  for (auto & data : ports_data) {
    updateState(data);

    switch (data.state) {
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

  LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "storage_readers: {}, subscription_readers: {}, finished: {}", reading_from_storage_count, reading_from_subscription_count, finished_count);

  chassert(finished_count + reading_from_storage_count + reading_from_subscription_count == ports_count);

  if (finished_count == ports_count) {
    return Status::Finished;
  }

  bool has_full_port = false;

  if (reading_from_storage_count > 0) {
    for (auto & data : ports_data) {
      if (data.state != PortsDataState::ReadingFromStorage) {
        continue;
      }

      Status status = prepareStoragePair(data);

      switch (status) {
          case IProcessor::Status::PortFull:
            has_full_port = true;
            break;
          case IProcessor::Status::NeedData:
            break;
          default:
              throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected status for StreamingAdapter::prepareStoragePair : {}", IProcessor::statusToName(status));
      }
    }

    if (has_full_port) {
      return Status::PortFull;
    }

    return Status::NeedData;
  } else {
    for (auto & data : ports_data) {
      if (data.state != PortsDataState::ReadingFromSubscription) {
        continue;
      }

      Status status = prepareSubscriptionPair(data);

      switch (status) {
          case IProcessor::Status::PortFull:
            has_full_port = true;
            break;
          case IProcessor::Status::Async:
            break;
          default:
              throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected status for StreamingAdapter::prepareSubscriptionPair : {}", IProcessor::statusToName(status));
      }
    }

    if (has_full_port) {
      return Status::PortFull;
    }

    if (subscriber_chunks.empty()) {
      return Status::Async;
    } else {
      return Status::Ready;
    }
  }
}


void StreamingAdapter::updateState(PortsData& data) {
  if (data.input_port->isFinished() && data.output_port->isFinished()) {
    data.state = PortsDataState::Finished;
    return;
  }

  if (data.output_port->isFinished()) {
    data.input_port->close();
    data.state = PortsDataState::Finished;
    return;
  }

  if (data.input_port->isFinished()) {
    chassert(data.state != PortsDataState::Finished);
    data.state = PortsDataState::ReadingFromSubscription;
    return;
  }

  chassert(data.state == PortsDataState::ReadingFromStorage);
}


IProcessor::Status StreamingAdapter::prepareStoragePair(PortsData& data) {
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

IProcessor::Status StreamingAdapter::prepareSubscriptionPair(PortsData& data) {
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

void StreamingAdapter::work() {
  auto new_chunks = subscriber->extractAll();
  subscriber_chunks.splice(subscriber_chunks.end(), new_chunks);

  LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "processing subscriber chunks, count: {}", subscriber_chunks.size());

  for (auto& data : ports_data) {
    chassert(data.state != PortsDataState::ReadingFromStorage);

    if (data.state == PortsDataState::Finished) {
      continue;
    }

    if (subscriber_chunks.empty()) {
      return;
    }

    if (data.need_next_subscriber_chunk) {
      data.subscriber_chunk = std::move(subscriber_chunks.front());
      subscriber_chunks.pop_front();
      data.need_next_subscriber_chunk = false;
    }
  }
}

int StreamingAdapter::schedule() {
  int fd = subscriber->fd();
  LOG_DEBUG(&Poco::Logger::get("StreamingAdapter"), "waiting on descriptor: {}", fd);
  return fd;
}

}
