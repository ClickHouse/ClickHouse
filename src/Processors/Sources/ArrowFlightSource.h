#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include "config.h"

#if USE_ARROWFLIGHT
#include <Processors/ISource.h>
#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCMessages.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <functional>
#include <mutex>


namespace grpc
{
class ClientContext;
}

namespace DB
{

class ArrowFlightConnection;
class IInputFormat;

/// Reads the data of Arrow Flight: from the endpoints of a dataset of a server, or, on the server, from the stream
/// of `DoPut`. The Arrow IPC messages are decoded by the native `ArrowStream` reader.
class ArrowFlightSource final : public ISource
{
    void initializeEndpoints(const String & dataset_name_);
    bool nextEndpoint();
    Block fillVirtualColumns(Block result_block);

public:
    using NextMessage = ArrowIPC::IPCMessagesReadBuffer::NextMessage;

    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, const String & dataset_name_, const Block & sample_block_, const Block & virtual_header_, ContextPtr context_);
    ArrowFlightSource(std::shared_ptr<ArrowFlightConnection> connection_, VectorWithMemoryTracking<String> tickets_, const Block & sample_block_, ContextPtr context_);
    /// Reads the messages that `next_message_` returns.
    ArrowFlightSource(NextMessage next_message_, const Block & sample_block_, ContextPtr context_);
    ~ArrowFlightSource() override;

protected:
    String getName() const override { return "ArrowFlightSource"; }
    Chunk generate() override;
    void onCancel() noexcept override;

private:
    /// Starts reading a stream of messages.
    void startReading(NextMessage next_message_);

    std::shared_ptr<ArrowFlightConnection> connection;
    Block sample_block;
    Block virtual_header;
    ContextPtr context;
    UInt64 request_timeout_sec = 0;
    /// The tickets of the endpoints, read one after another.
    VectorWithMemoryTracking<String> tickets;
    size_t current_endpoint = 0;

    struct Stream;
    std::unique_ptr<Stream> stream;

    std::mutex call_mutex;
    /// The context of the `DoGet` call while it is open, for cancellation: `onCancel` runs on another thread while
    /// `generate` is blocked reading from the call.
    std::shared_ptr<grpc::ClientContext> call_context TSA_GUARDED_BY(call_mutex);
};

}

#endif
