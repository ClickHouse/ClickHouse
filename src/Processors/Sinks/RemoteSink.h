#pragma once
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/RemoteInserter.h>

namespace DB
{

class RemoteSink final : public RemoteInserter, public SinkToStorage
{
public:
    explicit RemoteSink(
        Connection & connection_,
        const ConnectionTimeouts & timeouts,
        const String & query_,
        const Settings & settings_,
        const ClientInfo & client_info_)
      : RemoteInserter(connection_, timeouts, query_, settings_, client_info_)
      , SinkToStorage(RemoteInserter::getHeader())
    {
    }

    String getName() const override { return "RemoteSink"; }
    void consume (Chunk chunk) override { write(RemoteInserter::getHeader().cloneWithColumns(chunk.detachColumns())); }
    void onFinish() override { RemoteInserter::onFinish(); }
};

}
