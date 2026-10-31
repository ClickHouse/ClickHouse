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
        const ClientInfo & client_info_,
        QueryStatusPtr process_list_element_ = nullptr)
      : RemoteInserter(connection_, timeouts, query_, settings_, client_info_, std::move(process_list_element_))
      , SinkToStorage(std::make_shared<const Block>(RemoteInserter::initializeAndGetHeader()))
    {
    }

    String getName() const override { return "RemoteSink"; }
    void consume (Chunk & chunk) override { write(RemoteInserter::getHeader().cloneWithColumns(chunk.getColumns())); }
    void onFinish() override { RemoteInserter::onFinish(); }
};

}
