#pragma once

#include <Core/Block.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/Throttler.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/ClientInfo.h>


namespace DB
{

class Connection;
class ReadBuffer;
struct Settings;


/** Allow to execute INSERT query on remote server and send data for it.
  */
class RemoteInserter
{
public:
    RemoteInserter(
        Connection & connection_,
        const ConnectionTimeouts & timeouts,
        const String & query_,
        const Settings & settings_,
        const ClientInfo & client_info_);

    void write(Block block);
    void onFinish();

    /// Send pre-serialized and possibly pre-compressed block of data, that will be read from 'input'.
    void writePrepared(ReadBuffer & buf, size_t size = 0);

    ~RemoteInserter();

    const Block & getHeader() const { return header; }

private:
    Connection & connection;
    String query;
    Block header;
    bool finished = false;
};

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
