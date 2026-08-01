#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

/** The commands that answer without touching a collection.
  *
  * `ping`, `buildInfo` and `connectionStatus` are what a driver or a shell asks on connect and for
  * a health check. `endSessions` and `killCursors` release server side state that this protocol
  * never creates - the whole result of a `find` is returned in the first batch, so there is no
  * cursor to kill - and answer that the work is done.
  */
struct ServerCommandsHandler : IHandler
{
    ServerCommandsHandler() = default;

    std::vector<String> getIdentifiers() const override
    {
        return {"ping", "buildInfo", "buildinfo", "connectionStatus", "endSessions", "killCursors", "getFreeMonitoringStatus"};
    }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

struct DropDatabaseHandler : IHandler
{
    DropDatabaseHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"dropDatabase"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
