#pragma once

#include <base/types.h>
#include <future>
#include <memory>
#include <vector>
#include <Common/ZooKeeper/IKeeper.h>
#include <Poco/Event.h>


namespace zkutil
{

using Strings = std::vector<std::string>;


namespace CreateMode
{
    extern const int Persistent;
    extern const int Ephemeral;
    extern const int EphemeralSequential;
    extern const int PersistentSequential;
}

using EventPtr = std::shared_ptr<Poco::Event>;

/// Gets multiple asynchronous results
/// Each pair, the first is path, the second is response eg. CreateResponse, RemoveResponse
template <typename R>
using AsyncResponses = std::vector<std::pair<std::string, std::future<R>>>;

Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode);
Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version);
Coordination::RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version);
Coordination::RequestPtr makeCheckRequest(const std::string & path, int version);

}
