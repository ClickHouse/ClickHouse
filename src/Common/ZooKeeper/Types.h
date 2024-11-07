#pragma once

#include <future>
#include <memory>
#include <vector>
#include <base/types.h>
#include <Poco/Event.h>
#include <Common/ZooKeeper/IKeeper.h>


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

Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode, bool ignore_if_exists = false);
Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version);
Coordination::RequestPtr makeRemoveRecursiveRequest(const std::string & path, uint32_t remove_nodes_limit);
Coordination::RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version);
Coordination::RequestPtr makeCheckRequest(const std::string & path, int version);

Coordination::RequestPtr makeGetRequest(const std::string & path);
Coordination::RequestPtr
makeListRequest(const std::string & path, Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL);
Coordination::RequestPtr makeSimpleListRequest(const std::string & path);
Coordination::RequestPtr makeExistsRequest(const std::string & path);
}
