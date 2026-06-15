#pragma once

#include <future>
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

/// Gets multiple asynchronous results
/// Each pair, the first is path, the second is response eg. CreateResponse, RemoveResponse
template <typename R>
using AsyncResponses = std::vector<std::pair<std::string, std::future<R>>>;

Coordination::RequestPtr makeCreateRequest(const std::string & path, const std::string & data, int create_mode, bool ignore_if_exists = false);
Coordination::RequestPtr makeRemoveRequest(const std::string & path, int version, bool try_remove = false);
Coordination::RequestPtr makeSetRequest(const std::string & path, const std::string & data, int version);
Coordination::RequestPtr makeCheckRequest(const std::string & path, int version, bool not_exists = false, std::optional<Coordination::Stat> stat_to_check = std::nullopt);
Coordination::RequestPtr makeGetRequest(const std::string & path, Coordination::WatchCallbackPtrOrEventPtr watch = {});
Coordination::RequestPtr makeListRequest(const std::string & path, Coordination::ListRequestType list_request_type = Coordination::ListRequestType::ALL, Coordination::WatchCallbackPtrOrEventPtr watch = {});
Coordination::RequestPtr makeListRequest(const std::string & path, Coordination::ListRequestType list_request_type, bool with_stat, bool with_data, Coordination::WatchCallbackPtrOrEventPtr watch = {});
Coordination::RequestPtr makeSimpleListRequest(const std::string & path, Coordination::WatchCallbackPtrOrEventPtr watch = {});
Coordination::RequestPtr makeExistsRequest(const std::string & path, Coordination::WatchCallbackPtrOrEventPtr watch = {});
Coordination::RequestPtr makeRemoveRecursiveRequest(const std::string & path, uint32_t remove_nodes_limit);

template <class Client>
Coordination::RequestPtr makeRemoveRecursiveRequest(const Client & client, const std::string & path, uint32_t remove_nodes_limit);

}
