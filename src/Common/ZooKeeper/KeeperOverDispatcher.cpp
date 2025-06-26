#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperOverDispatcher.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace Coordination
{

void KeeperOverDispatcher::create(
    const String & path,
    const String & data,
    bool is_ephemeral,
    bool is_sequential,
    const ACLs & acls,
    CreateCallback callback)
{
    const auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data;
    request->is_ephemeral = is_ephemeral;
    request->is_sequential = is_sequential;
    request->acls = acls;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const CreateResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    const auto request = std::make_shared<ZooKeeperRemoveRequest>();
    request->path = path;
    request->version = version;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const RemoveResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::removeRecursive(
    const String & path,
    uint32_t remove_nodes_limit,
    RemoveRecursiveCallback callback)
{
    const auto request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
    request->path = path;
    request->remove_nodes_limit = remove_nodes_limit;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const RemoveRecursiveResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallbackPtr watch)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");

    const auto request = std::make_shared<ZooKeeperExistsRequest>();
    request->path = path;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const ExistsResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putLocalReadRequest(request, session_id);
}

void KeeperOverDispatcher::get(
    const String & path,
    GetCallback callback,
    WatchCallbackPtr watch)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");

    const auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = path;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const GetResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putLocalReadRequest(request, session_id);
}

void KeeperOverDispatcher::set(
    const String & path,
    const String & data,
    int32_t version,
    SetCallback callback)
{
    const auto request = std::make_shared<ZooKeeperSetRequest>();
    request->path = path;
    request->data = data;
    request->version = version;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const SetResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::list(
    const String & path,
    ListRequestType list_request_type,
    ListCallback callback,
    WatchCallbackPtr watch)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");

    const auto request = std::make_shared<ZooKeeperFilteredListRequest>();
    request->path = path;
    request->list_request_type = list_request_type;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const ListResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putLocalReadRequest(request, session_id);
}

void KeeperOverDispatcher::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    const auto request = std::make_shared<ZooKeeperCheckRequest>();
    request->path = path;
    request->version = version;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const CheckResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putLocalReadRequest(request, session_id);
}

void KeeperOverDispatcher::sync(
    const String & path,
    SyncCallback callback)
{
    const auto request = std::make_shared<ZooKeeperSyncRequest>();
    request->path = path;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const SyncResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::reconfig(
    std::string_view joining,
    std::string_view leaving,
    std::string_view new_members,
    int32_t version,
    ReconfigCallback callback)
{
    const auto request = std::make_shared<ZooKeeperReconfigRequest>();
    request->joining = joining;
    request->leaving = leaving;
    request->new_members = new_members;
    request->version = version;

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const ReconfigResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::multi(
        const Requests & requests,
        MultiCallback callback)
{
    multi(std::span(requests), std::move(callback));
}

void KeeperOverDispatcher::multi(
    std::span<const RequestPtr> requests,
    MultiCallback callback)
{
    const auto request = std::shared_ptr<ZooKeeperMultiRequest>(new ZooKeeperMultiRequest(requests, {}));  // NOLINT(modernize-make-shared)

    const auto session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());

    auto response_callback = [callback](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
            return;

        callback(dynamic_cast<const MultiResponse &>(*response));
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
    keeper_dispatcher->putRequest(request, session_id, false);
}

}
