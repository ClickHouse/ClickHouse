#include "config.h"

#if USE_NURAFT

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/KeeperOverDispatcher.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace Coordination
{

KeeperOverDispatcher::KeeperOverDispatcher(
    const std::shared_ptr<KeeperDispatcher> & keeper_dispatcher_,
    const Poco::Timespan & session_timeout_)
    : keeper_dispatcher(keeper_dispatcher_)
    , session_timeout(session_timeout_)
    , session_id(keeper_dispatcher_->getSessionID(session_timeout_.totalMilliseconds()))
{
    LOG_DEBUG(&Poco::Logger::get("KeeperOverDispatcher"), "Created KeeperOverDispatcher session {} with timeout {} ms", session_id, session_timeout_.totalMilliseconds());

    /// Register session with a callback that dispatches responses based on XID.
    /// Capture callback_state by shared_ptr so the lambda remains valid even after
    /// this KeeperOverDispatcher is destroyed (prevents use-after-free when
    /// routeResponse invokes the callback outside its mutex).
    auto state = callback_state;
    auto response_callback = [state](const ZooKeeperResponsePtr & response, ZooKeeperRequestPtr)
    {
        if (dynamic_cast<const ZooKeeperCloseResponse *>(response.get()))
        {
            state->expired = true;
            return;
        }

        ResponseCallback callback;
        {
            std::lock_guard lock(state->callbacks_mutex);
            auto it = state->callbacks.find(response->xid);
            if (it != state->callbacks.end())
            {
                callback = std::move(it->second);
                state->callbacks.erase(it);
            }
        }

        if (callback)
            callback(response);
    };

    keeper_dispatcher->registerSession(session_id, response_callback);
}

KeeperOverDispatcher::~KeeperOverDispatcher()
{
    keeper_dispatcher->terminateSession(session_id);
}

void KeeperOverDispatcher::finalize(const String & /* reason */)
{
    callback_state->expired = true;
}

void KeeperOverDispatcher::pushRequest(ZooKeeperRequestPtr request, ResponseCallback callback)
{
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = std::move(callback);
    }

    keeper_dispatcher->putRequest(request, session_id, false);
}

void KeeperOverDispatcher::failCallback(XID xid, ZooKeeperResponsePtr response, Coordination::Error error)
{
    ResponseCallback cb;
    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        auto it = callback_state->callbacks.find(xid);
        if (it != callback_state->callbacks.end())
        {
            cb = std::move(it->second);
            callback_state->callbacks.erase(it);
        }
    }
    if (cb)
    {
        response->error = error;
        cb(response);
    }
}

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

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const CreateResponse &>(*response));
    });
}

void KeeperOverDispatcher::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    const auto request = std::make_shared<ZooKeeperRemoveRequest>();
    request->path = path;
    request->version = version;

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const RemoveResponse &>(*response));
    });
}

void KeeperOverDispatcher::removeRecursive(
    const String & path,
    uint32_t remove_nodes_limit,
    RemoveRecursiveCallback callback)
{
    const auto request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
    request->path = path;
    request->remove_nodes_limit = remove_nodes_limit;

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const RemoveRecursiveResponse &>(*response));
    });
}

void KeeperOverDispatcher::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallbackPtrOrEventPtr watch)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");

    const auto request = std::make_shared<ZooKeeperExistsRequest>();
    request->path = path;
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = [callback](const ZooKeeperResponsePtr & response)
        {
            callback(dynamic_cast<const ExistsResponse &>(*response));
        };
    }

    if (!keeper_dispatcher->putLocalReadRequest(request, session_id))
        failCallback(request->xid, request->makeResponse(), Coordination::Error::ZSESSIONEXPIRED);
}

void KeeperOverDispatcher::get(
    const String & path,
    GetCallback callback,
    WatchCallbackPtrOrEventPtr watch)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");

    const auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = path;
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = [callback](const ZooKeeperResponsePtr & response)
        {
            callback(dynamic_cast<const GetResponse &>(*response));
        };
    }

    if (!keeper_dispatcher->putLocalReadRequest(request, session_id))
        failCallback(request->xid, request->makeResponse(), Coordination::Error::ZSESSIONEXPIRED);
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

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const SetResponse &>(*response));
    });
}

void KeeperOverDispatcher::list(
    const String & path,
    ListRequestType list_request_type,
    ListCallback callback,
    WatchCallbackPtrOrEventPtr watch,
    bool with_stat,
    bool with_data)
{
    if (watch)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watch is not implemented");
    if (with_stat || with_data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "with_stat and with_data are not implemented");

    const auto request = std::make_shared<ZooKeeperFilteredListRequest>();
    request->path = path;
    request->list_request_type = list_request_type;
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = [callback](const ZooKeeperResponsePtr & response)
        {
            callback(dynamic_cast<const ListResponse &>(*response));
        };
    }

    if (!keeper_dispatcher->putLocalReadRequest(request, session_id))
        failCallback(request->xid, request->makeResponse(), Coordination::Error::ZSESSIONEXPIRED);
}

void KeeperOverDispatcher::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    const auto request = std::make_shared<ZooKeeperCheckRequest>();
    request->path = path;
    request->version = version;
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = [callback](const ZooKeeperResponsePtr & response)
        {
            callback(dynamic_cast<const CheckResponse &>(*response));
        };
    }

    if (!keeper_dispatcher->putLocalReadRequest(request, session_id))
        failCallback(request->xid, request->makeResponse(), Coordination::Error::ZSESSIONEXPIRED);
}

void KeeperOverDispatcher::sync(
    const String & path,
    SyncCallback callback)
{
    const auto request = std::make_shared<ZooKeeperSyncRequest>();
    request->path = path;

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const SyncResponse &>(*response));
    });
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

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const ReconfigResponse &>(*response));
    });
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

    pushRequest(request, [callback](const ZooKeeperResponsePtr & response)
    {
        callback(dynamic_cast<const MultiResponse &>(*response));
    });
}

void KeeperOverDispatcher::getACL(const String & path, GetACLCallback callback)
{
    const auto request = std::make_shared<ZooKeeperGetACLRequest>();
    request->path = path;
    request->xid = next_xid++;

    {
        std::lock_guard lock(callback_state->callbacks_mutex);
        callback_state->callbacks[request->xid] = [callback](const ZooKeeperResponsePtr & response)
        {
            callback(dynamic_cast<const GetACLResponse &>(*response));
        };
    }

    if (!keeper_dispatcher->putLocalReadRequest(request, session_id))
        failCallback(request->xid, request->makeResponse(), Coordination::Error::ZSESSIONEXPIRED);
}


}

#endif
