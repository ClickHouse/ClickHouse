#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include "KeeperOperationLogger.h"

namespace DB
{
class ZooKeeperLog;

namespace FoundationDB
{

using namespace Coordination;

KeeperOperationLogger::KeeperOperationLogger(std::shared_ptr<ZooKeeperLog> zk_log_, KeeperSession & session_) : session(session_)
{
    std::atomic_store(&zk_log, std::move(zk_log_));
}

void KeeperOperationLogger::setZooKeeperLog(std::shared_ptr<ZooKeeperLog> zk_log_)
{
    std::atomic_store(&zk_log, std::move(zk_log_));
}

KeeperResponseLogger
KeeperOperationLogger::createRequest(const String & path, const String & data, bool is_ephemeral, bool is_sequential, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = false;
    elem.op_num = static_cast<uint32_t>(OpNum::Create);
    elem.path = path;
    elem.data = data;
    elem.is_ephemeral = is_ephemeral;
    elem.is_sequential = is_sequential;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));

    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::removeRequest(const String & path, int32_t version, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = false;
    elem.op_num = static_cast<uint32_t>(OpNum::Remove);
    elem.path = path;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::setRequest(const String & path, const String & data, int32_t version, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = false;
    elem.op_num = static_cast<uint32_t>(OpNum::Set);
    elem.path = path;
    elem.data = data;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::getRequest(const String & path, bool has_watch, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Get);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::existsRequest(const String & path, bool has_watch, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Exists);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::listRequest(const String & path, bool has_watch, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::List);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::simpleListRequest(const String & path, bool has_watch, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::SimpleList);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::filteredListRequest(const String & path, bool has_watch, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::FilteredList);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::checkRequest(const String & path, int32_t version, OptionalXID force_xid)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = next_xid(force_xid);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = false;
    elem.op_num = static_cast<uint32_t>(OpNum::Check);
    elem.path = path;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));
    return KeeperResponseLogger{zk_log, session_id, xid};
}

KeeperResponseLogger KeeperOperationLogger::multiRequest(const Requests & requests)
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return {};

    auto session_id = session.currentSessionSync();
    auto xid = xid_cnt.fetch_add(1);

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = false;
    elem.op_num = static_cast<uint32_t>(OpNum::Multi);
    elem.requests_size = static_cast<uint32_t>(requests.size());
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));

    for (const auto & request : requests)
    {
        if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(request.get()))
        {
            createRequest(
                concrete_request_create->path,
                concrete_request_create->data,
                concrete_request_create->is_ephemeral,
                concrete_request_create->is_sequential,
                xid);
        }
        else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(request.get()))
        {
            removeRequest(concrete_request_remove->path, concrete_request_remove->version, xid);
        }
        else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(request.get()))
        {
            setRequest(concrete_request_set->path, concrete_request_set->data, concrete_request_set->version, xid);
        }
        else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(request.get()))
        {
            checkRequest(concrete_request_check->path, concrete_request_check->version, xid);
        }
        else if (const auto * concrete_request_get = dynamic_cast<const GetRequest *>(request.get()))
        {
            getRequest(concrete_request_get->path, false, xid);
        }
        else if (const auto * concrete_request_exists = dynamic_cast<const ExistsRequest *>(request.get()))
        {
            existsRequest(concrete_request_exists->path, false, xid);
        }
        else if (const auto * concrete_request_simple_list = dynamic_cast<const ZooKeeperSimpleListRequest *>(request.get()))
        {
            simpleListRequest(concrete_request_simple_list->path, false, xid);
        }
        else if (const auto * concrete_request_filted_list = dynamic_cast<const ZooKeeperFilteredListRequest *>(request.get()))
        {
            filteredListRequest(concrete_request_filted_list->path, false, xid);
        }
    }

    return KeeperResponseLogger{zk_log, session_id, xid};
}

void KeeperResponseLogger::response(const Response & resp, size_t elapsed_ms) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time
        = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    if (const auto * create_response = dynamic_cast<const CreateResponse *>(&resp))
    {
        elem.path_created = create_response->path_created;
        elem.op_num = static_cast<uint32_t>(OpNum::Create);
    }
    else if (const auto * exists_response = dynamic_cast<const ExistsResponse *>(&resp))
    {
        elem.stat = exists_response->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Exists);
    }
    else if (const auto * get_response = dynamic_cast<const GetResponse *>(&resp))
    {
        elem.data = get_response->data;
        elem.stat = get_response->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Get);
    }
    else if (const auto * set_response = dynamic_cast<const SetResponse *>(&resp))
    {
        elem.stat = set_response->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Set);
    }
    else if (const auto * list_response = dynamic_cast<const ListResponse *>(&resp))
    {
        elem.stat = list_response->stat;
        elem.children = list_response->names;
        elem.op_num = static_cast<uint32_t>(OpNum::List);
    }
    else if (const auto * watch_response = dynamic_cast<const WatchResponse *>(&resp))
    {
        elem.watch_type = watch_response->type;
        elem.watch_state = watch_response->state;
        elem.path = watch_response->path;
    }
    else if (const auto * check_response = dynamic_cast<const CheckResponse *>(&resp))
    {
        elem.op_num = static_cast<uint32_t>(OpNum::Check);
    }
    else if (const auto * multi_response = dynamic_cast<const MultiResponse *>(&resp))
    {
        elem.op_num = static_cast<uint32_t>(OpNum::Multi);
    }

    elem.type = ZooKeeperLogElement::RESPONSE;
    elem.error = static_cast<Int32>(resp.error);
    elem.zxid = resp.zxid;
    elem.event_time = event_time;
    elem.session_id = request_session;
    elem.duration_ms = elapsed_ms;
    elem.xid = xid;

    maybe_zk_log->add(std::move(elem));

    if (const auto * multi_response = dynamic_cast<const MultiResponse *>(&resp))
    {
        for (const auto & response : multi_response->responses)
        {
            auto & res = dynamic_cast<Response &>(*response);
            this->response(res, elapsed_ms);
        }
    }    
}
}

}
