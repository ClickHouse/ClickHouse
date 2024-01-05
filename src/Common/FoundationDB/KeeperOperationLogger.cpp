#include <Common/FoundationDB/KeeperOperationLogger.h>

namespace DB
{
class ZooKeeperLog;
}

namespace Coordination
{

KeeperOperationLogger::KeeperOperationLogger(std::shared_ptr<ZooKeeperLog> zk_log_)
{
    std::atomic_store(&zk_log, std::move(zk_log_));
}

void KeeperOperationLogger::setZooKeeperLog(std::shared_ptr<ZooKeeperLog> zk_log_)
{
    std::atomic_store(&zk_log, std::move(zk_log_));
}

void KeeperOperationLogger::createRequest(const String & path, const String & data, bool is_ephemeral, bool is_sequential, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Create);
    elem.path = path;
    elem.data = data;
    elem.is_ephemeral = is_ephemeral;
    elem.is_sequential = is_sequential;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::removeRequest(const String & path, int32_t version, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Remove);
    elem.path = path;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::setRequest(const String & path, const String & data, int32_t version, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Set);
    elem.path = path;
    elem.data = data;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::getRequest(const String & path, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Get);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::existsRequest(const String & path, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Exists);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::listRequest(const String & path, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::List);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::simpleListRequest(const String & path, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::SimpleList);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::filteredListRequest(const String & path, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::FilteredList);
    elem.path = path;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::checkRequest(const String & path, int32_t version, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Check);
    elem.path = path;
    elem.version = version;
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::multiRequest(const Requests & requests, bool has_watch, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                               std::chrono::system_clock::now().time_since_epoch()
                               ).count();

    elem.type = ZooKeeperLogElement::REQUEST;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(OpNum::Multi);
    elem.requests_size = static_cast<uint32_t>(requests.size());
    elem.event_time = event_time;
    elem.session_id = session_id;

    maybe_zk_log->add(std::move(elem));
}

void KeeperOperationLogger::response(const Response & resp, size_t elapsed_ms, int64_t session_id) const
{
    auto maybe_zk_log = std::atomic_load(&zk_log);
    if (!maybe_zk_log)
        return;

    ZooKeeperLogElement elem;
    Decimal64 event_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch()
                            ).count();

    if (auto * createResponse = dynamic_cast<const CreateResponse *>(&resp))
    {
        elem.path_created = createResponse->path_created;
        elem.op_num = static_cast<uint32_t>(OpNum::Create);
    }
    else if (auto * existsResponse = dynamic_cast<const ExistsResponse *>(&resp))
    {
        elem.stat = existsResponse->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Exists);
    }    
    else if (auto * getResponse = dynamic_cast<const GetResponse *>(&resp))
    {
        elem.data = getResponse->data;
        elem.stat = getResponse->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Get);
    }
    else if (auto * setResponse = dynamic_cast<const SetResponse *>(&resp))
    {
        elem.stat = setResponse->stat;
        elem.op_num = static_cast<uint32_t>(OpNum::Set);
    }
    else if (auto * listResponse = dynamic_cast<const ListResponse *>(&resp))
    {
        elem.stat = listResponse->stat;
        elem.children = listResponse->names;
        elem.op_num = static_cast<uint32_t>(OpNum::List);
    }
    else if (auto * watchResponse = dynamic_cast<const WatchResponse *>(&resp))
    {
        elem.watch_type = watchResponse->type;
        elem.watch_state = watchResponse->state;
        elem.path = watchResponse->path;
    }
    else if (auto * checkResponse = dynamic_cast<const CheckResponse *>(&resp))
    {
        elem.op_num = static_cast<uint32_t>(OpNum::Check);
    }
    else if (auto * multiResponse = dynamic_cast<const MultiResponse *>(&resp))
    {
        elem.op_num = static_cast<uint32_t>(OpNum::Multi);

        for (const auto & resp_ : multiResponse->responses)
        {
            auto & res = dynamic_cast<Response &>(*resp_);
            response(res, elapsed_ms, session_id);
        }
    }

    elem.type = ZooKeeperLogElement::RESPONSE;
    elem.error = static_cast<Int32>(resp.error);
    elem.zxid = resp.zxid;
    elem.event_time = event_time;
    elem.session_id = session_id;
    elem.duration_ms = elapsed_ms;

    maybe_zk_log->add(std::move(elem));
}

}
