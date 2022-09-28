#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Stopwatch.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <base/logger_useful.h>
#include <array>


namespace Coordination
{

using namespace DB;

void ZooKeeperResponse::write(WriteBuffer & out) const
{
    /// Excessive copy to calculate length.
    WriteBufferFromOwnString buf;
    Coordination::write(xid, buf);
    Coordination::write(zxid, buf);
    Coordination::write(error, buf);
    if (error == Error::ZOK)
        writeImpl(buf);
    Coordination::write(buf.str(), out);
    out.next();
}

void ZooKeeperRequest::write(WriteBuffer & out) const
{
    /// Excessive copy to calculate length.
    WriteBufferFromOwnString buf;
    Coordination::write(xid, buf);
    Coordination::write(getOpNum(), buf);
    writeImpl(buf);
    Coordination::write(buf.str(), out);
    out.next();
}

void ZooKeeperSyncRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
}

void ZooKeeperSyncRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
}

void ZooKeeperSyncResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
}

void ZooKeeperSyncResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
}

void ZooKeeperWatchResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(type, in);
    Coordination::read(state, in);
    Coordination::read(path, in);
}

void ZooKeeperWatchResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(type, out);
    Coordination::write(state, out);
    Coordination::write(path, out);
}

void ZooKeeperWatchResponse::write(WriteBuffer & out) const
{
    if (error == Error::ZOK)
        ZooKeeperResponse::write(out);
    /// skip bad responses for watches
}

void ZooKeeperAuthRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(type, out);
    Coordination::write(scheme, out);
    Coordination::write(data, out);
}

void ZooKeeperAuthRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(type, in);
    Coordination::read(scheme, in);
    Coordination::read(data, in);
}

void ZooKeeperCreateRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(data, out);
    Coordination::write(acls, out);

    int32_t flags = 0;

    if (is_ephemeral)
        flags |= 1;
    if (is_sequential)
        flags |= 2;

    Coordination::write(flags, out);
}

void ZooKeeperCreateRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(data, in);
    Coordination::read(acls, in);

    int32_t flags = 0;
    Coordination::read(flags, in);

    if (flags & 1)
        is_ephemeral = true;
    if (flags & 2)
        is_sequential = true;
}

void ZooKeeperCreateResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(path_created, in);
}

void ZooKeeperCreateResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path_created, out);
}

void ZooKeeperRemoveRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(version, out);
}

void ZooKeeperRemoveRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(version, in);
}

void ZooKeeperExistsRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

void ZooKeeperExistsRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

void ZooKeeperExistsResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(stat, in);
}

void ZooKeeperExistsResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

void ZooKeeperGetRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

void ZooKeeperGetRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

void ZooKeeperGetResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(data, in);
    Coordination::read(stat, in);
}

void ZooKeeperGetResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(data, out);
    Coordination::write(stat, out);
}

void ZooKeeperSetRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(data, out);
    Coordination::write(version, out);
}

void ZooKeeperSetRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(data, in);
    Coordination::read(version, in);
}

void ZooKeeperSetResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(stat, in);
}

void ZooKeeperSetResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

void ZooKeeperListRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

void ZooKeeperListRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

void ZooKeeperListResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(names, in);
    Coordination::read(stat, in);
}

void ZooKeeperListResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(names, out);
    Coordination::write(stat, out);
}


void ZooKeeperSetACLRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(acls, out);
    Coordination::write(version, out);
}

void ZooKeeperSetACLRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(acls, in);
    Coordination::read(version, in);
}

void ZooKeeperSetACLResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

void ZooKeeperSetACLResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(stat, in);
}

void ZooKeeperGetACLRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
}

void ZooKeeperGetACLRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
}

void ZooKeeperGetACLResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(acl, out);
    Coordination::write(stat, out);
}

void ZooKeeperGetACLResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(acl, in);
    Coordination::read(stat, in);
}

void ZooKeeperCheckRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(version, out);
}

void ZooKeeperCheckRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(version, in);
}

void ZooKeeperErrorResponse::readImpl(ReadBuffer & in)
{
    Coordination::Error read_error;
    Coordination::read(read_error, in);

    if (read_error != error)
        throw Exception(fmt::format("Error code in ErrorResponse ({}) doesn't match error code in header ({})", read_error, error),
            Error::ZMARSHALLINGERROR);
}

void ZooKeeperErrorResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(error, out);
}

ZooKeeperMultiRequest::ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls)
{
    /// Convert nested Requests to ZooKeeperRequests.
    /// Note that deep copy is required to avoid modifying path in presence of chroot prefix.
    requests.reserve(generic_requests.size());

    for (const auto & generic_request : generic_requests)
    {
        if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
        {
            auto create = std::make_shared<ZooKeeperCreateRequest>(*concrete_request_create);
            if (create->acls.empty())
                create->acls = default_acls;
            requests.push_back(create);
        }
        else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
        {
            requests.push_back(std::make_shared<ZooKeeperRemoveRequest>(*concrete_request_remove));
        }
        else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
        {
            requests.push_back(std::make_shared<ZooKeeperSetRequest>(*concrete_request_set));
        }
        else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
        {
            requests.push_back(std::make_shared<ZooKeeperCheckRequest>(*concrete_request_check));
        }
        else
            throw Exception("Illegal command as part of multi ZooKeeper request", Error::ZBADARGUMENTS);
    }
}

void ZooKeeperMultiRequest::writeImpl(WriteBuffer & out) const
{
    for (const auto & request : requests)
    {
        const auto & zk_request = dynamic_cast<const ZooKeeperRequest &>(*request);

        bool done = false;
        int32_t error = -1;

        Coordination::write(zk_request.getOpNum(), out);
        Coordination::write(done, out);
        Coordination::write(error, out);

        zk_request.writeImpl(out);
    }

    OpNum op_num = OpNum::Error;
    bool done = true;
    int32_t error = -1;

    Coordination::write(op_num, out);
    Coordination::write(done, out);
    Coordination::write(error, out);
}

void ZooKeeperMultiRequest::readImpl(ReadBuffer & in)
{

    while (true)
    {
        OpNum op_num;
        bool done;
        int32_t error;
        Coordination::read(op_num, in);
        Coordination::read(done, in);
        Coordination::read(error, in);

        if (done)
        {
            if (op_num != OpNum::Error)
                throw Exception("Unexpected op_num received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
            if (error != -1)
                throw Exception("Unexpected error value received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
            break;
        }

        ZooKeeperRequestPtr request = ZooKeeperRequestFactory::instance().get(op_num);
        request->readImpl(in);
        requests.push_back(request);

        if (in.eof())
            throw Exception("Not enough results received for multi transaction", Error::ZMARSHALLINGERROR);
    }
}

bool ZooKeeperMultiRequest::isReadRequest() const
{
    /// Possibly we can do better
    return false;
}

void ZooKeeperMultiResponse::readImpl(ReadBuffer & in)
{
    for (auto & response : responses)
    {
        OpNum op_num;
        bool done;
        Error op_error;

        Coordination::read(op_num, in);
        Coordination::read(done, in);
        Coordination::read(op_error, in);

        if (done)
            throw Exception("Not enough results received for multi transaction", Error::ZMARSHALLINGERROR);

        /// op_num == -1 is special for multi transaction.
        /// For unknown reason, error code is duplicated in header and in response body.

        if (op_num == OpNum::Error)
            response = std::make_shared<ZooKeeperErrorResponse>();

        if (op_error != Error::ZOK)
        {
            response->error = op_error;

            /// Set error for whole transaction.
            /// If some operations fail, ZK send global error as zero and then send details about each operation.
            /// It will set error code for first failed operation and it will set special "runtime inconsistency" code for other operations.
            if (error == Error::ZOK && op_error != Error::ZRUNTIMEINCONSISTENCY)
                error = op_error;
        }

        if (op_error == Error::ZOK || op_num == OpNum::Error)
            dynamic_cast<ZooKeeperResponse &>(*response).readImpl(in);
    }

    /// Footer.
    {
        OpNum op_num;
        bool done;
        int32_t error_read;

        Coordination::read(op_num, in);
        Coordination::read(done, in);
        Coordination::read(error_read, in);

        if (!done)
            throw Exception("Too many results received for multi transaction", Error::ZMARSHALLINGERROR);
        if (op_num != OpNum::Error)
            throw Exception("Unexpected op_num received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
        if (error_read != -1)
            throw Exception("Unexpected error value received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
    }
}

void ZooKeeperMultiResponse::writeImpl(WriteBuffer & out) const
{
    for (const auto & response : responses)
    {
        const ZooKeeperResponse & zk_response = dynamic_cast<const ZooKeeperResponse &>(*response);
        OpNum op_num = zk_response.getOpNum();
        bool done = false;
        Error op_error = zk_response.error;

        Coordination::write(op_num, out);
        Coordination::write(done, out);
        Coordination::write(op_error, out);
        if (op_error == Error::ZOK || op_num == OpNum::Error)
            zk_response.writeImpl(out);
    }

    /// Footer.
    {
        OpNum op_num = OpNum::Error;
        bool done = true;
        int32_t error_read = - 1;

        Coordination::write(op_num, out);
        Coordination::write(done, out);
        Coordination::write(error_read, out);
    }
}

ZooKeeperResponsePtr ZooKeeperHeartbeatRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperHeartbeatResponse>()); }
ZooKeeperResponsePtr ZooKeeperSyncRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperSyncResponse>()); }
ZooKeeperResponsePtr ZooKeeperAuthRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperAuthResponse>()); }
ZooKeeperResponsePtr ZooKeeperCreateRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperCreateResponse>()); }
ZooKeeperResponsePtr ZooKeeperRemoveRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperRemoveResponse>()); }
ZooKeeperResponsePtr ZooKeeperExistsRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperExistsResponse>()); }
ZooKeeperResponsePtr ZooKeeperGetRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperGetResponse>()); }
ZooKeeperResponsePtr ZooKeeperSetRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperSetResponse>()); }
ZooKeeperResponsePtr ZooKeeperListRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperListResponse>()); }
ZooKeeperResponsePtr ZooKeeperCheckRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperCheckResponse>()); }
ZooKeeperResponsePtr ZooKeeperMultiRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperMultiResponse>(requests)); }
ZooKeeperResponsePtr ZooKeeperCloseRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperCloseResponse>()); }
ZooKeeperResponsePtr ZooKeeperSetACLRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperSetACLResponse>()); }
ZooKeeperResponsePtr ZooKeeperGetACLRequest::makeResponse() const { return setTime(std::make_shared<ZooKeeperGetACLResponse>()); }

void ZooKeeperSessionIDRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(internal_id, out);
    Coordination::write(session_timeout_ms, out);
    Coordination::write(server_id, out);
}

void ZooKeeperSessionIDRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(internal_id, in);
    Coordination::read(session_timeout_ms, in);
    Coordination::read(server_id, in);
}

Coordination::ZooKeeperResponsePtr ZooKeeperSessionIDRequest::makeResponse() const
{
    return std::make_shared<ZooKeeperSessionIDResponse>();
}

void ZooKeeperSessionIDResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(internal_id, in);
    Coordination::read(session_id, in);
    Coordination::read(server_id, in);
}

void ZooKeeperSessionIDResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(internal_id, out);
    Coordination::write(session_id, out);
    Coordination::write(server_id, out);
}


void ZooKeeperRequest::createLogElements(LogElements & elems) const
{
    elems.emplace_back();
    auto & elem =  elems.back();
    elem.xid = xid;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(getOpNum());
    elem.path = getPath();
    elem.request_idx = elems.size() - 1;
}


void ZooKeeperCreateRequest::createLogElements(LogElements & elems) const
{
    ZooKeeperRequest::createLogElements(elems);
    auto & elem =  elems.back();
    elem.data = data;
    elem.is_ephemeral = is_ephemeral;
    elem.is_sequential = is_sequential;
}

void ZooKeeperRemoveRequest::createLogElements(LogElements & elems) const
{
    ZooKeeperRequest::createLogElements(elems);
    auto & elem =  elems.back();
    elem.version = version;
}

void ZooKeeperSetRequest::createLogElements(LogElements & elems) const
{
    ZooKeeperRequest::createLogElements(elems);
    auto & elem =  elems.back();
    elem.data = data;
    elem.version = version;
}

void ZooKeeperCheckRequest::createLogElements(LogElements & elems) const
{
    ZooKeeperRequest::createLogElements(elems);
    auto & elem =  elems.back();
    elem.version = version;
}

void ZooKeeperMultiRequest::createLogElements(LogElements & elems) const
{
    ZooKeeperRequest::createLogElements(elems);
    elems.back().requests_size = requests.size();
    for (const auto & request : requests)
    {
        auto & req = dynamic_cast<ZooKeeperRequest &>(*request);
        assert(!req.xid || req.xid == xid);
        req.createLogElements(elems);
    }
}


void ZooKeeperResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    auto & elem =  elems[idx];
    assert(!elem.xid || elem.xid == xid);
    elem.xid = xid;
    int32_t response_op = tryGetOpNum();
    assert(!elem.op_num || elem.op_num == response_op || response_op < 0);
    elem.op_num = response_op;

    elem.zxid = zxid;
    elem.error = static_cast<Int32>(error);
}

void ZooKeeperWatchResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.watch_type = type;
    elem.watch_state = state;
    elem.path = path;
}

void ZooKeeperCreateResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.path_created = path_created;
}

void ZooKeeperExistsResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.stat = stat;
}

void ZooKeeperGetResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.data = data;
    elem.stat = stat;
}

void ZooKeeperSetResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.stat = stat;
}

void ZooKeeperListResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    ZooKeeperResponse::fillLogElements(elems, idx);
    auto & elem =  elems[idx];
    elem.stat = stat;
    elem.children = names;
}

void ZooKeeperMultiResponse::fillLogElements(LogElements & elems, size_t idx) const
{
    assert(idx == 0);
    assert(elems.size() == responses.size() + 1);
    ZooKeeperResponse::fillLogElements(elems, idx);
    for (const auto & response : responses)
    {
        auto & resp = dynamic_cast<ZooKeeperResponse &>(*response);
        assert(!resp.xid || resp.xid == xid);
        assert(!resp.zxid || resp.zxid == zxid);
        resp.xid = xid;
        resp.zxid = zxid;
        resp.fillLogElements(elems, ++idx);
    }
}


void ZooKeeperRequestFactory::registerRequest(OpNum op_num, Creator creator)
{
    if (!op_num_to_request.try_emplace(op_num, creator).second)
        throw Coordination::Exception("Request type " + toString(op_num) + " already registered", Coordination::Error::ZRUNTIMEINCONSISTENCY);
}

std::shared_ptr<ZooKeeperRequest> ZooKeeperRequest::read(ReadBuffer & in)
{
    XID xid;
    OpNum op_num;

    Coordination::read(xid, in);
    Coordination::read(op_num, in);

    auto request = ZooKeeperRequestFactory::instance().get(op_num);
    request->xid = xid;
    request->readImpl(in);
    return request;
}

ZooKeeperRequest::~ZooKeeperRequest()
{
    if (!request_created_time_ns)
        return;
    UInt64 elapsed_ns = clock_gettime_ns() - request_created_time_ns;
    constexpr UInt64 max_request_time_ns = 1000000000ULL; /// 1 sec
    if (max_request_time_ns < elapsed_ns)
    {
        LOG_TEST(&Poco::Logger::get(__PRETTY_FUNCTION__), "Processing of request xid={} took {} ms", xid, elapsed_ns / 1000000UL);
    }
}

ZooKeeperResponsePtr ZooKeeperRequest::setTime(ZooKeeperResponsePtr response) const
{
    if (request_created_time_ns)
    {
        response->response_created_time_ns = clock_gettime_ns();
    }
    return response;
}

ZooKeeperResponse::~ZooKeeperResponse()
{
    if (!response_created_time_ns)
        return;
    UInt64 elapsed_ns = clock_gettime_ns() - response_created_time_ns;
    constexpr UInt64 max_request_time_ns = 1000000000ULL; /// 1 sec
    if (max_request_time_ns < elapsed_ns)
    {
        LOG_TEST(&Poco::Logger::get(__PRETTY_FUNCTION__), "Processing of response xid={} took {} ms", xid, elapsed_ns / 1000000UL);
    }
}


ZooKeeperRequestPtr ZooKeeperRequestFactory::get(OpNum op_num) const
{
    auto it = op_num_to_request.find(op_num);
    if (it == op_num_to_request.end())
        throw Exception("Unknown operation type " + toString(op_num), Error::ZBADARGUMENTS);

    return it->second();
}

ZooKeeperRequestFactory & ZooKeeperRequestFactory::instance()
{
    static ZooKeeperRequestFactory factory;
    return factory;
}

template<OpNum num, typename RequestT>
void registerZooKeeperRequest(ZooKeeperRequestFactory & factory)
{
    factory.registerRequest(num, []
    {
        auto res = std::make_shared<RequestT>();
        res->request_created_time_ns = clock_gettime_ns();
        return res;
    });
}

ZooKeeperRequestFactory::ZooKeeperRequestFactory()
{
    registerZooKeeperRequest<OpNum::Heartbeat, ZooKeeperHeartbeatRequest>(*this);
    registerZooKeeperRequest<OpNum::Sync, ZooKeeperSyncRequest>(*this);
    registerZooKeeperRequest<OpNum::Auth, ZooKeeperAuthRequest>(*this);
    registerZooKeeperRequest<OpNum::Close, ZooKeeperCloseRequest>(*this);
    registerZooKeeperRequest<OpNum::Create, ZooKeeperCreateRequest>(*this);
    registerZooKeeperRequest<OpNum::Remove, ZooKeeperRemoveRequest>(*this);
    registerZooKeeperRequest<OpNum::Exists, ZooKeeperExistsRequest>(*this);
    registerZooKeeperRequest<OpNum::Get, ZooKeeperGetRequest>(*this);
    registerZooKeeperRequest<OpNum::Set, ZooKeeperSetRequest>(*this);
    registerZooKeeperRequest<OpNum::SimpleList, ZooKeeperSimpleListRequest>(*this);
    registerZooKeeperRequest<OpNum::List, ZooKeeperListRequest>(*this);
    registerZooKeeperRequest<OpNum::Check, ZooKeeperCheckRequest>(*this);
    registerZooKeeperRequest<OpNum::Multi, ZooKeeperMultiRequest>(*this);
    registerZooKeeperRequest<OpNum::SessionID, ZooKeeperSessionIDRequest>(*this);
    registerZooKeeperRequest<OpNum::GetACL, ZooKeeperGetACLRequest>(*this);
    registerZooKeeperRequest<OpNum::SetACL, ZooKeeperSetACLRequest>(*this);
}

}
