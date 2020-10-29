#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <array>


namespace Coordination
{

using namespace DB;

/// ZooKeeper has 1 MB node size and serialization limit by default,
/// but it can be raised up, so we have a slightly larger limit on our side.
#define MAX_STRING_OR_ARRAY_SIZE (1 << 28)  /// 256 MiB

/// Assuming we are at little endian.

static void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

static void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

static void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

static void write(const String & s, WriteBuffer & out)
{
    write(int32_t(s.size()), out);
    out.write(s.data(), s.size());
}

template <size_t N> void write(std::array<char, N> s, WriteBuffer & out)
{
    write(int32_t(N), out);
    out.write(s.data(), N);
}

template <typename T> void write(const std::vector<T> & arr, WriteBuffer & out)
{
    write(int32_t(arr.size()), out);
    for (const auto & elem : arr)
        write(elem, out);
}

static void write(const ACL & acl, WriteBuffer & out)
{
    write(acl.permissions, out);
    write(acl.scheme, out);
    write(acl.id, out);
}

static void write(const Stat & stat, WriteBuffer & out)
{
    write(stat.czxid, out);
    write(stat.mzxid, out);
    write(stat.ctime, out);
    write(stat.mtime, out);
    write(stat.version, out);
    write(stat.cversion, out);
    write(stat.aversion, out);
    write(stat.ephemeralOwner, out);
    write(stat.dataLength, out);
    write(stat.numChildren, out);
    write(stat.pzxid, out);
}

static void write(const Error & x, WriteBuffer & out)
{
    write(static_cast<int32_t>(x), out);
}

static void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

static void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

static void read(Error & x, ReadBuffer & in)
{
    int32_t code;
    read(code, in);
    x = Error(code);
}

static void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

static void read(String & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        return;
    }

    if (size < 0)
        throw Exception("Negative size while reading string from ZooKeeper", Error::ZMARSHALLINGERROR);

    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large string size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);

    s.resize(size);
    in.read(s.data(), size);
}

template <size_t N> void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception("Unexpected array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    in.read(s.data(), N);
}

static void read(Stat & stat, ReadBuffer & in)
{
    read(stat.czxid, in);
    read(stat.mzxid, in);
    read(stat.ctime, in);
    read(stat.mtime, in);
    read(stat.version, in);
    read(stat.cversion, in);
    read(stat.aversion, in);
    read(stat.ephemeralOwner, in);
    read(stat.dataLength, in);
    read(stat.numChildren, in);
    read(stat.pzxid, in);
}

template <typename T> void read(std::vector<T> & arr, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size while reading array from ZooKeeper", Error::ZMARSHALLINGERROR);
    if (size > MAX_STRING_OR_ARRAY_SIZE)
        throw Exception("Too large array size while reading from ZooKeeper", Error::ZMARSHALLINGERROR);
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}

static void read(ACL & acl, ReadBuffer & in)
{
    read(acl.permissions, in);
    read(acl.scheme, in);
    read(acl.id, in);
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

    OpNum op_num = -1;
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
            if (op_num != -1)
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

        if (op_num == -1)
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

        if (op_error == Error::ZOK || op_num == -1)
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
        if (op_num != -1)
            throw Exception("Unexpected op_num received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
        if (error_read != -1)
            throw Exception("Unexpected error value received at the end of results for multi transaction", Error::ZMARSHALLINGERROR);
    }
}

void ZooKeeperMultiResponse::writeImpl(WriteBuffer & out) const
{
    for (auto & response : responses)
    {
        const ZooKeeperResponse & zk_response = dynamic_cast<const ZooKeeperResponse &>(*response);
        OpNum op_num = zk_response.getOpNum();
        bool done = false;
        Error op_error = zk_response.error;

        Coordination::write(op_num, out);
        Coordination::write(done, out);
        Coordination::write(op_error, out);
        zk_response.writeImpl(out);
    }

    /// Footer.
    {
        OpNum op_num = -1;
        bool done = true;
        int32_t error_read = - 1;

        Coordination::write(op_num, out);
        Coordination::write(done, out);
        Coordination::write(error_read, out);
    }
}

ZooKeeperResponsePtr ZooKeeperHeartbeatRequest::makeResponse() const { return std::make_shared<ZooKeeperHeartbeatResponse>(); }
ZooKeeperResponsePtr ZooKeeperAuthRequest::makeResponse() const { return std::make_shared<ZooKeeperAuthResponse>(); }
ZooKeeperResponsePtr ZooKeeperCreateRequest::makeResponse() const { return std::make_shared<ZooKeeperCreateResponse>(); }
ZooKeeperResponsePtr ZooKeeperRemoveRequest::makeResponse() const { return std::make_shared<ZooKeeperRemoveResponse>(); }
ZooKeeperResponsePtr ZooKeeperExistsRequest::makeResponse() const { return std::make_shared<ZooKeeperExistsResponse>(); }
ZooKeeperResponsePtr ZooKeeperGetRequest::makeResponse() const { return std::make_shared<ZooKeeperGetResponse>(); }
ZooKeeperResponsePtr ZooKeeperSetRequest::makeResponse() const { return std::make_shared<ZooKeeperSetResponse>(); }
ZooKeeperResponsePtr ZooKeeperListRequest::makeResponse() const { return std::make_shared<ZooKeeperListResponse>(); }
ZooKeeperResponsePtr ZooKeeperCheckRequest::makeResponse() const { return std::make_shared<ZooKeeperCheckResponse>(); }
ZooKeeperResponsePtr ZooKeeperMultiRequest::makeResponse() const { return std::make_shared<ZooKeeperMultiResponse>(requests); }
ZooKeeperResponsePtr ZooKeeperCloseRequest::makeResponse() const { return std::make_shared<ZooKeeperCloseResponse>(); }

void ZooKeeperRequestFactory::registerRequest(OpNum op_num, Creator creator)
{
    if (!op_num_to_request.try_emplace(op_num, creator).second)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Request with op num {} already registered", op_num);
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

ZooKeeperRequestPtr ZooKeeperRequestFactory::get(OpNum op_num) const
{
    auto it = op_num_to_request.find(op_num);
    if (it == op_num_to_request.end())
        throw Exception("Unknown operation type " + std::to_string(op_num), Error::ZBADARGUMENTS);

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
    factory.registerRequest(num, [] { return std::make_shared<RequestT>(); });
}

ZooKeeperRequestFactory::ZooKeeperRequestFactory()
{
    registerZooKeeperRequest<11, ZooKeeperHeartbeatRequest>(*this);
    registerZooKeeperRequest<100, ZooKeeperAuthRequest>(*this);
    registerZooKeeperRequest<-11, ZooKeeperCloseRequest>(*this);
    registerZooKeeperRequest<1, ZooKeeperCreateRequest>(*this);
    registerZooKeeperRequest<2, ZooKeeperRemoveRequest>(*this);
    registerZooKeeperRequest<3, ZooKeeperExistsRequest>(*this);
    registerZooKeeperRequest<4, ZooKeeperGetRequest>(*this);
    registerZooKeeperRequest<5, ZooKeeperSetRequest>(*this);
    registerZooKeeperRequest<12, ZooKeeperListRequest>(*this);
    registerZooKeeperRequest<13, ZooKeeperCheckRequest>(*this);
    registerZooKeeperRequest<14, ZooKeeperMultiRequest>(*this);
}
  
}
