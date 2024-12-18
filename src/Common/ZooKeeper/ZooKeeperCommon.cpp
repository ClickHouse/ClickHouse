#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Stopwatch.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <fmt/format.h>
#include <Common/logger_useful.h>


namespace Coordination
{

using namespace DB;

void ZooKeeperResponse::write(WriteBuffer & out) const
{
    auto response_size = Coordination::size(xid) + Coordination::size(zxid) + Coordination::size(error);
    if (error == Error::ZOK)
        response_size += sizeImpl();

    Coordination::write(static_cast<int32_t>(response_size), out);
    Coordination::write(xid, out);
    Coordination::write(zxid, out);
    Coordination::write(error, out);
    if (error == Error::ZOK)
        writeImpl(out);
}

std::string ZooKeeperRequest::toString(bool short_format) const
{
    return fmt::format(
        "XID = {}\n"
        "OpNum = {}\n"
        "Additional info:\n{}",
        xid,
        getOpNum(),
        toStringImpl(short_format));
}

void ZooKeeperRequest::write(WriteBuffer & out) const
{
    auto request_size = Coordination::size(xid) + Coordination::size(getOpNum()) + sizeImpl();

    Coordination::write(static_cast<int32_t>(request_size), out);
    Coordination::write(xid, out);
    Coordination::write(getOpNum(), out);
    writeImpl(out);
}

void ZooKeeperSyncRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
}

size_t ZooKeeperSyncRequest::sizeImpl() const
{
    return Coordination::size(path);
}

void ZooKeeperSyncRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
}

std::string ZooKeeperSyncRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}", path);
}

void ZooKeeperSyncResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
}

void ZooKeeperSyncResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
}

size_t ZooKeeperSyncResponse::sizeImpl() const
{
    return Coordination::size(path);
}

void ZooKeeperReconfigRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(joining, out);
    Coordination::write(leaving, out);
    Coordination::write(new_members, out);
    Coordination::write(version, out);
}

size_t ZooKeeperReconfigRequest::sizeImpl() const
{
    return Coordination::size(joining) + Coordination::size(leaving) + Coordination::size(new_members) + Coordination::size(version);
}

void ZooKeeperReconfigRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(joining, in);
    Coordination::read(leaving, in);
    Coordination::read(new_members, in);
    Coordination::read(version, in);
}

std::string ZooKeeperReconfigRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "joining = {}\nleaving = {}\nnew_members = {}\nversion = {}",
        joining, leaving, new_members, version);
}

void ZooKeeperReconfigResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(value, in);
    Coordination::read(stat, in);
}

void ZooKeeperReconfigResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(value, out);
    Coordination::write(stat, out);
}

size_t ZooKeeperReconfigResponse::sizeImpl() const
{
    return Coordination::size(value) + Coordination::size(stat);
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

size_t ZooKeeperWatchResponse::sizeImpl() const
{
    return Coordination::size(type) + Coordination::size(state) + Coordination::size(path);
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

size_t ZooKeeperAuthRequest::sizeImpl() const
{
    return Coordination::size(type) + Coordination::size(scheme) + Coordination::size(data);
}

void ZooKeeperAuthRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(type, in);
    Coordination::read(scheme, in);
    Coordination::read(data, in);
}

std::string ZooKeeperAuthRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "type = {}\n"
        "scheme = {}",
        type,
        scheme);
}

void ZooKeeperCreateRequest::writeImpl(WriteBuffer & out) const
{
    /// See https://github.com/ClickHouse/clickhouse-private/issues/3029
    if (path.starts_with("/clickhouse/tables/") && path.find("/parts/") != std::string::npos)
    {
        LOG_TRACE(getLogger(__PRETTY_FUNCTION__), "Creating part at path {}", path);
    }

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

size_t ZooKeeperCreateRequest::sizeImpl() const
{
    int32_t flags = 0;
    return Coordination::size(path) + Coordination::size(data) + Coordination::size(acls) + Coordination::size(flags);
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

std::string ZooKeeperCreateRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "path = {}\n"
        "is_ephemeral = {}\n"
        "is_sequential = {}",
        path,
        is_ephemeral,
        is_sequential);
}

void ZooKeeperCreateResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(path_created, in);
}

void ZooKeeperCreateResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path_created, out);
}

size_t ZooKeeperCreateResponse::sizeImpl() const
{
    return Coordination::size(path_created);
}

void ZooKeeperRemoveRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(version, out);
}

size_t ZooKeeperRemoveRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(version);
}

std::string ZooKeeperRemoveRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "path = {}\n"
        "version = {}",
        path,
        version);
}

void ZooKeeperRemoveRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(version, in);
}

void ZooKeeperRemoveRecursiveRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(remove_nodes_limit, out);
}

void ZooKeeperRemoveRecursiveRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(remove_nodes_limit, in);
}

size_t ZooKeeperRemoveRecursiveRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(remove_nodes_limit);
}

std::string ZooKeeperRemoveRecursiveRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "path = {}\n"
        "remove_nodes_limit = {}",
        path,
        remove_nodes_limit);
}

void ZooKeeperExistsRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

size_t ZooKeeperExistsRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(has_watch);
}

void ZooKeeperExistsRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

std::string ZooKeeperExistsRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}", path);
}

void ZooKeeperExistsResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(stat, in);
}

void ZooKeeperExistsResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

size_t ZooKeeperExistsResponse::sizeImpl() const
{
    return Coordination::size(stat);
}

void ZooKeeperGetRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

size_t ZooKeeperGetRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(has_watch);
}

void ZooKeeperGetRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

std::string ZooKeeperGetRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}", path);
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

size_t ZooKeeperGetResponse::sizeImpl() const
{
    return Coordination::size(data) + Coordination::size(stat);
}

void ZooKeeperSetRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(data, out);
    Coordination::write(version, out);
}

size_t ZooKeeperSetRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(data) + Coordination::size(version);
}

void ZooKeeperSetRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(data, in);
    Coordination::read(version, in);
}

std::string ZooKeeperSetRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
        "path = {}\n"
        "version = {}",
        path,
        version);
}

void ZooKeeperSetResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(stat, in);
}

void ZooKeeperSetResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

size_t ZooKeeperSetResponse::sizeImpl() const
{
    return Coordination::size(stat);
}

void ZooKeeperListRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
}

size_t ZooKeeperListRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(has_watch);
}

void ZooKeeperListRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);
}

std::string ZooKeeperListRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}", path);
}

void ZooKeeperFilteredListRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(has_watch, out);
    Coordination::write(static_cast<uint8_t>(list_request_type), out);
}

size_t ZooKeeperFilteredListRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(has_watch) + Coordination::size(static_cast<uint8_t>(list_request_type));
}

void ZooKeeperFilteredListRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(has_watch, in);

    uint8_t read_request_type{0};
    Coordination::read(read_request_type, in);
    list_request_type = static_cast<ListRequestType>(read_request_type);
}

std::string ZooKeeperFilteredListRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format(
            "path = {}\n"
            "list_request_type = {}",
            path,
            list_request_type);
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

size_t ZooKeeperListResponse::sizeImpl() const
{
    return Coordination::size(names) + Coordination::size(stat);
}

void ZooKeeperSimpleListResponse::readImpl(ReadBuffer & in)
{
    Coordination::read(names, in);
}

void ZooKeeperSimpleListResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(names, out);
}

size_t ZooKeeperSimpleListResponse::sizeImpl() const
{
    return Coordination::size(names);
}

void ZooKeeperSetACLRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(path, out);
    Coordination::write(acls, out);
    Coordination::write(version, out);
}

size_t ZooKeeperSetACLRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(acls) + Coordination::size(version);
}

void ZooKeeperSetACLRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(acls, in);
    Coordination::read(version, in);
}

std::string ZooKeeperSetACLRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}\nversion = {}", path, version);
}

void ZooKeeperSetACLResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(stat, out);
}

size_t ZooKeeperSetACLResponse::sizeImpl() const
{
    return Coordination::size(stat);
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

size_t ZooKeeperGetACLRequest::sizeImpl() const
{
    return Coordination::size(path);
}

std::string ZooKeeperGetACLRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}", path);
}

void ZooKeeperGetACLResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(acl, out);
    Coordination::write(stat, out);
}

size_t ZooKeeperGetACLResponse::sizeImpl() const
{
    return Coordination::size(acl) + Coordination::size(stat);
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

size_t ZooKeeperCheckRequest::sizeImpl() const
{
    return Coordination::size(path) + Coordination::size(version);
}

void ZooKeeperCheckRequest::readImpl(ReadBuffer & in)
{
    Coordination::read(path, in);
    Coordination::read(version, in);
}

std::string ZooKeeperCheckRequest::toStringImpl(bool /*short_format*/) const
{
    return fmt::format("path = {}\nversion = {}", path, version);
}

void ZooKeeperErrorResponse::readImpl(ReadBuffer & in)
{
    Coordination::Error read_error;
    Coordination::read(read_error, in);

    if (read_error != error)
        throw Exception(Error::ZMARSHALLINGERROR, "Error code in ErrorResponse ({}) doesn't match error code in header ({})", read_error, error);
}

void ZooKeeperErrorResponse::writeImpl(WriteBuffer & out) const
{
    Coordination::write(error, out);
}

size_t ZooKeeperErrorResponse::sizeImpl() const
{
    return Coordination::size(error);
}

void ZooKeeperMultiRequest::checkOperationType(OperationType type)
{
    chassert(!operation_type.has_value() || *operation_type == type);
    operation_type = type;
}

OpNum ZooKeeperMultiRequest::getOpNum() const
{
    return !operation_type.has_value() || *operation_type == OperationType::Write ? OpNum::Multi : OpNum::MultiRead;
}

ZooKeeperMultiRequest::ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls)
    : ZooKeeperMultiRequest(std::span{generic_requests}, default_acls)
{}

ZooKeeperMultiRequest::ZooKeeperMultiRequest(std::span<const Coordination::RequestPtr> generic_requests, const ACLs & default_acls)
{
    /// Convert nested Requests to ZooKeeperRequests.
    /// Note that deep copy is required to avoid modifying path in presence of chroot prefix.
    requests.reserve(generic_requests.size());

    using enum OperationType;
    for (const auto & generic_request : generic_requests)
    {
        if (const auto * concrete_request_create = dynamic_cast<const CreateRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            auto create = std::make_shared<ZooKeeperCreateRequest>(*concrete_request_create);
            if (create->acls.empty())
                create->acls = default_acls;
            requests.push_back(create);
        }
        else if (const auto * concrete_request_remove = dynamic_cast<const RemoveRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<ZooKeeperRemoveRequest>(*concrete_request_remove));
        }
        else if (const auto * concrete_request_remove_recursive = dynamic_cast<const RemoveRecursiveRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<ZooKeeperRemoveRecursiveRequest>(*concrete_request_remove_recursive));
        }
        else if (const auto * concrete_request_set = dynamic_cast<const SetRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<ZooKeeperSetRequest>(*concrete_request_set));
        }
        else if (const auto * concrete_request_check = dynamic_cast<const CheckRequest *>(generic_request.get()))
        {
            checkOperationType(Write);
            requests.push_back(std::make_shared<ZooKeeperCheckRequest>(*concrete_request_check));
        }
        else if (const auto * concrete_request_get = dynamic_cast<const GetRequest *>(generic_request.get()))
        {
            checkOperationType(Read);
            requests.push_back(std::make_shared<ZooKeeperGetRequest>(*concrete_request_get));
        }
        else if (const auto * concrete_request_exists = dynamic_cast<const ExistsRequest *>(generic_request.get()))
        {
            checkOperationType(Read);
            requests.push_back(std::make_shared<ZooKeeperExistsRequest>(*concrete_request_exists));
        }
        else if (const auto * concrete_request_simple_list = dynamic_cast<const ZooKeeperSimpleListRequest *>(generic_request.get()))
        {
            checkOperationType(Read);
            requests.push_back(std::make_shared<ZooKeeperSimpleListRequest>(*concrete_request_simple_list));
        }
        else if (const auto * concrete_request_list = dynamic_cast<const ZooKeeperFilteredListRequest *>(generic_request.get()))
        {
            checkOperationType(Read);
            requests.push_back(std::make_shared<ZooKeeperFilteredListRequest>(*concrete_request_list));
        }
        else
            throw Exception::fromMessage(Error::ZBADARGUMENTS, "Illegal command as part of multi ZooKeeper request");
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

size_t ZooKeeperMultiRequest::sizeImpl() const
{
    size_t total_size = 0;
    for (const auto & request : requests)
    {
        const auto & zk_request = dynamic_cast<const ZooKeeperRequest &>(*request);

        bool done = false;
        int32_t error = -1;

        total_size
            += Coordination::size(zk_request.getOpNum()) + Coordination::size(done) + Coordination::size(error) + zk_request.sizeImpl();
    }

    OpNum op_num = OpNum::Error;
    bool done = true;
    int32_t error = -1;

    return total_size + Coordination::size(op_num) + Coordination::size(done) + Coordination::size(error);
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
                throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected op_num received at the end of results for multi transaction");
            if (error != -1)
                throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected error value received at the end of results for multi transaction");
            break;
        }

        ZooKeeperRequestPtr request = ZooKeeperRequestFactory::instance().get(op_num);
        request->readImpl(in);
        requests.push_back(request);

        if (in.eof())
            throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Not enough results received for multi transaction");
    }
}

std::string ZooKeeperMultiRequest::toStringImpl(bool short_format) const
{
    if (short_format)
        return fmt::format("Subrequests size = {}", requests.size());

    auto out = fmt::memory_buffer();
    for (const auto & request : requests)
    {
        const auto & zk_request = dynamic_cast<const ZooKeeperRequest &>(*request);
        fmt::format_to(std::back_inserter(out), "SubRequest\n{}\n", zk_request.toString());
    }
    return {out.data(), out.size()};
}

bool ZooKeeperMultiRequest::isReadRequest() const
{
    return getOpNum() == OpNum::MultiRead;
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
            throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Not enough results received for multi transaction");

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

        response->zxid = zxid;
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
            throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Too many results received for multi transaction");
        if (op_num != OpNum::Error)
            throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected op_num received at the end of results for multi transaction");
        if (error_read != -1)
            throw Exception::fromMessage(Error::ZMARSHALLINGERROR, "Unexpected error value received at the end of results for multi transaction");
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

size_t ZooKeeperMultiResponse::sizeImpl() const
{
    size_t total_size = 0;
    for (const auto & response : responses)
    {
        const ZooKeeperResponse & zk_response = dynamic_cast<const ZooKeeperResponse &>(*response);
        OpNum op_num = zk_response.getOpNum();
        bool done = false;
        Error op_error = zk_response.error;

        total_size += Coordination::size(op_num) + Coordination::size(done) + Coordination::size(op_error);
        if (op_error == Error::ZOK || op_num == OpNum::Error)
            total_size += zk_response.sizeImpl();
    }

    /// Footer.
    OpNum op_num = OpNum::Error;
    bool done = true;
    int32_t error_read = - 1;

    return total_size + Coordination::size(op_num) + Coordination::size(done) + Coordination::size(error_read);
}

ZooKeeperResponsePtr ZooKeeperHeartbeatRequest::makeResponse() const { return std::make_shared<ZooKeeperHeartbeatResponse>(); }
ZooKeeperResponsePtr ZooKeeperSyncRequest::makeResponse() const { return std::make_shared<ZooKeeperSyncResponse>(); }
ZooKeeperResponsePtr ZooKeeperAuthRequest::makeResponse() const { return std::make_shared<ZooKeeperAuthResponse>(); }
ZooKeeperResponsePtr ZooKeeperRemoveRequest::makeResponse() const { return std::make_shared<ZooKeeperRemoveResponse>(); }
ZooKeeperResponsePtr ZooKeeperRemoveRecursiveRequest::makeResponse() const { return std::make_shared<ZooKeeperRemoveRecursiveResponse>(); }
ZooKeeperResponsePtr ZooKeeperExistsRequest::makeResponse() const { return std::make_shared<ZooKeeperExistsResponse>(); }
ZooKeeperResponsePtr ZooKeeperGetRequest::makeResponse() const { return std::make_shared<ZooKeeperGetResponse>(); }
ZooKeeperResponsePtr ZooKeeperSetRequest::makeResponse() const { return std::make_shared<ZooKeeperSetResponse>(); }
ZooKeeperResponsePtr ZooKeeperReconfigRequest::makeResponse() const { return std::make_shared<ZooKeeperReconfigResponse>(); }
ZooKeeperResponsePtr ZooKeeperListRequest::makeResponse() const { return std::make_shared<ZooKeeperListResponse>(); }
ZooKeeperResponsePtr ZooKeeperSimpleListRequest::makeResponse() const { return std::make_shared<ZooKeeperSimpleListResponse>(); }

ZooKeeperResponsePtr ZooKeeperCreateRequest::makeResponse() const
{
    if (not_exists)
        return std::make_shared<ZooKeeperCreateIfNotExistsResponse>();
    return std::make_shared<ZooKeeperCreateResponse>();
}

ZooKeeperResponsePtr ZooKeeperCheckRequest::makeResponse() const
{
    if (not_exists)
        return std::make_shared<ZooKeeperCheckNotExistsResponse>();

    return std::make_shared<ZooKeeperCheckResponse>();
}

ZooKeeperResponsePtr ZooKeeperMultiRequest::makeResponse() const
{
    std::shared_ptr<ZooKeeperMultiResponse> response;
    if (getOpNum() == OpNum::Multi)
       response = std::make_shared<ZooKeeperMultiWriteResponse>(requests);
    else
       response = std::make_shared<ZooKeeperMultiReadResponse>(requests);

    return std::move(response);
}

ZooKeeperResponsePtr ZooKeeperCloseRequest::makeResponse() const { return std::make_shared<ZooKeeperCloseResponse>(); }
ZooKeeperResponsePtr ZooKeeperSetACLRequest::makeResponse() const { return std::make_shared<ZooKeeperSetACLResponse>(); }
ZooKeeperResponsePtr ZooKeeperGetACLRequest::makeResponse() const { return std::make_shared<ZooKeeperGetACLResponse>(); }

void ZooKeeperSessionIDRequest::writeImpl(WriteBuffer & out) const
{
    Coordination::write(internal_id, out);
    Coordination::write(session_timeout_ms, out);
    Coordination::write(server_id, out);
}

size_t ZooKeeperSessionIDRequest::sizeImpl() const
{
    return Coordination::size(internal_id) + Coordination::size(session_timeout_ms) + Coordination::size(server_id);
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

size_t ZooKeeperSessionIDResponse::sizeImpl() const
{
    return Coordination::size(internal_id) + Coordination::size(session_id) + Coordination::size(server_id);
}


void ZooKeeperRequest::createLogElements(LogElements & elems) const
{
    elems.emplace_back();
    auto & elem =  elems.back();
    elem.xid = xid;
    elem.has_watch = has_watch;
    elem.op_num = static_cast<uint32_t>(getOpNum());
    elem.path = getPath();
    elem.request_idx = static_cast<uint32_t>(elems.size() - 1);
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
    elems.back().requests_size = static_cast<uint32_t>(requests.size());
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

    [[maybe_unused]] const bool is_filtered_list = elem.op_num == static_cast<int32_t>(Coordination::OpNum::FilteredList)
        && response_op == static_cast<int32_t>(Coordination::OpNum::List);
    assert(!elem.op_num || elem.op_num == response_op || is_filtered_list || response_op < 0);
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
        throw Coordination::Exception(Coordination::Error::ZRUNTIMEINCONSISTENCY,
            "Request type {} already registered", op_num);
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
        throw Exception(Error::ZBADARGUMENTS, "Unknown operation type {}", op_num);

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

        if constexpr (num == OpNum::MultiRead)
            res->operation_type = ZooKeeperMultiRequest::OperationType::Read;
        else if constexpr (num == OpNum::Multi)
            res->operation_type = ZooKeeperMultiRequest::OperationType::Write;
        else if constexpr (num == OpNum::CheckNotExists || num == OpNum::CreateIfNotExists)
            res->not_exists = true;

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
    registerZooKeeperRequest<OpNum::Reconfig, ZooKeeperReconfigRequest>(*this);
    registerZooKeeperRequest<OpNum::Multi, ZooKeeperMultiRequest>(*this);
    registerZooKeeperRequest<OpNum::MultiRead, ZooKeeperMultiRequest>(*this);
    registerZooKeeperRequest<OpNum::CreateIfNotExists, ZooKeeperCreateRequest>(*this);
    registerZooKeeperRequest<OpNum::SessionID, ZooKeeperSessionIDRequest>(*this);
    registerZooKeeperRequest<OpNum::GetACL, ZooKeeperGetACLRequest>(*this);
    registerZooKeeperRequest<OpNum::SetACL, ZooKeeperSetACLRequest>(*this);
    registerZooKeeperRequest<OpNum::FilteredList, ZooKeeperFilteredListRequest>(*this);
    registerZooKeeperRequest<OpNum::CheckNotExists, ZooKeeperCheckRequest>(*this);
    registerZooKeeperRequest<OpNum::RemoveRecursive, ZooKeeperRemoveRecursiveRequest>(*this);
}

PathMatchResult matchPath(std::string_view path, std::string_view match_to)
{
    using enum PathMatchResult;

    if (path.ends_with('/'))
        path.remove_suffix(1);

    if (match_to.ends_with('/'))
        match_to.remove_suffix(1);

    auto [first_it, second_it] = std::mismatch(path.begin(), path.end(), match_to.begin(), match_to.end());

    if (second_it != match_to.end())
        return NOT_MATCH;

    if (first_it == path.end())
        return EXACT;

    return *first_it == '/' ? IS_CHILD : NOT_MATCH;
}

}
