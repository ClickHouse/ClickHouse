#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Interpreters/ZooKeeperLog.h>

#include <boost/noncopyable.hpp>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <map>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <cstdint>
#include <optional>
#include <functional>


namespace Coordination
{

using LogElements = std::vector<ZooKeeperLogElement>;

struct ZooKeeperResponse : virtual Response
{
    XID xid = 0;
    int64_t zxid = 0;

    UInt64 response_created_time_ns = 0;

    ZooKeeperResponse() = default;
    ZooKeeperResponse(const ZooKeeperResponse &) = default;
    ~ZooKeeperResponse() override;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void write(WriteBuffer & out) const;
    virtual OpNum getOpNum() const = 0;
    virtual void fillLogElements(LogElements & elems, size_t idx) const;
    virtual int32_t tryGetOpNum() const { return static_cast<int32_t>(getOpNum()); }
};

using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

/// Exposed in header file for some external code.
struct ZooKeeperRequest : virtual Request
{
    XID xid = 0;
    bool has_watch = false;
    /// If the request was not send and the error happens, we definitely sure, that it has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    bool restored_from_zookeeper_log = false;

    UInt64 request_created_time_ns = 0;
    UInt64 thread_id = 0;
    String query_id;

    ZooKeeperRequest() = default;
    ZooKeeperRequest(const ZooKeeperRequest &) = default;
    ~ZooKeeperRequest() override;

    virtual OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out) const;
    std::string toString() const;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    virtual std::string toStringImpl() const { return ""; }

    static std::shared_ptr<ZooKeeperRequest> read(ReadBuffer & in);

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
    ZooKeeperResponsePtr setTime(ZooKeeperResponsePtr response) const;
    virtual bool isReadRequest() const = 0;

    virtual void createLogElements(LogElements & elems) const;
};

using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

struct ZooKeeperSyncRequest final : ZooKeeperRequest
{
    String path;
    String getPath() const override { return path; }
    OpNum getOpNum() const override { return OpNum::Sync; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return ZooKeeperRequest::bytesSize() + path.size(); }
};

struct ZooKeeperSyncResponse final : SyncResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Sync; }
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
};

struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    void write(WriteBuffer & out) const override;

    OpNum getOpNum() const override
    {
        chassert(false);
        throw Exception("OpNum for watch response doesn't exist", Error::ZRUNTIMEINCONSISTENCY);
    }

    void fillLogElements(LogElements & elems, size_t idx) const override;
    int32_t tryGetOpNum() const override { return 0; }
};

struct ZooKeeperAuthRequest final : ZooKeeperRequest
{
    int32_t type = 0;   /// ignored by the server
    String scheme;
    String data;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Auth; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return ZooKeeperRequest::bytesSize() + sizeof(xid) +
            sizeof(type) + scheme.size() + data.size(); }
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return OpNum::Auth; }

    size_t bytesSize() const override { return ZooKeeperResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Close; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override
    {
        throw Exception("Received response for close request", Error::ZRUNTIMEINCONSISTENCY);
    }

    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return OpNum::Close; }
};

struct ZooKeeperCreateRequest final : public CreateRequest, ZooKeeperRequest
{
    /// used only during restore from zookeeper log
    int32_t parent_cversion = -1;

    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Create; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return CreateRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Create; }

    size_t bytesSize() const override { return CreateResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Remove; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return RemoveRequest::bytesSize() + sizeof(xid); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Remove; }

    size_t bytesSize() const override { return RemoveResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Exists; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return ExistsRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Exists; }

    size_t bytesSize() const override { return ExistsResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Get; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return GetRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Get; }

    size_t bytesSize() const override { return GetResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() = default;
    explicit ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Set; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return SetRequest::bytesSize() + sizeof(xid); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Set; }

    size_t bytesSize() const override { return SetResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperListRequest : ListRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::List; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return ListRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperSimpleListRequest final : ZooKeeperListRequest
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
};

struct ZooKeeperFilteredListRequest final : ZooKeeperListRequest
{
    ListRequestType list_request_type{ListRequestType::ALL};

    OpNum getOpNum() const override { return OpNum::FilteredList; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    size_t bytesSize() const override { return ZooKeeperListRequest::bytesSize() + sizeof(list_request_type); }
};

struct ZooKeeperListResponse : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::List; }

    size_t bytesSize() const override { return ListResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperSimpleListResponse final : ZooKeeperListResponse
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
};

struct ZooKeeperCheckRequest final : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() = default;
    explicit ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Check; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return CheckRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperCheckResponse final : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return OpNum::Check; }

    size_t bytesSize() const override { return CheckResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Error; }

    size_t bytesSize() const override { return ErrorResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperSetACLRequest final : SetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::SetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return SetACLRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperSetACLResponse final : SetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::SetACL; }

    size_t bytesSize() const override { return SetACLResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperGetACLRequest final : GetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::GetACL; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return GetACLRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperGetACLResponse final : GetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::GetACL; }

    size_t bytesSize() const override { return GetACLResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Multi; }
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl() const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override;

    size_t bytesSize() const override { return MultiRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperMultiResponse final : MultiResponse, ZooKeeperResponse
{
    OpNum getOpNum() const override { return OpNum::Multi; }

    explicit ZooKeeperMultiResponse(const Requests & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(dynamic_cast<const ZooKeeperRequest &>(*request).makeResponse());
    }

    explicit ZooKeeperMultiResponse(const Responses & responses_)
    {
        responses = responses_;
    }

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    size_t bytesSize() const override { return MultiResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

/// Fake internal coordination (keeper) response. Never received from client
/// and never send to client.
struct ZooKeeperSessionIDRequest final : ZooKeeperRequest
{
    int64_t internal_id;
    int64_t session_timeout_ms;
    /// Who requested this session
    int32_t server_id;

    Coordination::OpNum getOpNum() const override { return OpNum::SessionID; }
    String getPath() const override { return {}; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    Coordination::ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

/// Fake internal coordination (keeper) response. Never received from client
/// and never send to client.
struct ZooKeeperSessionIDResponse final : ZooKeeperResponse
{
    int64_t internal_id;
    int64_t session_id;
    /// Who requested this session
    int32_t server_id;

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    Coordination::OpNum getOpNum() const override { return OpNum::SessionID; }
};

class ZooKeeperRequestFactory final : private boost::noncopyable
{

public:
    using Creator = std::function<ZooKeeperRequestPtr()>;
    using OpNumToRequest = std::unordered_map<OpNum, Creator>;

    static ZooKeeperRequestFactory & instance();

    ZooKeeperRequestPtr get(OpNum op_num) const;

    void registerRequest(OpNum op_num, Creator creator);

private:
    OpNumToRequest op_num_to_request;

    ZooKeeperRequestFactory();
};

}
