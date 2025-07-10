#pragma once

#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Interpreters/ZooKeeperLog.h>

#include <vector>
#include <memory>
#include <cstdint>
#include <optional>
#include <functional>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace Coordination
{

using LogElements = std::vector<ZooKeeperLogElement>;

struct ZooKeeperResponse : virtual Response
{
    XID xid = 0;

    ZooKeeperResponse() = default;
    ZooKeeperResponse(const ZooKeeperResponse &) = default;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual size_t sizeImpl() const = 0;
    virtual void write(WriteBuffer & out, bool use_xid_64) const;
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

    bool add_root_path = true;

    bool restored_from_zookeeper_log = false;

    UInt64 thread_id = 0;
    String query_id;

    ZooKeeperRequest() = default;
    ZooKeeperRequest(const ZooKeeperRequest &) = default;

    virtual OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out, bool use_xid_64) const;
    std::string toString(bool short_format = false) const;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual size_t sizeImpl() const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    virtual std::string toStringImpl(bool /*short_format*/) const { return ""; }

    static std::shared_ptr<ZooKeeperRequest> read(ReadBuffer & in);

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
    virtual bool isReadRequest() const = 0;

    virtual void createLogElements(LogElements & elems) const;
};

using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
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
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return ZooKeeperRequest::bytesSize() + path.size(); }
};

struct ZooKeeperSyncResponse final : SyncResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::Sync; }
};

struct ZooKeeperReconfigRequest final : ZooKeeperRequest
{
    String joining;
    String leaving;
    String new_members;
    int64_t version; // kazoo sends a 64bit integer in this request

    String getPath() const override { return keeper_config_path; }
    OpNum getOpNum() const override { return OpNum::Reconfig; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override
    {
        return ZooKeeperRequest::bytesSize() + joining.size() + leaving.size() + new_members.size()
            + sizeof(version);
    }
};

struct ZooKeeperReconfigResponse final : ReconfigResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::Reconfig; }
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
    OpNum getOpNum() const override { return OpNum::Heartbeat; }
};

struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;

    void write(WriteBuffer & out, bool use_xid_64) const override;

    OpNum getOpNum() const override;

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
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return ZooKeeperRequest::bytesSize() + sizeof(xid) +
            sizeof(type) + scheme.size() + data.size(); }
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }

    OpNum getOpNum() const override { return OpNum::Auth; }

    size_t bytesSize() const override { return ZooKeeperResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return OpNum::Close; }
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
    void readImpl(ReadBuffer &) override {}

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }

    OpNum getOpNum() const override { return OpNum::Close; }
};

struct ZooKeeperCreateRequest final : public CreateRequest, ZooKeeperRequest
{
    /// used only during restore from zookeeper log
    int32_t parent_cversion = -1;

    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    OpNum getOpNum() const override { return not_exists ? OpNum::CreateIfNotExists : OpNum::Create; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return CreateRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperCreateResponse : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;

    OpNum getOpNum() const override { return OpNum::Create; }

    size_t bytesSize() const override { return CreateResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperCreateIfNotExistsResponse : ZooKeeperCreateResponse
{
    OpNum getOpNum() const override { return OpNum::CreateIfNotExists; }
    using ZooKeeperCreateResponse::ZooKeeperCreateResponse;
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Remove; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return RemoveRequest::bytesSize() + sizeof(xid); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
    OpNum getOpNum() const override { return OpNum::Remove; }

    size_t bytesSize() const override { return RemoveResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperRemoveRecursiveRequest final : RemoveRecursiveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRecursiveRequest() = default;
    explicit ZooKeeperRemoveRecursiveRequest(const RemoveRecursiveRequest & base) : RemoveRecursiveRequest(base) {}

    OpNum getOpNum() const override { return OpNum::RemoveRecursive; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    size_t sizeImpl() const override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return RemoveRecursiveRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperRemoveRecursiveResponse : RemoveRecursiveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
    OpNum getOpNum() const override { return OpNum::RemoveRecursive; }

    size_t bytesSize() const override { return RemoveRecursiveResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    ZooKeeperExistsRequest() = default;
    explicit ZooKeeperExistsRequest(const ExistsRequest & base) : ExistsRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Exists; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return ExistsRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::Exists; }

    size_t bytesSize() const override { return ExistsResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    ZooKeeperGetRequest() = default;
    explicit ZooKeeperGetRequest(const GetRequest & base) : GetRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Get; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return GetRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
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
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return SetRequest::bytesSize() + sizeof(xid); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::Set; }

    size_t bytesSize() const override { return SetResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperListRequest : ListRequest, ZooKeeperRequest
{
    ZooKeeperListRequest() = default;
    explicit ZooKeeperListRequest(const ListRequest & base) : ListRequest(base) {}

    OpNum getOpNum() const override { return OpNum::List; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return ListRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperSimpleListRequest final : ZooKeeperListRequest
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperFilteredListRequest final : ZooKeeperListRequest
{
    ListRequestType list_request_type{ListRequestType::ALL};

    OpNum getOpNum() const override { return OpNum::FilteredList; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    size_t bytesSize() const override { return ZooKeeperListRequest::bytesSize() + sizeof(list_request_type); }
};

struct ZooKeeperListResponse : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::List; }

    size_t bytesSize() const override { return ListResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperSimpleListResponse final : ZooKeeperListResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::SimpleList; }

    size_t bytesSize() const override { return ZooKeeperListResponse::bytesSize() - sizeof(stat); }
};

struct ZooKeeperCheckRequest : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() = default;
    explicit ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}

    OpNum getOpNum() const override { return not_exists ? OpNum::CheckNotExists : OpNum::Check; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return CheckRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;
};

struct ZooKeeperCheckResponse : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    size_t sizeImpl() const override { return 0; }
    OpNum getOpNum() const override { return OpNum::Check; }

    size_t bytesSize() const override { return CheckResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperCheckNotExistsResponse : public ZooKeeperCheckResponse
{
    OpNum getOpNum() const override { return OpNum::CheckNotExists; }
    using ZooKeeperCheckResponse::ZooKeeperCheckResponse;
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;

    OpNum getOpNum() const override { return OpNum::Error; }

    size_t bytesSize() const override { return ErrorResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperSetACLRequest final : SetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::SetACL; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return SetACLRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperSetACLResponse final : SetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::SetACL; }

    size_t bytesSize() const override { return SetACLResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperGetACLRequest final : GetACLRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::GetACL; }
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;
    std::string toStringImpl(bool short_format) const override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return true; }

    size_t bytesSize() const override { return GetACLRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperGetACLResponse final : GetACLResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    OpNum getOpNum() const override { return OpNum::GetACL; }

    size_t bytesSize() const override { return GetACLResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperMultiRequest final : MultiRequest<ZooKeeperRequestPtr>, ZooKeeperRequest
{
    OpNum getOpNum() const override;
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);
    ZooKeeperMultiRequest(std::span<const Coordination::RequestPtr> generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;
    void readImpl(ReadBuffer & in) override;

    using RequestValidator = std::function<void(const ZooKeeperRequest &)>;
    void readImpl(ReadBuffer & in, RequestValidator request_validator);
    std::string toStringImpl(bool short_format) const override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override;

    size_t bytesSize() const override { return MultiRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }

    void createLogElements(LogElements & elems) const override;

    enum class OperationType : UInt8
    {
        Read,
        Write
    };

    std::optional<OperationType> operation_type;
private:
    void checkOperationType(OperationType type);
};

struct ZooKeeperMultiResponse : MultiResponse, ZooKeeperResponse
{
    ZooKeeperMultiResponse() = default;

    explicit ZooKeeperMultiResponse(const std::vector<ZooKeeperRequestPtr> & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(request->makeResponse());
    }

    explicit ZooKeeperMultiResponse(const Responses & responses_)
    {
        responses = responses_;
    }

    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;
    size_t sizeImpl() const override;

    size_t bytesSize() const override { return MultiResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }

    void fillLogElements(LogElements & elems, size_t idx) const override;
};

struct ZooKeeperMultiWriteResponse final : public ZooKeeperMultiResponse
{
    OpNum getOpNum() const override { return OpNum::Multi; }
    using ZooKeeperMultiResponse::ZooKeeperMultiResponse;
};

struct ZooKeeperMultiReadResponse final : public ZooKeeperMultiResponse
{
    OpNum getOpNum() const override { return OpNum::MultiRead; }
    using ZooKeeperMultiResponse::ZooKeeperMultiResponse;
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
    size_t sizeImpl() const override;
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
    size_t sizeImpl() const override;

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

enum class PathMatchResult : uint8_t
{
    NOT_MATCH,
    EXACT,
    IS_CHILD
};

PathMatchResult matchPath(std::string_view path, std::string_view match_to);

}
