#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

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

struct ZooKeeperResponse : virtual Response
{
    XID xid = 0;
    int64_t zxid = 0;

    virtual ~ZooKeeperResponse() override = default;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void write(WriteBuffer & out) const;
    virtual OpNum getOpNum() const = 0;
};

using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

/// Exposed in header file for Yandex.Metrica code.
struct ZooKeeperRequest : virtual Request
{
    XID xid = 0;
    bool has_watch = false;
    /// If the request was not send and the error happens, we definitely sure, that it has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    ZooKeeperRequest() = default;
    ZooKeeperRequest(const ZooKeeperRequest &) = default;
    virtual ~ZooKeeperRequest() override = default;

    virtual OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out) const;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    static std::shared_ptr<ZooKeeperRequest> read(ReadBuffer & in);

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
    virtual bool isReadRequest() const = 0;
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
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return ZooKeeperRequest::bytesSize() + path.size(); }
};

struct ZooKeeperSyncResponse final : ZooKeeperResponse
{
    String path;
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Sync; }

    size_t bytesSize() const override { return path.size(); }
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
        throw Exception("OpNum for watch response doesn't exist", Error::ZRUNTIMEINCONSISTENCY);
    }
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
    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Create; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return CreateRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return OpNum::Create; }

    size_t bytesSize() const override { return CreateResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Remove; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return RemoveRequest::bytesSize() + sizeof(xid); }
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

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return !has_watch; }

    size_t bytesSize() const override { return ExistsRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Exists; }

    size_t bytesSize() const override { return ExistsResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Get; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return !has_watch; }

    size_t bytesSize() const override { return GetRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Get; }

    size_t bytesSize() const override { return GetResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() = default;
    explicit ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}

    OpNum getOpNum() const override { return OpNum::Set; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return false; }

    size_t bytesSize() const override { return SetRequest::bytesSize() + sizeof(xid); }
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::Set; }

    size_t bytesSize() const override { return SetResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
};

struct ZooKeeperListRequest : ListRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::List; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return !has_watch; }

    size_t bytesSize() const override { return ListRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
};

struct ZooKeeperSimpleListRequest final : ZooKeeperListRequest
{
    OpNum getOpNum() const override { return OpNum::SimpleList; }
};

struct ZooKeeperListResponse : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return OpNum::List; }

    size_t bytesSize() const override { return ListResponse::bytesSize() + sizeof(xid) + sizeof(zxid); }
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

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override { return !has_watch; }

    size_t bytesSize() const override { return CheckRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
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

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return OpNum::Multi; }
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
    bool isReadRequest() const override;

    size_t bytesSize() const override { return MultiRequest::bytesSize() + sizeof(xid) + sizeof(has_watch); }
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

private:
    ZooKeeperRequestFactory();
};

}
