#pragma once

#include <Common/ZooKeeper/IKeeper.h>

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

using XID = int32_t;
using OpNum = int32_t;


struct ZooKeeperResponse : virtual Response
{
    virtual ~ZooKeeperResponse() override = default;
    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;
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
};

using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return 11; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return 11; }
};

struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    /// TODO FIXME alesap
    OpNum getOpNum() const override { return 0; }
};

struct ZooKeeperAuthRequest final : ZooKeeperRequest
{
    int32_t type = 0;   /// ignored by the server
    String scheme;
    String data;

    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return 100; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return 100; }
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    OpNum getOpNum() const override { return -11; }
    void writeImpl(WriteBuffer &) const override {}
    void readImpl(ReadBuffer &) override {}

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override
    {
        throw Exception("Received response for close request", Error::ZRUNTIMEINCONSISTENCY);
    }

    void writeImpl(WriteBuffer &) const override {}

    OpNum getOpNum() const override { return -11; }
};

struct ZooKeeperCreateRequest final : CreateRequest, ZooKeeperRequest
{
    ZooKeeperCreateRequest() = default;
    explicit ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    OpNum getOpNum() const override { return 1; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;

    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return 1; }
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() = default;
    explicit ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    OpNum getOpNum() const override { return 2; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return 2; }
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return 3; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return 3; }
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return 4; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return 4; }
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() = default;
    explicit ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}

    OpNum getOpNum() const override { return 5; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return 5; }
};

struct ZooKeeperListRequest final : ListRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return 12; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperListResponse final : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;
    OpNum getOpNum() const override { return 12; }
};

struct ZooKeeperCheckRequest final : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() = default;
    explicit ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}

    OpNum getOpNum() const override { return 13; }
    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCheckResponse final : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
    void writeImpl(WriteBuffer &) const override {}
    OpNum getOpNum() const override { return 13; }
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override;
    void writeImpl(WriteBuffer & out) const override;

    OpNum getOpNum() const override { return -1; }
};

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    OpNum getOpNum() const override { return 14; }
    ZooKeeperMultiRequest() = default;

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls);

    void writeImpl(WriteBuffer & out) const override;
    void readImpl(ReadBuffer & in) override;

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperMultiResponse final : MultiResponse, ZooKeeperResponse
{
    OpNum getOpNum() const override { return 14; }

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
