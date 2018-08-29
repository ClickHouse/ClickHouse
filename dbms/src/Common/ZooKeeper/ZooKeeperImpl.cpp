#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <Poco/Exception.h>
#include <Poco/Net/NetException.h>

#include <array>


namespace ProfileEvents
{
    extern const Event ZooKeeperInit;
    extern const Event ZooKeeperTransactions;
    extern const Event ZooKeeperCreate;
    extern const Event ZooKeeperRemove;
    extern const Event ZooKeeperExists;
    extern const Event ZooKeeperMulti;
    extern const Event ZooKeeperGet;
    extern const Event ZooKeeperSet;
    extern const Event ZooKeeperList;
    extern const Event ZooKeeperCheck;
    extern const Event ZooKeeperClose;
    extern const Event ZooKeeperWaitMicroseconds;
    extern const Event ZooKeeperBytesSent;
    extern const Event ZooKeeperBytesReceived;
    extern const Event ZooKeeperWatchResponse;
}

namespace CurrentMetrics
{
    extern const Metric ZooKeeperRequest;
    extern const Metric ZooKeeperWatch;
}


/** ZooKeeper wire protocol.

Debugging example:
strace -t -f -e trace=network -s1000 -x ./clickhouse-zookeeper-cli localhost:2181

All numbers are in network byte order (big endian). Sizes are 32 bit. Numbers are signed.

zxid - incremental transaction number at server side.
xid - unique request number at client side.

Client connects to one of the specified hosts.
Client sends:

int32_t sizeof_connect_req;   \x00\x00\x00\x2c (44 bytes)

struct connect_req
{
    int32_t protocolVersion;  \x00\x00\x00\x00 (Currently zero)
    int64_t lastZxidSeen;     \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t timeOut;          \x00\x00\x75\x30 (Session timeout in milliseconds: 30000)
    int64_t sessionId;        \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t passwd_len;       \x00\x00\x00\x10 (16)
    char passwd[16];          \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
};

Server replies:

struct prime_struct
{
    int32_t len;              \x00\x00\x00\x24 (36 bytes)
    int32_t protocolVersion;  \x00\x00\x00\x00
    int32_t timeOut;          \x00\x00\x75\x30
    int64_t sessionId;        \x01\x62\x2c\x3d\x82\x43\x00\x27
    int32_t passwd_len;       \x00\x00\x00\x10
    char passwd[16];          \x3b\x8c\xe0\xd4\x1f\x34\xbc\x88\x9c\xa7\x68\x69\x78\x64\x98\xe9
};

Client remembers session id and session password.


Client may send authentication request (optional).


Each one third of timeout, client sends heartbeat:

int32_t length_of_heartbeat_request \x00\x00\x00\x08 (8)
int32_t ping_xid                    \xff\xff\xff\xfe (-2, constant)
int32_t ping_op                     \x00\x00\x00\x0b ZOO_PING_OP 11

Server replies:

int32_t length_of_heartbeat_response \x00\x00\x00\x10
int32_t ping_xid                     \xff\xff\xff\xfe
int64 zxid                           \x00\x00\x00\x00\x00\x01\x87\x98 (incremental server generated number)
int32_t err                          \x00\x00\x00\x00


Client sends requests. For example, create persistent node '/hello' with value 'world'.

int32_t request_length \x00\x00\x00\x3a
int32_t xid            \x5a\xad\x72\x3f      Arbitary number. Used for identification of requests/responses.
                                         libzookeeper uses unix timestamp for first xid and then autoincrement to that value.
int32_t op_num         \x00\x00\x00\x01      ZOO_CREATE_OP 1
int32_t path_length    \x00\x00\x00\x06
path                   \x2f\x68\x65\x6c\x6c\x6f  /hello
int32_t data_length    \x00\x00\x00\x05
data                   \x77\x6f\x72\x6c\x64      world
ACLs:
    int32_t num_acls   \x00\x00\x00\x01
    ACL:
        int32_t permissions \x00\x00\x00\x1f
        string scheme   \x00\x00\x00\x05
                        \x77\x6f\x72\x6c\x64      world
        string id       \x00\x00\x00\x06
                        \x61\x6e\x79\x6f\x6e\x65  anyone
int32_t flags           \x00\x00\x00\x00

Server replies:

int32_t response_length \x00\x00\x00\x1a
int32_t xid             \x5a\xad\x72\x3f
int64 zxid              \x00\x00\x00\x00\x00\x01\x87\x99
int32_t err             \x00\x00\x00\x00
string path_created     \x00\x00\x00\x06
                        \x2f\x68\x65\x6c\x6c\x6f  /hello - may differ to original path in case of sequential nodes.


Client may place a watch in their request.

For example, client sends "exists" request with watch:

request length \x00\x00\x00\x12
xid            \x5a\xae\xb2\x0d
op_num         \x00\x00\x00\x03
path           \x00\x00\x00\x05
               \x2f\x74\x65\x73\x74     /test
bool watch     \x01

Server will send response as usual.
And later, server may send special watch event.

struct WatcherEvent
{
    int32_t type;
    int32_t state;
    char * path;
};

response length    \x00\x00\x00\x21
special watch xid  \xff\xff\xff\xff
special watch zxid \xff\xff\xff\xff\xff\xff\xff\xff
err                \x00\x00\x00\x00
type               \x00\x00\x00\x02     DELETED_EVENT_DEF 2
state              \x00\x00\x00\x03     CONNECTED_STATE_DEF 3
path               \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74  /test


Example of multi request:

request length     \x00\x00\x00\x82 130
xid                \x5a\xae\xd6\x16
op_num             \x00\x00\x00\x0e 14

for every command:

    int32_t type;  \x00\x00\x00\x01 create
    bool done;     \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74 /test
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x31 multi1
    acl            \x00\x00\x00\x01
                   \x00\x00\x00\x1f
                   \x00\x00\x00\x05
                   \x77\x6f\x72\x6c\x64     world
                   \x00\x00\x00\x06
                   \x61\x6e\x79\x6f\x6e\x65 anyone
    flags          \x00\x00\x00\x00

    int32_t type;  \x00\x00\x00\x05 set
    bool done      \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x32 multi2
    version        \xff\xff\xff\xff

    int32_t type   \x00\x00\x00\x02 remove
    bool done      \x00
    int32_t err    \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    version        \xff\xff\xff\xff

after commands:

    int32_t type   \xff\xff\xff\xff -1
    bool done      \x01 true
    int32_t err    \xff\xff\xff\xff

Example of multi response:

response length    \x00\x00\x00\x81 129
xid                \x5a\xae\xd6\x16
zxid               \x00\x00\x00\x00\x00\x01\x87\xe1
err                \x00\x00\x00\x00

in a loop:

    type           \x00\x00\x00\x01 create
    done           \x00
    err            \x00\x00\x00\x00

    path_created   \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74

    type           \x00\x00\x00\x05 set
    done           \x00
    err            \x00\x00\x00\x00

    stat           \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x00\x01
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x00\x00\x00
                   \x00\x00\x00\x06
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x01\x87\xe1

    type           \x00\x00\x00\x02 remove
    done           \x00
    err            \x00\x00\x00\x00

after:

    type           \xff\xff\xff\xff
    done           \x01
    err            \xff\xff\xff\xff

  */


namespace Coordination
{

using namespace DB;


/// Assuming we are at little endian.

void write(int64_t x, WriteBuffer & out)
{
    x = __builtin_bswap64(x);
    writeBinary(x, out);
}

void write(int32_t x, WriteBuffer & out)
{
    x = __builtin_bswap32(x);
    writeBinary(x, out);
}

void write(bool x, WriteBuffer & out)
{
    writeBinary(x, out);
}

void write(const String & s, WriteBuffer & out)
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

void write(const ACL & acl, WriteBuffer & out)
{
    write(acl.permissions, out);
    write(acl.scheme, out);
    write(acl.id, out);
}


void read(int64_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap64(x);
}

void read(int32_t & x, ReadBuffer & in)
{
    readBinary(x, in);
    x = __builtin_bswap32(x);
}

void read(bool & x, ReadBuffer & in)
{
    readBinary(x, in);
}

void read(String & s, ReadBuffer & in)
{
    static constexpr int32_t max_string_size = 1 << 20;
    int32_t size = 0;
    read(size, in);

    if (size == -1)
    {
        /// It means that zookeeper node has NULL value. We will treat it like empty string.
        s.clear();
        return;
    }

    if (size < 0)
        throw Exception("Negative size while reading string from ZooKeeper", ZMARSHALLINGERROR);

    if (size > max_string_size)
        throw Exception("Too large string size while reading from ZooKeeper", ZMARSHALLINGERROR);

    s.resize(size);
    in.read(&s[0], size);
}

template <size_t N> void read(std::array<char, N> & s, ReadBuffer & in)
{
    int32_t size = 0;
    read(size, in);
    if (size != N)
        throw Exception("Unexpected array size while reading from ZooKeeper", ZMARSHALLINGERROR);
    in.read(&s[0], N);
}

void read(Stat & stat, ReadBuffer & in)
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
    static constexpr int32_t max_array_size = 1 << 20;
    int32_t size = 0;
    read(size, in);
    if (size < 0)
        throw Exception("Negative size while reading array from ZooKeeper", ZMARSHALLINGERROR);
    if (size > max_array_size)
        throw Exception("Too large array size while reading from ZooKeeper", ZMARSHALLINGERROR);
    arr.resize(size);
    for (auto & elem : arr)
        read(elem, in);
}


template <typename T>
void ZooKeeper::write(const T & x)
{
    Coordination::write(x, *out);
}

template <typename T>
void ZooKeeper::read(T & x)
{
    Coordination::read(x, *in);
}


struct ZooKeeperResponse;
using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;


struct ZooKeeperRequest : virtual Request
{
    ZooKeeper::XID xid = 0;
    bool has_watch = false;
    /// If the request was not send and the error happens, we definitely sure, that is has not been processed by the server.
    /// If the request was sent and we didn't get the response and the error happens, then we cannot be sure was it processed or not.
    bool probably_sent = false;

    virtual ~ZooKeeperRequest() {}

    virtual ZooKeeper::OpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out) const
    {
        /// Excessive copy to calculate length.
        WriteBufferFromOwnString buf;
        Coordination::write(xid, buf);
        Coordination::write(getOpNum(), buf);
        writeImpl(buf);
        Coordination::write(buf.str(), out);
        out.next();
    }

    virtual void writeImpl(WriteBuffer &) const = 0;

    virtual ZooKeeperResponsePtr makeResponse() const = 0;
};

struct ZooKeeperResponse : virtual Response
{
    virtual ~ZooKeeperResponse() {}
    virtual void readImpl(ReadBuffer &) = 0;
};


struct ZooKeeperHeartbeatRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    ZooKeeper::OpNum getOpNum() const override { return 11; }
    void writeImpl(WriteBuffer &) const override {}
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperHeartbeatResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
};

struct ZooKeeperWatchResponse final : WatchResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(type, in);
        Coordination::read(state, in);
        Coordination::read(path, in);
    }
};

struct ZooKeeperAuthRequest final : ZooKeeperRequest
{
    int32_t type = 0;   /// ignored by the server
    String scheme;
    String data;

    String getPath() const override { return {}; }
    ZooKeeper::OpNum getOpNum() const override { return 100; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(type, out);
        Coordination::write(scheme, out);
        Coordination::write(data, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperAuthResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
};

struct ZooKeeperCloseRequest final : ZooKeeperRequest
{
    String getPath() const override { return {}; }
    ZooKeeper::OpNum getOpNum() const override { return -11; }
    void writeImpl(WriteBuffer &) const override {}
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCloseResponse final : ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override
    {
        throw Exception("Received response for close request", ZRUNTIMEINCONSISTENCY);
    }
};

struct ZooKeeperCreateRequest final : CreateRequest, ZooKeeperRequest
{
    ZooKeeperCreateRequest() {}
    ZooKeeperCreateRequest(const CreateRequest & base) : CreateRequest(base) {}

    ZooKeeper::OpNum getOpNum() const override { return 1; }
    void writeImpl(WriteBuffer & out) const override
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
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCreateResponse final : CreateResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(path_created, in);
    }
};

struct ZooKeeperRemoveRequest final : RemoveRequest, ZooKeeperRequest
{
    ZooKeeperRemoveRequest() {}
    ZooKeeperRemoveRequest(const RemoveRequest & base) : RemoveRequest(base) {}

    ZooKeeper::OpNum getOpNum() const override { return 2; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(version, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperRemoveResponse final : RemoveResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
};

struct ZooKeeperExistsRequest final : ExistsRequest, ZooKeeperRequest
{
    ZooKeeper::OpNum getOpNum() const override { return 3; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(has_watch, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperExistsResponse final : ExistsResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(stat, in);
    }
};

struct ZooKeeperGetRequest final : GetRequest, ZooKeeperRequest
{
    ZooKeeper::OpNum getOpNum() const override { return 4; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(has_watch, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperGetResponse final : GetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(data, in);
        Coordination::read(stat, in);
    }
};

struct ZooKeeperSetRequest final : SetRequest, ZooKeeperRequest
{
    ZooKeeperSetRequest() {}
    ZooKeeperSetRequest(const SetRequest & base) : SetRequest(base) {}

    ZooKeeper::OpNum getOpNum() const override { return 5; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(data, out);
        Coordination::write(version, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperSetResponse final : SetResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(stat, in);
    }
};

struct ZooKeeperListRequest final : ListRequest, ZooKeeperRequest
{
    ZooKeeper::OpNum getOpNum() const override { return 12; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(has_watch, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperListResponse final : ListResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        Coordination::read(names, in);
        Coordination::read(stat, in);
    }
};

struct ZooKeeperCheckRequest final : CheckRequest, ZooKeeperRequest
{
    ZooKeeperCheckRequest() {}
    ZooKeeperCheckRequest(const CheckRequest & base) : CheckRequest(base) {}

    ZooKeeper::OpNum getOpNum() const override { return 13; }
    void writeImpl(WriteBuffer & out) const override
    {
        Coordination::write(path, out);
        Coordination::write(version, out);
    }
    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperCheckResponse final : CheckResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer &) override {}
};

/// This response may be received only as an element of responses in MultiResponse.
struct ZooKeeperErrorResponse final : ErrorResponse, ZooKeeperResponse
{
    void readImpl(ReadBuffer & in) override
    {
        int32_t read_error;
        Coordination::read(read_error, in);

        if (read_error != error)
            throw Exception("Error code in ErrorResponse (" + toString(read_error) + ") doesn't match error code in header (" + toString(error) + ")",
                ZMARSHALLINGERROR);
    }
};

struct ZooKeeperMultiRequest final : MultiRequest, ZooKeeperRequest
{
    ZooKeeper::OpNum getOpNum() const override { return 14; }

    ZooKeeperMultiRequest(const Requests & generic_requests, const ACLs & default_acls)
    {
        /// Convert nested Requests to ZooKeeperRequests.
        /// Note that deep copy is required to avoid modifying path in presence of chroot prefix.
        requests.reserve(generic_requests.size());

        for (const auto & generic_request : generic_requests)
        {
            if (auto * concrete_request = dynamic_cast<const CreateRequest *>(generic_request.get()))
            {
                auto create = std::make_shared<ZooKeeperCreateRequest>(*concrete_request);
                if (create->acls.empty())
                    create->acls = default_acls;
                requests.push_back(create);
            }
            else if (auto * concrete_request = dynamic_cast<const RemoveRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<ZooKeeperRemoveRequest>(*concrete_request));
            }
            else if (auto * concrete_request = dynamic_cast<const SetRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<ZooKeeperSetRequest>(*concrete_request));
            }
            else if (auto * concrete_request = dynamic_cast<const CheckRequest *>(generic_request.get()))
            {
                requests.push_back(std::make_shared<ZooKeeperCheckRequest>(*concrete_request));
            }
            else
                throw Exception("Illegal command as part of multi ZooKeeper request", ZBADARGUMENTS);
        }
    }

    void writeImpl(WriteBuffer & out) const override
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

        ZooKeeper::OpNum op_num = -1;
        bool done = true;
        int32_t error = -1;

        Coordination::write(op_num, out);
        Coordination::write(done, out);
        Coordination::write(error, out);
    }

    ZooKeeperResponsePtr makeResponse() const override;
};

struct ZooKeeperMultiResponse final : MultiResponse, ZooKeeperResponse
{
    ZooKeeperMultiResponse(const Requests & requests)
    {
        responses.reserve(requests.size());

        for (const auto & request : requests)
            responses.emplace_back(dynamic_cast<const ZooKeeperRequest &>(*request).makeResponse());
    }

    void readImpl(ReadBuffer & in) override
    {
        for (auto & response : responses)
        {
            ZooKeeper::OpNum op_num;
            bool done;
            int32_t op_error;

            Coordination::read(op_num, in);
            Coordination::read(done, in);
            Coordination::read(op_error, in);

            if (done)
                throw Exception("Not enough results received for multi transaction", ZMARSHALLINGERROR);

            /// op_num == -1 is special for multi transaction.
            /// For unknown reason, error code is duplicated in header and in response body.

            if (op_num == -1)
                response = std::make_shared<ZooKeeperErrorResponse>();

            if (op_error)
            {
                response->error = op_error;

                /// Set error for whole transaction.
                /// If some operations fail, ZK send global error as zero and then send details about each operation.
                /// It will set error code for first failed operation and it will set special "runtime inconsistency" code for other operations.
                if (!error && op_error != ZRUNTIMEINCONSISTENCY)
                    error = op_error;
            }

            if (!op_error || op_num == -1)
                dynamic_cast<ZooKeeperResponse &>(*response).readImpl(in);
        }

        /// Footer.
        {
            ZooKeeper::OpNum op_num;
            bool done;
            int32_t error;

            Coordination::read(op_num, in);
            Coordination::read(done, in);
            Coordination::read(error, in);

            if (!done)
                throw Exception("Too many results received for multi transaction", ZMARSHALLINGERROR);
            if (op_num != -1)
                throw Exception("Unexpected op_num received at the end of results for multi transaction", ZMARSHALLINGERROR);
            if (error != -1)
                throw Exception("Unexpected error value received at the end of results for multi transaction", ZMARSHALLINGERROR);
        }
    }
};


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


static constexpr int32_t protocol_version = 0;

static constexpr ZooKeeper::XID watch_xid = -1;
static constexpr ZooKeeper::XID ping_xid = -2;
static constexpr ZooKeeper::XID auth_xid = -4;

static constexpr ZooKeeper::XID close_xid = 0x7FFFFFFF;


ZooKeeper::~ZooKeeper()
{
    try
    {
        finalize(false, false);

        if (send_thread.joinable())
            send_thread.join();

        if (receive_thread.joinable())
            receive_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


ZooKeeper::ZooKeeper(
    const Addresses & addresses,
    const String & root_path_,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout,
    Poco::Timespan connection_timeout,
    Poco::Timespan operation_timeout)
    : root_path(root_path_),
    session_timeout(session_timeout),
    operation_timeout(std::min(operation_timeout, session_timeout))
{
    if (!root_path.empty())
    {
        if (root_path.back() == '/')
            root_path.pop_back();
    }

    if (auth_scheme.empty())
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        default_acls.emplace_back(std::move(acl));
    }
    else
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "auth";
        acl.id = "";
        default_acls.emplace_back(std::move(acl));
    }

    connect(addresses, connection_timeout);

    if (!auth_scheme.empty())
        sendAuth(auth_scheme, auth_data);

    send_thread = std::thread([this] { sendThread(); });
    receive_thread = std::thread([this] { receiveThread(); });

    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);
}


void ZooKeeper::connect(
    const Addresses & addresses,
    Poco::Timespan connection_timeout)
{
    if (addresses.empty())
        throw Exception("No addresses passed to ZooKeeper constructor", ZBADARGUMENTS);

    static constexpr size_t num_tries = 3;
    bool connected = false;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & address : addresses)
        {
            try
            {
                socket = Poco::Net::StreamSocket();     /// Reset the state of previous attempt.
                socket.connect(address, connection_timeout);

                socket.setReceiveTimeout(operation_timeout);
                socket.setSendTimeout(operation_timeout);
                socket.setNoDelay(true);

                in.emplace(socket);
                out.emplace(socket);

                sendHandshake();
                receiveHandshake();

                connected = true;
                break;
            }
            catch (const Poco::Net::NetException & e)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false) << ", " << address.toString();
            }
            catch (const Poco::TimeoutException & e)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false);
            }
        }

        if (connected)
            break;
    }

    if (!connected)
    {
        WriteBufferFromOwnString out;
        out << "All connection tries failed while connecting to ZooKeeper. Addresses: ";
        bool first = true;
        for (const auto & address : addresses)
        {
            if (first)
                first = false;
            else
                out << ", ";
            out << address.toString();
        }

        out << fail_reasons.str() << "\n";
        throw Exception(out.str(), ZCONNECTIONLOSS);
    }
}


void ZooKeeper::sendHandshake()
{
    int32_t handshake_length = 44;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t session_id = 0;
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    write(handshake_length);
    write(protocol_version);
    write(last_zxid_seen);
    write(timeout);
    write(session_id);
    write(passwd);

    out->next();
}


void ZooKeeper::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd;

    read(handshake_length);
    if (handshake_length != 36)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length), ZMARSHALLINGERROR);

    read(protocol_version_read);
    if (protocol_version_read != protocol_version)
        throw Exception("Unexpected protocol version: " + toString(protocol_version_read), ZMARSHALLINGERROR);

    read(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        /// Use timeout from server.
        session_timeout = timeout * Poco::Timespan::MILLISECONDS;

    read(session_id);
    read(passwd);
}


void ZooKeeper::sendAuth(const String & scheme, const String & data)
{
    ZooKeeperAuthRequest request;
    request.scheme = scheme;
    request.data = data;
    request.xid = auth_xid;
    request.write(*out);

    int32_t length;
    XID xid;
    int64_t zxid;
    int32_t err;

    read(length);
    size_t count_before_event = in->count();
    read(xid);
    read(zxid);
    read(err);

    if (xid != auth_xid)
        throw Exception("Unexpected event recieved in reply to auth request: " + toString(xid),
            ZMARSHALLINGERROR);

    int32_t actual_length = in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match. Expected: " + toString(length) + ", actual: " + toString(actual_length),
            ZMARSHALLINGERROR);

    if (err)
        throw Exception("Error received in reply to auth request. Code: " + toString(err) + ". Message: " + String(errorMessage(err)),
            ZMARSHALLINGERROR);
}


void ZooKeeper::sendThread()
{
    setThreadName("ZooKeeperSend");

    auto prev_heartbeat_time = clock::now();

    try
    {
        while (!expired)
        {
            auto prev_bytes_sent = out->count();

            auto now = clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count(),
                    operation_timeout.totalMilliseconds());

                RequestInfo info;
                if (requests_queue.tryPop(info, max_wait))
                {
                    /// After we popped element from the queue, we must register callbacks (even in the case when expired == true right now),
                    ///  because they must not be lost (callbacks must be called because the user will wait for them).

                    if (info.request->xid != close_xid)
                    {
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperRequest);
                        std::lock_guard lock(operations_mutex);
                        operations[info.request->xid] = info;
                    }

                    if (info.watch)
                    {
                        info.request->has_watch = true;
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperWatch);
                        std::lock_guard lock(watches_mutex);
                        watches[info.request->getPath()].emplace_back(std::move(info.watch));
                    }

                    if (expired)
                        break;

                    info.request->addRootPath(root_path);

                    info.request->probably_sent = true;
                    info.request->write(*out);

                    if (info.request->xid == close_xid)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                ZooKeeperHeartbeatRequest request;
                request.xid = ping_xid;
                request.write(*out);
            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesSent, out->count() - prev_bytes_sent);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize(true, false);
    }
}


void ZooKeeper::receiveThread()
{
    setThreadName("ZooKeeperRecv");

    try
    {
        Int64 waited = 0;
        while (!expired)
        {
            auto prev_bytes_received = in->count();

            clock::time_point now = clock::now();
            UInt64 max_wait = operation_timeout.totalMicroseconds();
            std::optional<RequestInfo> earliest_operation;

            {
                std::lock_guard lock(operations_mutex);
                if (!operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    earliest_operation = operations.begin()->second;
                    auto earliest_operation_deadline = earliest_operation->time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                    if (now > earliest_operation_deadline)
                        throw Exception("Operation timeout (deadline already expired) for path: " + earliest_operation->request->getPath(), ZOPERATIONTIMEOUT);
                    max_wait = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (in->poll(max_wait))
            {
                if (expired)
                    break;

                receiveEvent();
                waited = 0;
            }
            else
            {
                if (earliest_operation)
                    throw Exception("Operation timeout (no response) for path: " + earliest_operation->request->getPath(), ZOPERATIONTIMEOUT);
                waited += max_wait;
                if (waited >= session_timeout.totalMicroseconds())
                    throw Exception("Nothing is received in session timeout", ZOPERATIONTIMEOUT);

            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesReceived, in->count() - prev_bytes_received);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        finalize(false, true);
    }
}


void ZooKeeper::receiveEvent()
{
    int32_t length;
    XID xid;
    int64_t zxid;
    int32_t err;

    read(length);
    size_t count_before_event = in->count();
    read(xid);
    read(zxid);
    read(err);

    RequestInfo request_info;
    ZooKeeperResponsePtr response;

    if (xid == ping_xid)
    {
        if (err)
            throw Exception("Received error in heartbeat response: " + String(errorMessage(err)), ZRUNTIMEINCONSISTENCY);

        response = std::make_shared<ZooKeeperHeartbeatResponse>();
    }
    else if (xid == watch_xid)
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperWatchResponse);
        response = std::make_shared<ZooKeeperWatchResponse>();

        request_info.callback = [this](const Response & response)
        {
            const WatchResponse & watch_response = dynamic_cast<const WatchResponse &>(response);

            std::lock_guard lock(watches_mutex);

            auto it = watches.find(watch_response.path);
            if (it == watches.end())
            {
                /// This is Ok.
                /// Because watches are identified by path.
                /// And there may exist many watches for single path.
                /// And watch is added to the list of watches on client side
                ///  slightly before than it is registered by the server.
                /// And that's why new watch may be already fired by old event,
                ///  but then the server will actually register new watch
                ///  and will send event again later.
            }
            else
            {
                for (auto & callback : it->second)
                    if (callback)
                        callback(watch_response);   /// NOTE We may process callbacks not under mutex.

                CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, it->second.size());
                watches.erase(it);
            }
        };
    }
    else
    {
        {
            std::lock_guard lock(operations_mutex);

            auto it = operations.find(xid);
            if (it == operations.end())
                throw Exception("Received response for unknown xid", ZRUNTIMEINCONSISTENCY);

            /// After this point, we must invoke callback, that we've grabbed from 'operations'.
            /// Invariant: all callbacks are invoked either in case of success or in case of error.
            /// (all callbacks in 'operations' are guaranteed to be invoked)

            request_info = std::move(it->second);
            operations.erase(it);
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest);
        }

        auto elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();
        ProfileEvents::increment(ProfileEvents::ZooKeeperWaitMicroseconds, elapsed_microseconds);
    }

    try
    {
        if (!response)
            response = request_info.request->makeResponse();

        if (err)
            response->error = err;
        else
        {
            response->readImpl(*in);
            response->removeRootPath(root_path);
        }

        int32_t actual_length = in->count() - count_before_event;
        if (length != actual_length)
            throw Exception("Response length doesn't match. Expected: " + toString(length) + ", actual: " + toString(actual_length), ZMARSHALLINGERROR);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// Unrecoverable. Don't leave incorrect state in memory.
        if (!response)
            std::terminate();

        /// In case we cannot read the response, we should indicate it as the error of that type
        ///  when the user cannot assume whether the request was processed or not.
        response->error = ZCONNECTIONLOSS;
        if (request_info.callback)
            request_info.callback(*response);

        throw;
    }

    /// Exception in callback will propagate to receiveThread and will lead to session expiration. This is Ok.

    if (request_info.callback)
        request_info.callback(*response);
}


void ZooKeeper::finalize(bool error_send, bool error_receive)
{
    {
        std::lock_guard lock(push_request_mutex);

        if (expired)
            return;
        expired = true;
    }

    active_session_metric_increment.destroy();

    try
    {
        if (!error_send)
        {
            /// Send close event. This also signals sending thread to wakeup and then stop.
            try
            {
                close();
            }
            catch (...)
            {
                /// This happens for example, when "Cannot push request to queue within operation timeout".
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            send_thread.join();
        }

        try
        {
            /// This will also wakeup the receiving thread.
            socket.shutdown();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (!error_receive)
            receive_thread.join();

        {
            std::lock_guard lock(operations_mutex);

            for (auto & op : operations)
            {
                RequestInfo & request_info = op.second;
                ResponsePtr response = request_info.request->makeResponse();

                response->error = request_info.request->probably_sent
                    ? ZCONNECTIONLOSS
                    : ZSESSIONEXPIRED;

                if (request_info.callback)
                {
                    try
                    {
                        request_info.callback(*response);
                    }
                    catch (...)
                    {
                        /// We must continue to all other callbacks, because the user is waiting for them.
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest, operations.size());
            operations.clear();
        }

        {
            std::lock_guard lock(watches_mutex);

            for (auto & path_watches : watches)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = ZSESSIONEXPIRED;

                for (auto & callback : path_watches.second)
                {
                    if (callback)
                    {
                        try
                        {
                            callback(response);
                        }
                        catch (...)
                        {
                            tryLogCurrentException(__PRETTY_FUNCTION__);
                        }
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, watches.size());
            watches.clear();
        }

        /// Drain queue
        RequestInfo info;
        while (requests_queue.tryPop(info))
        {
            if (info.callback)
            {
                ResponsePtr response = info.request->makeResponse();
                response->error = ZSESSIONEXPIRED;
                try
                {
                    info.callback(*response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
            if (info.watch)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = ZSESSIONEXPIRED;
                try
                {
                    info.watch(response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void ZooKeeper::pushRequest(RequestInfo && info)
{
    try
    {
        info.time = clock::now();

        if (!info.request->xid)
        {
            info.request->xid = xid.fetch_add(1);
            if (info.request->xid < 0)
                throw Exception("XID overflow", ZSESSIONEXPIRED);
        }

        /// We must serialize 'pushRequest' and 'finalize' (from sendThread, receiveThread) calls
        ///  to avoid forgotten operations in the queue when session is expired.
        /// Invariant: when expired, no new operations will be pushed to the queue in 'pushRequest'
        ///  and the queue will be drained in 'finalize'.
        std::lock_guard lock(push_request_mutex);

        if (expired)
            throw Exception("Session expired", ZSESSIONEXPIRED);

        if (!requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
            throw Exception("Cannot push request to queue within operation timeout", ZOPERATIONTIMEOUT);
    }
    catch (...)
    {
        finalize(false, false);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
}


void ZooKeeper::create(
    const String & path,
    const String & data,
    bool is_ephemeral,
    bool is_sequential,
    const ACLs & acls,
    CreateCallback callback)
{
    ZooKeeperCreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;
    request.acls = acls.empty() ? default_acls : acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCreateRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
}


void ZooKeeper::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    ZooKeeperRemoveRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperRemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
}


void ZooKeeper::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallback watch)
{
    ZooKeeperExistsRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperExistsRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
}


void ZooKeeper::get(
    const String & path,
    GetCallback callback,
    WatchCallback watch)
{
    ZooKeeperGetRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperGetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
}


void ZooKeeper::set(
    const String & path,
    const String & data,
    int32_t version,
    SetCallback callback)
{
    ZooKeeperSetRequest request;
    request.path = path;
    request.data = data;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperSetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
}


void ZooKeeper::list(
    const String & path,
    ListCallback callback,
    WatchCallback watch)
{
    ZooKeeperListRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperListRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperList);
}


void ZooKeeper::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    ZooKeeperCheckRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCheckRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCheck);
}


void ZooKeeper::multi(
    const Requests & requests,
    MultiCallback callback)
{
    ZooKeeperMultiRequest request(requests, default_acls);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
}


void ZooKeeper::close()
{
    ZooKeeperCloseRequest request;
    request.xid = close_xid;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push close request to queue within operation timeout", ZOPERATIONTIMEOUT);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}


}
