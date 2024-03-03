#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/HTTPConnectionPool.h>

#include <Poco/URI.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

#include <thread>
#include <gtest/gtest.h>

namespace
{

size_t stream_copy_n(std::istream & in, std::ostream & out, std::size_t count = std::numeric_limits<size_t>::max())
{
    const size_t buffer_size = 4096;
    char buffer[buffer_size];

    size_t total_read = 0;

    while (count > buffer_size)
    {
        in.read(buffer, buffer_size);
        size_t read = in.gcount();
        out.write(buffer, read);
        count -= read;
        total_read += read;

        if (read == 0)
            return total_read;
    }

    in.read(buffer, count);
    size_t read = in.gcount();
    out.write(buffer, read);
    total_read += read;

    return total_read;
}

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MockRequestHandler(std::shared_ptr<std::atomic<size_t>> slowdown_)
        : slowdown(std::move(slowdown_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        auto size = request.getContentLength();
        if (size > 0)
            response.setContentLength(size); // ContentLength is required for keep alive
        else
            response.setChunkedTransferEncoding(true); // or chunk encoding

        sleepForSeconds(*slowdown);

        stream_copy_n(request.stream(), response.send(), size);
    }

    std::shared_ptr<std::atomic<size_t>> slowdown;
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit HTTPRequestHandlerFactory(std::shared_ptr<std::atomic<size_t>> slowdown_)
        : slowdown(std::move(slowdown_))
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler(slowdown);
    }

    std::shared_ptr<std::atomic<size_t>> slowdown;
};

}

using HTTPSession = Poco::Net::HTTPClientSession;
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

class ConnectionPoolTest : public testing::Test {
protected:
    ConnectionPoolTest()
    {
        startServer();
    }

    void SetUp() override {
        timeouts = DB::ConnectionTimeouts();
        DB::HTTPConnectionPools::Limits def_limits{};
        DB::HTTPConnectionPools::instance().setLimits(def_limits, def_limits, def_limits);

        setSlowDown(0);

        DB::HTTPConnectionPools::instance().dropCache();
        DB::CurrentThread::getProfileEvents().reset();
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    DB::IHTTPConnectionPoolForEndpoint::Ptr getPool()
    {
        auto uri = Poco::URI(getServerUrl());
        return DB::HTTPConnectionPools::instance().getPool(DB::HTTPConnectionGroupType::HTTP, uri, DB::ProxyConfiguration{});
    }

    std::string getServerUrl() const
    {
        return "http://" + server_data.socket->address().toString();
    }

    void startServer()
    {
        server_data.reset();
        server_data.params = new Poco::Net::HTTPServerParams();
        server_data.socket = std::make_unique<Poco::Net::ServerSocket>(server_data.port);
        server_data.handler_factory = new HTTPRequestHandlerFactory(slowdown_receive);
        server_data.server =  std::make_unique<Poco::Net::HTTPServer>(
            server_data.handler_factory, *server_data.socket, server_data.params);

        server_data.server->start();
    }

    Poco::Net::HTTPServer & getServer() const
    {
        return *server_data.server;
    }

    void setSlowDown(size_t seconds)
    {
        *slowdown_receive = seconds;
    }

    DB::ConnectionTimeouts timeouts;
    std::shared_ptr<std::atomic<size_t>> slowdown_receive = std::make_shared<std::atomic<size_t>>(0);

    struct ServerData
    {
        // just some port to avoid collisions with others tests
        UInt16 port = 9871;
        Poco::Net::HTTPServerParams::Ptr params;
        std::unique_ptr<Poco::Net::ServerSocket> socket;
        HTTPRequestHandlerFactory::Ptr handler_factory;
        std::unique_ptr<Poco::Net::HTTPServer> server;

        ServerData() = default;
        ServerData(ServerData &&) = default;
        ServerData & operator =(ServerData &&) = delete;

        void reset()
        {
            if (server)
                server->stop();

            server = nullptr;
            handler_factory = nullptr;
            socket = nullptr;
            params = nullptr;
        }

        ~ServerData() {
            reset();
        }
    };

    ServerData server_data;
};


void wait_until(std::function<bool()> pred)
{
    while (!pred())
        sleepForMilliseconds(250);
}

void echoRequest(String data, HTTPSession & session)
{
    {
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", "HTTP/1.1"); // HTTP/1.1 is required for keep alive
        request.setContentLength(data.size());
        std::ostream & ostream = session.sendRequest(request);
        ostream << data;
    }

    {
        std::stringstream result;
        Poco::Net::HTTPResponse response;
        std::istream & istream = session.receiveResponse(response);
        ASSERT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_OK);

        stream_copy_n(istream, result);
        ASSERT_EQ(data, result.str());
    }
}

TEST_F(ConnectionPoolTest, CanConnect)
{
    auto pool = getPool();
    auto connection = pool->getConnection(timeouts);

    ASSERT_TRUE(connection->connected());
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);

    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
}

TEST_F(ConnectionPoolTest, CanRequest)
{
    auto pool = getPool();
    auto connection = pool->getConnection(timeouts);

    echoRequest("Hello", *connection);

    ASSERT_EQ(1, getServer().totalConnections());
    ASSERT_EQ(1, getServer().currentConnections());

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
}

TEST_F(ConnectionPoolTest, CanPreserve)
{
    auto pool = getPool();

    {
        auto connection = pool->getConnection(timeouts);
    }

    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
}

TEST_F(ConnectionPoolTest, CanReuse)
{
    auto pool = getPool();

    {
        auto connection = pool->getConnection(timeouts);
        // DB::setReuseTag(*connection);
    }

    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().stored_count));

    {
        auto connection = pool->getConnection(timeouts);

        ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().active_count));
        ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

        wait_until([&] () { return getServer().currentConnections() == 1; });
        ASSERT_EQ(1, getServer().currentConnections());

        echoRequest("Hello", *connection);

        ASSERT_EQ(1, getServer().totalConnections());
        ASSERT_EQ(1, getServer().currentConnections());

        connection->reset();
    }

    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
}

TEST_F(ConnectionPoolTest, CanReuse10)
{
    auto pool = getPool();


    for (int i = 0; i < 10; ++i)
    {
        auto connection = pool->getConnection(timeouts);
        echoRequest("Hello", *connection);
    }

    {
        auto connection = pool->getConnection(timeouts);
        connection->reset(); // reset just not to wait its expiration here
    }

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
}

TEST_F(ConnectionPoolTest, CanReuse5)
{
    timeouts.withHTTPKeepAliveTimeout(1);

    auto pool = getPool();

    std::vector<DB::HTTPSessionPtr> connections;
    connections.reserve(5);
    for (int i = 0; i < 5; ++i)
    {
        connections.push_back(pool->getConnection(timeouts));
    }
    connections.clear();

    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
    ASSERT_EQ(5, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(5, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 5; });
    ASSERT_EQ(5, getServer().currentConnections());
    ASSERT_EQ(5, getServer().totalConnections());

    for (int i = 0; i < 5; ++i)
    {
        auto connection = pool->getConnection(timeouts);
        echoRequest("Hello", *connection);
    }

    ASSERT_EQ(5, getServer().totalConnections());

    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
    ASSERT_EQ(5, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(5, CurrentMetrics::get(pool->getMetrics().stored_count));
}

TEST_F(ConnectionPoolTest, CanReconnectAndCreate)
{
    auto pool = getPool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection(timeouts);
        // DB::setReuseTag(*connection);
        in_use.push_back(connection);
    }

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);

    ASSERT_EQ(count, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

    auto connection = std::move(in_use.back());
    in_use.pop_back();

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, new connection

    echoRequest("Hello", *connection);

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(count+1, getServer().totalConnections());

    ASSERT_EQ(count+1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
}

TEST_F(ConnectionPoolTest, CanReconnectAndReuse)
{
    auto pool = getPool();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 2;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection(timeouts);
        // DB::setReuseTag(*connection);
        in_use.push_back(std::move(connection));
    }

    auto connection = std::move(in_use.back());
    in_use.pop_back();
    in_use.clear(); // other connection will be reused

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, reuse connection from pool

    echoRequest("Hello", *connection);

    connection->reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(2, getServer().totalConnections());

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
}

TEST_F(ConnectionPoolTest, ReceiveTimeout)
{
    setSlowDown(2);
    timeouts.withReceiveTimeout(1);

    auto pool = getPool();

    {
        auto connection = pool->getConnection(timeouts);
        ASSERT_ANY_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reset]);

    {
        timeouts.withReceiveTimeout(3);
        auto connection = pool->getConnection(timeouts);
        ASSERT_NO_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reset]);

    {
        /// timeouts have effect for reused session
        timeouts.withReceiveTimeout(1);
        auto connection = pool->getConnection(timeouts);
        ASSERT_ANY_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reused]);
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reset]);
}

TEST_F(ConnectionPoolTest, ReadWriteBufferFromHTTP)
{
    std::string_view message = "Hello ReadWriteBufferFromHTTP";
    auto uri = Poco::URI(getServerUrl());
    auto metrics = DB::HTTPConnectionPools::instance().getPool(DB::HTTPConnectionGroupType::HTTP, uri, DB::ProxyConfiguration{})->getMetrics();
    Poco::Net::HTTPBasicCredentials empty_creds;
    auto buf_from_http = DB::BuilderRWBufferFromHTTP(uri)
                             .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
                             .withOutCallback(
                                 [&] (std::ostream & in)
                                 {
                                     in << message;
                                 })
                             .withDelayInit(false)
                             .create(empty_creds);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));

    char buf[256];
    std::fill(buf, buf + sizeof(buf), 0);

    buf_from_http->readStrict(buf, message.size());
    ASSERT_EQ(std::string_view(buf), message);
    ASSERT_TRUE(buf_from_http->eof());

    buf_from_http.reset();

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, HardLimit)
{
    DB::HTTPConnectionPools::Limits zero_limits {0, 0, 0};
    DB::HTTPConnectionPools::instance().setLimits(zero_limits, zero_limits, zero_limits);

    auto pool = getPool();

    {
        auto connection = pool->getConnection(timeouts);
    }

    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));


    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[pool->getMetrics().preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().reset]);
}
