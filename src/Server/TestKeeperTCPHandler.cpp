#include <Server/TestKeeperTCPHandler.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Poco/Net/NetException.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/NetException.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;

}

void TestKeeperTCPHandler::sendHandshake()
{
    session_id = test_keeper_storage->getSessionID();
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, *out);
    Coordination::write(Coordination::DEFAULT_SESSION_TIMEOUT_MS, *out);
    Coordination::write(session_id, *out);
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    Coordination::write(passwd, *out);
    out->next();
}

void TestKeeperTCPHandler::run()
{
    runImpl();
}

void TestKeeperTCPHandler::receiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout;
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    std::array<char, Coordination::PASSWORD_LENGTH> passwd {};

    Coordination::read(handshake_length, *in);
    if (handshake_length != Coordination::CLIENT_HANDSHAKE_LENGTH)
        throw Exception("Unexpected handshake length received: " + toString(handshake_length), ErrorCodes::LOGICAL_ERROR);

    Coordination::read(protocol_version, *in);

    if (protocol_version != Coordination::ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + toString(protocol_version), ErrorCodes::LOGICAL_ERROR);

    Coordination::read(last_zxid_seen, *in);

    if (last_zxid_seen != 0)
        throw Exception("Non zero last_zxid_seen is not supported", ErrorCodes::LOGICAL_ERROR);

    Coordination::read(timeout, *in);
    Coordination::read(previous_session_id, *in);

    if (previous_session_id != 0)
        throw Exception("Non zero previous session id is not supported", ErrorCodes::LOGICAL_ERROR);

    Coordination::read(passwd, *in);
}


void TestKeeperTCPHandler::runImpl()
{
    setThreadName("TstKprHandler");
    ThreadStatus thread_status;
    auto global_receive_timeout = global_context.getSettingsRef().receive_timeout;
    auto global_send_timeout = global_context.getSettingsRef().send_timeout;

    socket().setReceiveTimeout(global_receive_timeout);
    socket().setSendTimeout(global_send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    try
    {
        receiveHandshake();
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        LOG_WARNING(log, "Cannot receive handshake {}", e.displayText());
        return;
    }

    sendHandshake();
    session_stopwatch.start();

    while (true)
    {
        using namespace std::chrono_literals;
        while (!responses.empty())
        {
            if (responses.front().wait_for(10ms) != std::future_status::ready)
                break;

            auto response = responses.front().get();
            response->write(*out);
            responses.pop();
        }

        for (auto it = watch_responses.begin(); it != watch_responses.end();)
        {
            if (it->wait_for(0s) == std::future_status::ready)
            {
                auto response = it->get();
                if (response->error == Coordination::Error::ZOK)
                    response->write(*out);
                it = watch_responses.erase(it);
            }
            else
            {
                ++it;
            }
        }

        Int64 poll_wait = responses.empty() ? session_timeout.totalMicroseconds() - session_stopwatch.elapsedMicroseconds() : 10000;

        if (in->poll(poll_wait))
        {
            auto received_op = receiveRequest();
            if (received_op == Coordination::OpNum::Close)
            {
                LOG_DEBUG(log, "Received close request for session #{}", session_id);
                break;
            }
            else if (received_op == Coordination::OpNum::Heartbeat)
            {
                session_stopwatch.restart();
            }
        }

        if (session_stopwatch.elapsedMicroseconds() > static_cast<UInt64>(session_timeout.totalMicroseconds()))
        {
            LOG_DEBUG(log, "Session #{} expired", session_id);
            putCloseRequest();
            break;
        }
    }
}

void TestKeeperTCPHandler::putCloseRequest()
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    test_keeper_storage->putCloseRequest(request, session_id);
}

Coordination::OpNum TestKeeperTCPHandler::receiveRequest()
{
    int32_t length;
    Coordination::read(length, *in);
    int32_t xid;
    Coordination::read(xid, *in);

    Coordination::OpNum opnum;
    Coordination::read(opnum, *in);

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(*in);
    if (opnum != Coordination::OpNum::Close)
    {
        auto request_future_responses = test_keeper_storage->putRequest(request, session_id);
        responses.push(std::move(request_future_responses.response));
        if (request_future_responses.watch_response)
            watch_responses.emplace_back(std::move(*request_future_responses.watch_response));
    }
    else
    {
        test_keeper_storage->putCloseRequest(request, session_id);
    }

    return opnum;
}


}
