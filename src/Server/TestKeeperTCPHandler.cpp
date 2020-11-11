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

    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, *out);
    Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, *out);
    Coordination::write(Coordination::DEFAULT_SESSION_TIMEOUT, *out);
    Coordination::write(test_keeper_storage->getSessionID(), *out);
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

    while (true)
    {
        using namespace std::chrono_literals;
        while(!responses.empty())
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
                it->get()->write(*out);
                it = watch_responses.erase(it);
            }
            else
            {
                ++it;
            }
        }

        long poll_wait = responses.empty() ? session_timeout.totalMicroseconds() : 10000;

        if (in->poll(poll_wait))
        {
            bool close_received = receiveRequest();
            if (close_received)
            {
                LOG_DEBUG(log, "Received close request");
                break;
            }
        }
    }
}


bool TestKeeperTCPHandler::receiveRequest()
{
    int32_t length;
    Coordination::read(length, *in);
    int32_t xid;
    Coordination::read(xid, *in);

    Coordination::OpNum opnum;
    Coordination::read(opnum, *in);
    if (opnum == Coordination::OpNum::Close)
        return true;

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(*in);
    auto request_future_responses = test_keeper_storage->putRequest(request);
    responses.push(std::move(request_future_responses.response));
    if (request_future_responses.watch_response)
        watch_responses.emplace_back(std::move(*request_future_responses.watch_response));

    return false;
}


}
