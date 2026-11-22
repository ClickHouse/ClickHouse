#include "config.h"

#if USE_PULSAR && USE_SSL

#include <Storages/Pulsar/Client/Client.h>
#include <Storages/Pulsar/Client/Consumer.h>
#include <Storages/Pulsar/Client/Producer.h>
#include <Storages/Pulsar/Client/Protocol.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_ephemeral_certificate.h>
#include <base/unaligned.h>

#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>

#include <gtest/gtest.h>

#include <zstd.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>

#include <sys/socket.h>

using namespace DB;
using namespace DB::Pulsar;
using namespace std::chrono_literals;

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_PULSAR;
    extern const int LIMIT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace
{

using Type = PulsarProto::BaseCommand;

/// Only a guard against hangs: the tests synchronize with the broker by events.
constexpr auto HANG_GUARD = 30s;

String topicName(const String & name) { return "persistent://public/default/" + name; }

/// A broker on 127.0.0.1 with one thread per connection. It answers the requests of the client and records them,
/// and the test sends what else it wants.
class FakeBroker
{
public:
    struct Event
    {
        size_t connection = 0;
        /// The size of the frame after its size field.
        size_t frame_size = 0;
        PulsarProto::BaseCommand command;
        PulsarProto::MessageMetadata metadata;
        String payload;
    };

    enum class SendMode : uint8_t
    {
        Receipt,
        Error,
        Silent,
        DropConnectionOnce,
    };

    explicit FakeBroker(const EphemeralCert * cert = nullptr, String advertised_host_ = "127.0.0.1")
        : advertised_host(std::move(advertised_host_))
    {
        if (cert)
        {
            context = cert->makeContext(Poco::Net::Context::SERVER_USE);
            listener = std::make_unique<Poco::Net::SecureServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0), 64, context);
        }
        else
            listener = std::make_unique<Poco::Net::ServerSocket>(Poco::Net::SocketAddress("127.0.0.1", 0));
        tls = cert != nullptr;
        accept_thread = std::thread([this] { acceptLoop(); });
    }

    ~FakeBroker()
    {
        stopping = true;
        accept_thread.join();
        std::vector<std::thread> threads; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        {
            std::lock_guard lock(mutex);
            for (auto & connection : connections)
            {
                ::shutdown(connection->socket.impl()->sockfd(), SHUT_RDWR);
                threads.push_back(std::move(connection->thread));
            }
        }
        for (auto & thread : threads)
            thread.join();
    }

    UInt16 port() const { return listener->address().port(); }
    String url() const { return fmt::format("{}://{}:{}", tls ? "pulsar+ssl" : "pulsar", advertised_host, port()); }

    template <typename Predicate>
    bool waitFor(Predicate predicate)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, HANG_GUARD, [&] { return predicate(events); });
    }

    std::vector<Event> eventsOf(Type::Type type) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        std::lock_guard lock(mutex);
        std::vector<Event> res; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
        for (const auto & event : events)
            if (event.command.type() == type)
                res.push_back(event);
        return res;
    }

    /// In the order of receipt.
    std::vector<Event> allEvents() /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        std::lock_guard lock(mutex);
        return events;
    }

    size_t count(Type::Type type) { return eventsOf(type).size(); }

    bool waitForCount(Type::Type type, size_t n)
    {
        return waitFor([&](const auto & all) { return std::count_if(all.begin(), all.end(), [&](const Event & e) { return e.command.type() == type; }) >= static_cast<ptrdiff_t>(n); });
    }

    size_t acceptedConnections()
    {
        std::lock_guard lock(mutex);
        return connections.size();
    }

    void sendRaw(size_t connection, std::string_view bytes)
    {
        Connection * c = nullptr;
        {
            std::lock_guard lock(mutex);
            c = connections.at(connection).get();
        }
        std::lock_guard write_lock(c->write_mutex);
        size_t sent = 0;
        while (sent < bytes.size())
            sent += c->socket.sendBytes(bytes.data() + sent, static_cast<int>(bytes.size() - sent));
    }

    void sendCommand(size_t connection, const PulsarProto::BaseCommand & command)
    {
        auto frame = encodeCommand(command);
        sendRaw(connection, frame);
    }

    static StringWithMemoryTracking messageFrame(
        UInt64 consumer_id, UInt64 entry, const PulsarProto::MessageMetadata & metadata, std::string_view payload, UInt64 ledger = 1)
    {
        PulsarProto::BaseCommand command;
        command.set_type(Type::MESSAGE);
        command.mutable_message()->set_consumer_id(consumer_id);
        command.mutable_message()->mutable_message_id()->set_ledgerid(ledger);
        command.mutable_message()->mutable_message_id()->set_entryid(entry);
        return encodeSendFrame(command, metadata, payload);
    }

    void sendMessage(
        size_t connection, UInt64 consumer_id, UInt64 entry, const PulsarProto::MessageMetadata & metadata, std::string_view payload, UInt64 ledger = 1)
    {
        sendRaw(connection, messageFrame(consumer_id, entry, metadata, payload, ledger));
    }

    void closeConnection(size_t connection)
    {
        std::lock_guard lock(mutex);
        ::shutdown(connections.at(connection)->socket.impl()->sockfd(), SHUT_RDWR);
    }

    void setPartitions(const String & topic, UInt32 n)
    {
        std::lock_guard lock(config_mutex);
        partitions[topic] = n;
    }

    void setRedirect(std::optional<String> url)
    {
        std::lock_guard lock(config_mutex);
        redirect_to = std::move(url);
    }

    void setMaxMessageSize(Int32 size)
    {
        std::lock_guard lock(config_mutex);
        max_message_size = size;
    }

    /// Configuration, used by the connection threads.
    std::mutex config_mutex;
    std::map<String, UInt32> partitions;
    std::optional<String> redirect_to;
    std::atomic<bool> hold_lookup = false;
    std::atomic<bool> hold_subscribe = false;
    std::atomic<SendMode> send_mode = SendMode::Receipt;
    Int32 max_message_size = 5 * 1024 * 1024;

private:
    struct Connection
    {
        Poco::Net::StreamSocket socket;
        std::mutex write_mutex;
        std::thread thread;
    };

    void acceptLoop()
    {
        while (!stopping)
        {
            if (!listener->poll(Poco::Timespan(0, 50'000), Poco::Net::Socket::SELECT_READ))
                continue;
            Poco::Net::StreamSocket socket;
            try
            {
                socket = listener->acceptConnection();
            }
            catch (const Poco::Exception &)
            {
                /// A failed TLS handshake.
                continue;
            }
            std::lock_guard lock(mutex);
            auto & connection = connections.emplace_back(std::make_unique<Connection>());
            connection->socket = socket;
            size_t index = connections.size() - 1;
            connection->thread = std::thread([this, index, c = connection.get()] { connectionLoop(index, *c); });
        }
    }

    bool readExact(Poco::Net::StreamSocket & socket, char * data, size_t size)
    {
        size_t done = 0;
        while (done < size)
        {
            int n = 0;
            try
            {
                /// A TLS socket is locked during a read, so wait outside of it to let the test write meanwhile.
                if (socket.available() == 0 && !socket.poll(Poco::Timespan(0, 100'000), Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR))
                {
                    if (stopping)
                        return false;
                    continue;
                }
                n = socket.receiveBytes(data + done, static_cast<int>(size - done));
            }
            catch (const Poco::TimeoutException &)
            {
                if (stopping)
                    return false;
                continue;
            }
            catch (const Poco::Exception &)
            {
                return false;
            }
            if (n <= 0)
                return false;
            done += n;
        }
        return true;
    }

    void connectionLoop(size_t index, Connection & connection)
    {
        connection.socket.setReceiveTimeout(Poco::Timespan(0, 100'000));
        while (!stopping)
        {
            char header[4];
            if (!readExact(connection.socket, header, 4))
                return;
            size_t size = unalignedLoadBigEndian<UInt32>(header);
            String body(size, '\0');
            if (!readExact(connection.socket, body.data(), size))
                return;

            /// [command size][command][for SEND: 0x0e01, crc32c, metadata size, metadata, payload]
            size_t command_size = unalignedLoadBigEndian<UInt32>(body.data());
            Event event;
            event.connection = index;
            event.frame_size = size;
            EXPECT_TRUE(event.command.ParseFromArray(body.data() + 4, static_cast<int>(command_size)));
            if (event.command.type() == Type::SEND)
            {
                std::string_view tail = std::string_view(body).substr(4 + command_size);
                tail.remove_prefix(6);
                size_t metadata_size = unalignedLoadBigEndian<UInt32>(tail.data());
                EXPECT_TRUE(event.metadata.ParseFromArray(tail.data() + 4, static_cast<int>(metadata_size)));
                event.payload = String(tail.substr(4 + metadata_size));
            }
            {
                std::lock_guard lock(mutex);
                events.push_back(event);
            }
            cv.notify_all();
            try
            {
                reply(index, event);
            }
            catch (const Poco::Exception &)
            {
                return;
            }
        }
    }

    void reply(size_t index, const Event & event)
    {
        const auto & command = event.command;
        PulsarProto::BaseCommand response;
        switch (command.type())
        {
            case Type::CONNECT:
            {
                response.set_type(Type::CONNECTED);
                response.mutable_connected()->set_server_version("fake");
                response.mutable_connected()->set_protocol_version(19);
                std::lock_guard lock(config_mutex);
                response.mutable_connected()->set_max_message_size(max_message_size);
                break;
            }
            case Type::PING:
                response.set_type(Type::PONG);
                response.mutable_pong();
                break;
            case Type::PARTITIONED_METADATA:
            {
                response.set_type(Type::PARTITIONED_METADATA_RESPONSE);
                auto & metadata = *response.mutable_partitionmetadataresponse();
                metadata.set_request_id(command.partitionmetadata().request_id());
                metadata.set_response(PulsarProto::CommandPartitionedTopicMetadataResponse::Success);
                std::lock_guard lock(config_mutex);
                metadata.set_partitions(partitions.contains(command.partitionmetadata().topic()) ? partitions[command.partitionmetadata().topic()] : 0);
                break;
            }
            case Type::LOOKUP:
            {
                if (hold_lookup)
                    return;
                response.set_type(Type::LOOKUP_RESPONSE);
                auto & lookup = *response.mutable_lookuptopicresponse();
                lookup.set_request_id(command.lookuptopic().request_id());
                std::lock_guard lock(config_mutex);
                if (redirect_to && !command.lookuptopic().authoritative())
                {
                    lookup.set_response(PulsarProto::CommandLookupTopicResponse::Redirect);
                    lookup.set_brokerserviceurl(*redirect_to);
                    lookup.set_brokerserviceurltls(*redirect_to);
                    lookup.set_authoritative(true);
                }
                else
                {
                    lookup.set_response(PulsarProto::CommandLookupTopicResponse::Connect);
                    lookup.set_brokerserviceurl(url());
                    lookup.set_brokerserviceurltls(url());
                }
                break;
            }
            case Type::SUBSCRIBE:
                if (hold_subscribe)
                    return;
                response.set_type(Type::SUCCESS);
                response.mutable_success()->set_request_id(command.subscribe().request_id());
                break;
            case Type::PRODUCER:
                response.set_type(Type::PRODUCER_SUCCESS);
                response.mutable_producer_success()->set_request_id(command.producer().request_id());
                response.mutable_producer_success()->set_producer_name(command.producer().has_producer_name() ? command.producer().producer_name() : "fake-producer");
                response.mutable_producer_success()->set_last_sequence_id(-1);
                break;
            case Type::SEND:
                switch (send_mode.load())
                {
                    case SendMode::Receipt:
                        response.set_type(Type::SEND_RECEIPT);
                        response.mutable_send_receipt()->set_producer_id(command.send().producer_id());
                        response.mutable_send_receipt()->set_sequence_id(command.send().sequence_id());
                        break;
                    case SendMode::Error:
                        response.set_type(Type::SEND_ERROR);
                        response.mutable_send_error()->set_producer_id(command.send().producer_id());
                        response.mutable_send_error()->set_sequence_id(command.send().sequence_id());
                        response.mutable_send_error()->set_error(PulsarProto::PersistenceError);
                        response.mutable_send_error()->set_message("injected");
                        break;
                    case SendMode::Silent:
                        return;
                    case SendMode::DropConnectionOnce:
                        send_mode = SendMode::Receipt;
                        closeConnection(index);
                        return;
                }
                break;
            default:
                return;
        }
        sendCommand(index, response);
    }

    bool tls = false;
    const String advertised_host;
    Poco::Net::Context::Ptr context;
    std::unique_ptr<Poco::Net::ServerSocket> listener;
    std::atomic<bool> stopping = false;
    std::thread accept_thread;

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<std::unique_ptr<Connection>> connections; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<Event> events; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
};

PulsarProto::MessageMetadata metadataOf(const String & key = {})
{
    PulsarProto::MessageMetadata metadata;
    metadata.set_producer_name("p");
    metadata.set_sequence_id(0);
    metadata.set_publish_time(1);
    if (!key.empty())
        metadata.set_ordering_key(key);
    return metadata;
}

String batchEntry(const String & payload)
{
    PulsarProto::SingleMessageMetadata metadata;
    metadata.set_payload_size(static_cast<Int32>(payload.size()));
    String serialized;
    EXPECT_TRUE(metadata.SerializeToString(&serialized));
    String size(4, '\0');
    unalignedStoreBigEndian<UInt32>(size.data(), static_cast<UInt32>(serialized.size()));
    return size + serialized + payload;
}

ClientConfiguration configuration(std::vector<std::pair<String, UInt16>> * validated = nullptr) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    ClientConfiguration res;
    res.connection_timeout = 5000ms;
    res.operation_timeout = 10000ms;
    if (validated)
        res.connection_validator = [validated](const String & host, UInt16 port) { validated->emplace_back(host, port); };
    return res;
}

/// The SUBSCRIBE events: topic -> (connection, consumer id), the latest for each topic.
std::map<String, std::pair<size_t, UInt64>> subscriptions(FakeBroker & broker)
{
    std::map<String, std::pair<size_t, UInt64>> res;
    for (const auto & event : broker.eventsOf(Type::SUBSCRIBE))
        res[event.command.subscribe().topic()] = {event.connection, event.command.subscribe().consumer_id()};
    return res;
}

Messages receiveAtLeast(Consumer & consumer, size_t n)
{
    Messages res;
    const auto deadline = std::chrono::steady_clock::now() + HANG_GUARD;
    while (res.size() < n && std::chrono::steady_clock::now() < deadline)
        for (auto & message : consumer.batchReceive(n - res.size(), 100ms))
            res.push_back(std::move(message));
    return res;
}

int errorCode(auto && f)
{
    try
    {
        f();
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return 0;
}

}


TEST(PulsarClient, LookupRedirectAndValidator)
{
    FakeBroker b;
    FakeBroker a;
    a.setRedirect(b.url());

    std::vector<std::pair<String, UInt16>> validated; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    Client client(a.url(), configuration(&validated));
    auto consumer = client.subscribe({"t"}, "s", {});
    ASSERT_EQ(b.count(Type::SUBSCRIBE), 1);
    EXPECT_EQ(a.count(Type::SUBSCRIBE), 0);
    ASSERT_EQ(validated.size(), 2);
    EXPECT_EQ(validated[0].second, a.port());
    EXPECT_EQ(validated[1].second, b.port());

    /// A rejected host is never connected.
    FakeBroker c;
    a.setRedirect(c.url());
    auto config = configuration();
    config.connection_validator = [&](const String &, UInt16 port)
    {
        if (port == c.port())
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "rejected");
    };
    Client rejecting(a.url(), config);
    EXPECT_EQ(errorCode([&] { rejecting.subscribe({"t"}, "s", {}); }), ErrorCodes::LIMIT_EXCEEDED);
    EXPECT_EQ(c.acceptedConnections(), 0);
}

TEST(PulsarClient, PartitionsAndBatchReceive)
{
    FakeBroker broker;
    broker.setPartitions(topicName("t"), 3);
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.receiver_queue_size = 10;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);

    auto subs = subscriptions(broker);
    ASSERT_EQ(subs.size(), 3);
    ASSERT_TRUE(broker.waitForCount(Type::FLOW, 3));
    for (const auto & flow : broker.eventsOf(Type::FLOW))
        EXPECT_EQ(flow.command.flow().messagepermits(), 10);

    /// Idle: returns empty after the timeout.
    EXPECT_TRUE(consumer->batchReceive(10, 100ms).empty());

    /// Returns as soon as nothing more can arrive (each partition has 10 permits), long before the timeout.
    UInt64 entry = 1;
    for (const auto & [topic, sub] : subs)
        for (size_t i = 0; i < 10; ++i)
            broker.sendMessage(sub.first, sub.second, entry++, metadataOf(), topic);
    const auto start = std::chrono::steady_clock::now();
    auto messages = consumer->batchReceive(65409, 600s);
    EXPECT_LT(std::chrono::steady_clock::now() - start, HANG_GUARD);
    ASSERT_EQ(messages.size(), 30);

    std::map<String, size_t> per_topic;
    for (const auto & message : messages)
    {
        ++per_topic[message.topic()];
        EXPECT_EQ(message.payload(), message.topic());
    }
    for (const auto & [topic, sub] : subs)
        EXPECT_EQ(per_topic[topic], 10) << topic;

    /// The permits are returned, and a call returns at `max_messages`.
    ASSERT_TRUE(broker.waitForCount(Type::FLOW, 6));
    for (const auto & [topic, sub] : subs)
        for (size_t i = 0; i < 10; ++i)
            broker.sendMessage(sub.first, sub.second, entry++, metadataOf(), topic);
    EXPECT_EQ(consumer->batchReceive(5, 600s).size(), 5);
    EXPECT_EQ(receiveAtLeast(*consumer, 25).size(), 25);
}

TEST(PulsarClient, TooManyPartitions)
{
    FakeBroker broker;
    broker.setPartitions(topicName("t"), 70000);
    Client client(broker.url(), configuration());
    EXPECT_EQ(errorCode([&] { client.subscribe({"t"}, "s", {}); }), ErrorCodes::LIMIT_EXCEEDED);
    EXPECT_EQ(broker.count(Type::LOOKUP), 0);
    EXPECT_EQ(broker.count(Type::SUBSCRIBE), 0);
}

TEST(PulsarClient, Acknowledgements)
{
    FakeBroker broker;
    broker.setPartitions(topicName("t"), 3);
    Client client(broker.url(), configuration());
    auto consumer = client.subscribe({"t"}, "s", {});
    auto subs = subscriptions(broker);

    /// A batch of 3 on the first partition, and one message on each.
    const auto & [first_topic, first] = *subs.begin();
    auto metadata = metadataOf();
    metadata.set_num_messages_in_batch(3);
    broker.sendMessage(first.first, first.second, 1, metadata, batchEntry("a") + batchEntry("b") + batchEntry("c"));
    UInt64 entry = 10;
    for (const auto & [topic, sub] : subs)
        broker.sendMessage(sub.first, sub.second, entry++, metadataOf(), "x");

    auto messages = receiveAtLeast(*consumer, 6);
    ASSERT_EQ(messages.size(), 6);
    std::vector<MessageId> batch; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    std::vector<MessageId> singles; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (const auto & message : messages)
        (message.id().batchIndex() >= 0 ? batch : singles).push_back(message.id());
    ASSERT_EQ(batch.size(), 3);
    ASSERT_EQ(singles.size(), 3);

    /// The batch is acknowledged only when all its messages are.
    consumer->acknowledge(std::span(batch.data(), 2));
    consumer->acknowledge(singles);
    ASSERT_TRUE(broker.waitForCount(Type::ACK, 3));
    std::map<UInt64, size_t> acks_per_consumer;
    for (const auto & ack : broker.eventsOf(Type::ACK))
    {
        ++acks_per_consumer[ack.command.ack().consumer_id()];
        EXPECT_EQ(ack.command.ack().message_id_size(), 1);
        EXPECT_NE(ack.command.ack().message_id(0).entryid(), 1);
    }
    EXPECT_EQ(acks_per_consumer.size(), 3);
    consumer->acknowledge(std::span(batch.data() + 2, 1));
    ASSERT_TRUE(broker.waitForCount(Type::ACK, 4));
    auto acks = broker.eventsOf(Type::ACK);
    EXPECT_EQ(acks.back().command.ack().message_id(0).entryid(), 1);
    EXPECT_EQ(acks.back().command.ack().consumer_id(), first.second);
    EXPECT_FALSE(acks.back().command.ack().has_validation_error());

    broker.sendMessage(first.first, first.second, 20, metadataOf(), "y");
    auto more = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(more.size(), 1);
    consumer->negativeAcknowledge(more[0].id());
    ASSERT_TRUE(broker.waitForCount(Type::REDELIVER_UNACKNOWLEDGED_MESSAGES, 1));
    auto redeliver = broker.eventsOf(Type::REDELIVER_UNACKNOWLEDGED_MESSAGES)[0].command.redeliverunacknowledgedmessages();
    EXPECT_EQ(redeliver.consumer_id(), first.second);
    ASSERT_EQ(redeliver.message_ids_size(), 1);
    EXPECT_EQ(redeliver.message_ids(0).entryid(), 20);
}

TEST(PulsarClient, ChecksumMismatchIsDiscarded)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.receiver_queue_size = 1;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;
    ASSERT_TRUE(broker.waitForCount(Type::FLOW, 1));

    auto frame = FakeBroker::messageFrame(consumer_id, 1, metadataOf(), "payload");
    frame.back() ^= 1;
    broker.sendRaw(connection, frame);

    /// Not delivered, acknowledged with the error, and the permit is returned.
    Messages received;
    const auto start = std::chrono::steady_clock::now();
    while (broker.count(Type::FLOW) < 2 && std::chrono::steady_clock::now() - start < HANG_GUARD)
        for (auto & message : consumer->batchReceive(1, 100ms))
            received.push_back(std::move(message));
    EXPECT_TRUE(received.empty());
    auto acks = broker.eventsOf(Type::ACK);
    ASSERT_EQ(acks.size(), 1);
    EXPECT_EQ(acks[0].command.ack().validation_error(), PulsarProto::CommandAck::ChecksumMismatch);
    EXPECT_EQ(broker.count(Type::FLOW), 2);

    broker.sendMessage(connection, consumer_id, 2, metadataOf(), "good");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), "good");
}

TEST(PulsarClient, ProtocolViolations)
{
    {
        FakeBroker broker;
        broker.setMaxMessageSize(1000);
        Client client(broker.url(), configuration());
        auto consumer = client.subscribe({"t"}, "s", {});
        auto [connection, consumer_id] = subscriptions(broker).begin()->second;
        broker.sendMessage(connection, consumer_id, 1, metadataOf(), String(20000, 'x'));
        int code = 0;
        for (size_t i = 0; i < 100 && !code; ++i)
            code = errorCode([&] { consumer->batchReceive(1, 100ms); });
        EXPECT_EQ(code, ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
        /// Every later call throws too.
        EXPECT_NE(errorCode([&] { consumer->batchReceive(1, 100ms); }), 0);
    }
    {
        FakeBroker broker;
        Client client(broker.url(), configuration());
        ConsumerConfiguration consumer_configuration;
        consumer_configuration.receiver_queue_size = 1;
        auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
        auto [connection, consumer_id] = subscriptions(broker).begin()->second;
        /// One permit, two messages, in one write so that they arrive together.
        auto frames = FakeBroker::messageFrame(consumer_id, 1, metadataOf(), "a");
        frames += FakeBroker::messageFrame(consumer_id, 2, metadataOf(), "b");
        broker.sendRaw(connection, frames);
        int code = 0;
        for (size_t i = 0; i < 100 && !code; ++i)
            code = errorCode([&] { consumer->batchReceive(10, 100ms); });
        EXPECT_EQ(code, ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
    }
}

TEST(PulsarClient, ResubscribeAfterCloseConsumerAndLostConnection)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    auto consumer = client.subscribe({"t"}, "s", {});
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    broker.sendMessage(connection, consumer_id, 1, metadataOf(), "first");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);

    /// The broker answers `PING` itself, the client too.
    PulsarProto::BaseCommand ping;
    ping.set_type(Type::PING);
    ping.mutable_ping();
    broker.sendCommand(connection, ping);
    while (broker.count(Type::PONG) == 0)
        consumer->batchReceive(1, 100ms);

    /// Closed by the broker: subscribes again with a new consumer id. The acknowledgement waits for it.
    broker.hold_subscribe = true;
    PulsarProto::BaseCommand close;
    close.set_type(Type::CLOSE_CONSUMER);
    close.mutable_close_consumer()->set_consumer_id(consumer_id);
    close.mutable_close_consumer()->set_request_id(1000);
    broker.sendCommand(connection, close);
    while (broker.count(Type::SUBSCRIBE) < 2)
        EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
    consumer->acknowledge(std::span(&messages[0].id(), 1));
    EXPECT_EQ(broker.count(Type::ACK), 0);
    for (size_t i = 0; i < 5; ++i)
        EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
    EXPECT_EQ(broker.count(Type::SUBSCRIBE), 2);

    auto second = broker.eventsOf(Type::SUBSCRIBE)[1];
    EXPECT_NE(second.command.subscribe().consumer_id(), consumer_id);
    broker.hold_subscribe = false;
    PulsarProto::BaseCommand success;
    success.set_type(Type::SUCCESS);
    success.mutable_success()->set_request_id(second.command.subscribe().request_id());
    broker.sendCommand(second.connection, success);
    while (broker.count(Type::ACK) == 0)
        consumer->batchReceive(1, 100ms);
    auto ack = broker.eventsOf(Type::ACK)[0];
    EXPECT_EQ(ack.command.ack().consumer_id(), second.command.subscribe().consumer_id());
    EXPECT_EQ(ack.command.ack().message_id(0).entryid(), 1);

    /// The acknowledgement is sent before the permits of the new subscription.
    UInt64 new_consumer_id = second.command.subscribe().consumer_id();
    auto is_new_flow = [&](const FakeBroker::Event & e)
    {
        return e.connection == second.connection && e.command.type() == Type::FLOW && e.command.flow().consumer_id() == new_consumer_id;
    };
    auto is_ack = [&](const FakeBroker::Event & e) { return e.connection == second.connection && e.command.type() == Type::ACK; };
    const auto start = std::chrono::steady_clock::now();
    while (std::ranges::none_of(broker.allEvents(), is_new_flow) && std::chrono::steady_clock::now() - start < HANG_GUARD)
        consumer->batchReceive(1, 100ms);
    auto all = broker.allEvents();
    auto flow_position = std::ranges::find_if(all, is_new_flow) - all.begin();
    auto ack_position = std::ranges::find_if(all, is_ack) - all.begin();
    ASSERT_LT(flow_position, std::ssize(all));
    EXPECT_LT(ack_position, flow_position);

    broker.sendMessage(second.connection, new_consumer_id, 2, metadataOf(), "second");
    messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), "second");

    /// The connection is lost: a new connection, a new lookup and a new subscription.
    broker.closeConnection(second.connection);
    while (broker.count(Type::SUBSCRIBE) < 3)
        consumer->batchReceive(1, 100ms);
    auto third = broker.eventsOf(Type::SUBSCRIBE)[2];
    EXPECT_NE(third.connection, second.connection);

    /// A frame in three segments, each sent after a call returned without it.
    auto frame = FakeBroker::messageFrame(third.command.subscribe().consumer_id(), 3, metadataOf(), "segmented");
    const size_t part = frame.size() / 3;
    for (size_t i = 0; i < 3; ++i)
    {
        EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
        broker.sendRaw(third.connection, std::string_view(frame).substr(i * part, i == 2 ? String::npos : part));
    }
    messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), "segmented");
}

TEST(PulsarClient, HeldLookupDoesNotBlockCalls)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    auto consumer = client.subscribe({"t"}, "s", {});
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    broker.hold_lookup = true;
    PulsarProto::BaseCommand close;
    close.set_type(Type::CLOSE_CONSUMER);
    close.mutable_close_consumer()->set_consumer_id(consumer_id);
    close.mutable_close_consumer()->set_request_id(1000);
    broker.sendCommand(connection, close);
    while (broker.count(Type::LOOKUP) < 2)
        EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
    for (size_t i = 0; i < 5; ++i)
        EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
    EXPECT_EQ(broker.count(Type::LOOKUP), 2);
    EXPECT_EQ(broker.count(Type::SUBSCRIBE), 1);

    broker.hold_lookup = false;
    auto lookup = broker.eventsOf(Type::LOOKUP)[1];
    PulsarProto::BaseCommand response;
    response.set_type(Type::LOOKUP_RESPONSE);
    response.mutable_lookuptopicresponse()->set_request_id(lookup.command.lookuptopic().request_id());
    response.mutable_lookuptopicresponse()->set_response(PulsarProto::CommandLookupTopicResponse::Connect);
    response.mutable_lookuptopicresponse()->set_brokerserviceurl(broker.url());
    broker.sendCommand(lookup.connection, response);
    while (broker.count(Type::SUBSCRIBE) < 2)
        consumer->batchReceive(1, 100ms);
    auto second = broker.eventsOf(Type::SUBSCRIBE)[1];
    broker.sendMessage(second.connection, second.command.subscribe().consumer_id(), 1, metadataOf(), "after");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
}

TEST(PulsarClient, StepTimeout)
{
    FakeBroker broker;
    auto config = configuration();
    config.operation_timeout = 2000ms;
    Client client(broker.url(), config);
    auto consumer = client.subscribe({"t"}, "s", {});
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    broker.sendMessage(connection, consumer_id, 1, metadataOf(), "x");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);

    /// The subscription is never confirmed, while the connection stays alive.
    broker.hold_subscribe = true;
    PulsarProto::BaseCommand close;
    close.set_type(Type::CLOSE_CONSUMER);
    close.mutable_close_consumer()->set_consumer_id(consumer_id);
    close.mutable_close_consumer()->set_request_id(1000);
    broker.sendCommand(connection, close);
    int code = 0;
    const auto start = std::chrono::steady_clock::now();
    while (!code && std::chrono::steady_clock::now() - start < HANG_GUARD)
        code = errorCode([&] { consumer->batchReceive(1, 100ms); });
    EXPECT_EQ(code, ErrorCodes::CANNOT_CONNECT_PULSAR);
    EXPECT_GE(std::chrono::steady_clock::now() - start, 2000ms);
    EXPECT_NE(errorCode([&] { consumer->batchReceive(1, 100ms); }), 0);
    EXPECT_NE(errorCode([&] { consumer->acknowledge(std::span(&messages[0].id(), 1)); }), 0);
    /// Do not throw.
    consumer->negativeAcknowledge(messages[0].id());
    consumer->close();
}

TEST(PulsarClient, PartitionRefresh)
{
    FakeBroker broker;
    broker.setPartitions(topicName("t"), 3);
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.partitions_refresh_interval = 1000ms;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
    EXPECT_EQ(broker.count(Type::SUBSCRIBE), 3);

    broker.setPartitions(topicName("t"), 4);
    const auto start = std::chrono::steady_clock::now();
    while (broker.count(Type::SUBSCRIBE) < 4 && std::chrono::steady_clock::now() - start < HANG_GUARD)
        consumer->batchReceive(1, 100ms);
    auto subs = subscriptions(broker);
    ASSERT_EQ(subs.size(), 4);
    const auto & [connection, consumer_id] = subs.at(topicName("t") + "-partition-3");
    broker.sendMessage(connection, consumer_id, 1, metadataOf(), "new");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].topic(), topicName("t") + "-partition-3");
}

TEST(PulsarClient, TlsHostNameIsChecked)
{
    EphemeralCert cert;
    /// The certificate chain is accepted, so that only the host name check of the client is tested.
    Poco::Net::SSLManager::instance().defaultClientContext();
    Poco::Net::AcceptCertificateHandler accept_certificate(/* handleErrorsOnServerSide = */ false);

    {
        FakeBroker broker(&cert, "localhost");
        Client client(broker.url(), configuration());
        auto consumer = client.subscribe({"t"}, "s", {});
        auto [connection, consumer_id] = subscriptions(broker).begin()->second;
        broker.sendMessage(connection, consumer_id, 1, metadataOf(), "secure");
        auto messages = receiveAtLeast(*consumer, 1);
        ASSERT_EQ(messages.size(), 1);
        EXPECT_EQ(messages[0].payload(), "secure");
    }
    {
        /// The certificate is for `localhost`.
        FakeBroker broker(&cert, "127.0.0.1");
        Client client(broker.url(), configuration());
        EXPECT_EQ(errorCode([&] { client.subscribe({"t"}, "s", {}); }), ErrorCodes::CANNOT_CONNECT_PULSAR);
        EXPECT_EQ(broker.count(Type::CONNECT), 0);
    }
}

TEST(PulsarClient, Producer)
{
    FakeBroker broker;
    auto config = configuration();
    Client client(broker.url(), config);
    ProducerConfiguration producer_configuration;
    producer_configuration.send_timeout = 2000ms;

    auto producer = client.createProducer("t", producer_configuration);
    producer->send("one", "key");
    auto sends = broker.eventsOf(Type::SEND);
    ASSERT_EQ(sends.size(), 1);
    EXPECT_EQ(sends[0].payload, "one");
    EXPECT_EQ(sends[0].metadata.ordering_key(), "key");
    EXPECT_EQ(sends[0].metadata.producer_name(), "fake-producer");
    EXPECT_EQ(sends[0].metadata.sequence_id(), 0);
    EXPECT_FALSE(broker.eventsOf(Type::PRODUCER)[0].command.producer().user_provided_producer_name());

    /// The connection is lost before the receipt: the producer is created again with the same name and the message
    /// is sent again with the same sequence id.
    broker.send_mode = FakeBroker::SendMode::DropConnectionOnce;
    producer->send("two");
    sends = broker.eventsOf(Type::SEND);
    ASSERT_EQ(sends.size(), 3);
    EXPECT_EQ(sends[1].metadata.sequence_id(), 1);
    EXPECT_EQ(sends[2].metadata.sequence_id(), 1);
    EXPECT_EQ(sends[2].metadata.producer_name(), "fake-producer");
    EXPECT_EQ(sends[2].payload, "two");
    EXPECT_NE(sends[1].connection, sends[2].connection);
    auto producers = broker.eventsOf(Type::PRODUCER);
    ASSERT_EQ(producers.size(), 2);
    EXPECT_EQ(producers[1].command.producer().producer_name(), "fake-producer");
    EXPECT_FALSE(producers[1].command.producer().user_provided_producer_name());

    broker.send_mode = FakeBroker::SendMode::Error;
    EXPECT_EQ(errorCode([&] { producer->send("three"); }), ErrorCodes::CANNOT_CONNECT_PULSAR);
    EXPECT_NE(errorCode([&] { producer->send("four"); }), 0);

    /// No receipt: throws after the send timeout.
    broker.send_mode = FakeBroker::SendMode::Silent;
    auto silent = client.createProducer("t", producer_configuration);
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(errorCode([&] { silent->send("five"); }), ErrorCodes::CANNOT_CONNECT_PULSAR);
    EXPECT_GE(std::chrono::steady_clock::now() - start, 2000ms);

    /// Interrupted from another thread while waiting for the receipt.
    producer_configuration.send_timeout = 600s;
    auto interrupted = client.createProducer("t", producer_configuration);
    const size_t sends_before = broker.count(Type::SEND);
    std::thread interrupter([&]
    {
        broker.waitForCount(Type::SEND, sends_before + 1);
        interrupted->interrupt();
    });
    EXPECT_EQ(errorCode([&] { interrupted->send("six"); }), ErrorCodes::ABORTED);
    interrupter.join();
    EXPECT_EQ(errorCode([&] { interrupted->send("seven"); }), ErrorCodes::ABORTED);
}

TEST(PulsarClient, ProducerMessageSizeIncludesMetadata)
{
    FakeBroker broker;
    broker.setMaxMessageSize(1000);
    Client client(broker.url(), configuration());

    auto producer = client.createProducer("t", {});
    EXPECT_EQ(errorCode([&] { producer->send(String(900, 'x'), String(200, 'k')); }), ErrorCodes::BAD_ARGUMENTS);
    EXPECT_EQ(broker.count(Type::SEND), 0);

    auto second = client.createProducer("t", {});
    second->send(String(900, 'x'), "k");
    EXPECT_EQ(broker.count(Type::SEND), 1);
}

TEST(PulsarClient, CloseFromAnotherThread)
{
    FakeBroker broker;
    auto config = configuration();
    config.keep_alive_interval = 1000ms;
    Client client(broker.url(), config);
    auto consumer = client.subscribe({"t"}, "s", {});

    /// The client sends `PING` only inside a call.
    std::thread closer([&]
    {
        broker.waitForCount(Type::PING, 1);
        client.close();
    });
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(errorCode([&] { consumer->batchReceive(10, 600s); }), ErrorCodes::ABORTED);
    EXPECT_LT(std::chrono::steady_clock::now() - start, HANG_GUARD);
    closer.join();
    EXPECT_EQ(errorCode([&] { consumer->batchReceive(1, 100ms); }), ErrorCodes::ABORTED);
    EXPECT_EQ(errorCode([&] { client.subscribe({"t"}, "s", {}); }), ErrorCodes::ABORTED);
}

TEST(PulsarClient, LargeAcknowledgementIsSplit)
{
    FakeBroker broker;
    broker.setMaxMessageSize(1000);
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.receiver_queue_size = 1000;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    for (UInt64 entry = 1; entry <= 200; ++entry)
        broker.sendMessage(connection, consumer_id, entry, metadataOf(), "x");
    auto messages = receiveAtLeast(*consumer, 200);
    ASSERT_EQ(messages.size(), 200);
    std::vector<MessageId> ids; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (const auto & message : messages)
        ids.push_back(message.id());
    consumer->acknowledge(ids);

    auto acknowledged = [](const auto & events)
    {
        size_t res = 0;
        for (const auto & event : events)
            if (event.command.type() == Type::ACK)
                res += event.command.ack().message_id_size();
        return res;
    };
    ASSERT_TRUE(broker.waitFor([&](const auto & all) { return acknowledged(all) >= 200; }));
    auto acks = broker.eventsOf(Type::ACK);
    EXPECT_EQ(acknowledged(acks), 200);
    EXPECT_GE(acks.size(), 2);
    for (const auto & ack : acks)
        EXPECT_LE(ack.frame_size, 1000);
}

TEST(PulsarClient, LargeAcknowledgementWithLongIdsIsSplit)
{
    FakeBroker broker;
    broker.setMaxMessageSize(20000);
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.receiver_queue_size = 2000;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    /// Ledger and entry ids of 10 bytes each, the longest encoding.
    constexpr UInt64 big = 1ULL << 63;
    for (UInt64 i = 0; i < 2000; ++i)
        broker.sendMessage(connection, consumer_id, big + i, metadataOf(), "x", big);
    auto messages = receiveAtLeast(*consumer, 2000);
    ASSERT_EQ(messages.size(), 2000);
    std::vector<MessageId> ids; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (const auto & message : messages)
        ids.push_back(message.id());
    consumer->acknowledge(ids);

    auto acknowledged = [](const auto & events)
    {
        size_t res = 0;
        for (const auto & event : events)
            if (event.command.type() == Type::ACK)
                res += event.command.ack().message_id_size();
        return res;
    };
    ASSERT_TRUE(broker.waitFor([&](const auto & all) { return acknowledged(all) >= 2000; }));
    auto acks = broker.eventsOf(Type::ACK);
    EXPECT_EQ(acknowledged(acks), 2000);
    EXPECT_GE(acks.size(), 3);
    /// The broker's limit on a whole frame.
    for (const auto & ack : acks)
        EXPECT_LE(ack.frame_size + 4, 20000 + FRAME_PADDING);
}

TEST(PulsarClient, CompressedChunkedMessage)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    ConsumerConfiguration consumer_configuration;
    consumer_configuration.receiver_queue_size = 3;
    auto consumer = client.subscribe({"t"}, "s", consumer_configuration);
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;
    ASSERT_TRUE(broker.waitForCount(Type::FLOW, 1));

    String data;
    for (size_t i = 0; data.size() < 30000; ++i)
        data += fmt::format("{}", i % 1000);
    data.resize(30000);
    String compressed(ZSTD_compressBound(data.size()), '\0');
    compressed.resize(ZSTD_compress(compressed.data(), compressed.size(), data.data(), data.size(), 1));
    ASSERT_GE(compressed.size(), 3);

    /// The producer compresses the whole payload and then slices it into chunks.
    const size_t part = compressed.size() / 3;
    for (Int32 i = 0; i < 3; ++i)
    {
        auto metadata = metadataOf();
        metadata.set_uuid("u");
        metadata.set_chunk_id(i);
        metadata.set_num_chunks_from_msg(3);
        metadata.set_total_chunk_msg_size(static_cast<Int32>(compressed.size()));
        metadata.set_compression(PulsarProto::ZSTD);
        metadata.set_uncompressed_size(static_cast<UInt32>(data.size()));
        broker.sendMessage(connection, consumer_id, i + 1, metadata, std::string_view(compressed).substr(i * part, i == 2 ? String::npos : part));
    }

    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), data);
    EXPECT_TRUE(consumer->batchReceive(10, 100ms).empty());

    /// One permit for each chunk.
    auto permits_after_initial = [](const auto & events)
    {
        UInt64 res = 0;
        bool initial = true;
        for (const auto & event : events)
        {
            if (event.command.type() != Type::FLOW)
                continue;
            if (!initial)
                res += event.command.flow().messagepermits();
            initial = false;
        }
        return res;
    };
    ASSERT_TRUE(broker.waitFor([&](const auto & all) { return permits_after_initial(all) >= 3; }));
    EXPECT_EQ(permits_after_initial(broker.allEvents()), 3);

    consumer->acknowledge(std::span(&messages[0].id(), 1));
    ASSERT_TRUE(broker.waitForCount(Type::ACK, 1));
    auto acks = broker.eventsOf(Type::ACK);
    ASSERT_EQ(acks.size(), 1);
    ASSERT_EQ(acks[0].command.ack().message_id_size(), 3);
    for (Int32 i = 0; i < 3; ++i)
        EXPECT_EQ(acks[0].command.ack().message_id(i).entryid(), static_cast<UInt64>(i + 1));
    EXPECT_FALSE(acks[0].command.ack().has_validation_error());
}

TEST(PulsarClient, LateResponseIsIgnored)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    auto consumer = client.subscribe({"t"}, "s", {});
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    /// The response to a request that is not awaited anymore.
    PulsarProto::BaseCommand success;
    success.set_type(Type::SUCCESS);
    success.mutable_success()->set_request_id(broker.eventsOf(Type::SUBSCRIBE)[0].command.subscribe().request_id());
    broker.sendCommand(connection, success);
    EXPECT_TRUE(consumer->batchReceive(1, 100ms).empty());
    broker.sendMessage(connection, consumer_id, 1, metadataOf(), "after");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), "after");

    /// The response to a request that was never sent.
    success.mutable_success()->set_request_id(1000000);
    broker.sendCommand(connection, success);
    int code = 0;
    for (size_t i = 0; i < 100 && !code; ++i)
        code = errorCode([&] { consumer->batchReceive(1, 100ms); });
    EXPECT_EQ(code, ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}

TEST(PulsarClient, PingFlood)
{
    FakeBroker broker;
    Client client(broker.url(), configuration());
    auto consumer = client.subscribe({"t"}, "s", {});
    auto [connection, consumer_id] = subscriptions(broker).begin()->second;

    PulsarProto::BaseCommand ping;
    ping.set_type(Type::PING);
    ping.mutable_ping();
    const auto one = encodeCommand(ping);
    String batch;
    for (size_t i = 0; i < 10000; ++i)
        batch.append(one.data(), one.size());

    std::atomic<bool> stop = false;
    std::atomic<bool> finished = false;
    std::atomic<size_t> pings = 0;
    std::thread flood([&]
    {
        const auto flood_start = std::chrono::steady_clock::now();
        while (!stop && std::chrono::steady_clock::now() - flood_start < 10s)
        {
            broker.sendRaw(connection, batch);
            pings += 10000;
        }
        finished = true;
    });

    const auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(consumer->batchReceive(1, 200ms).empty());
    const auto elapsed = std::chrono::steady_clock::now() - start;
    stop = true;
    /// Reads the rest, so that the writer is not blocked on a full socket.
    while (!finished)
        consumer->batchReceive(1, 100ms);
    flood.join();
    EXPECT_LT(elapsed, 5s);

    broker.sendMessage(connection, consumer_id, 1, metadataOf(), "after");
    auto messages = receiveAtLeast(*consumer, 1);
    ASSERT_EQ(messages.size(), 1);
    EXPECT_EQ(messages[0].payload(), "after");
    EXPECT_TRUE(consumer->batchReceive(1, 500ms).empty());
    /// Not one PONG per PING.
    EXPECT_LT(broker.count(Type::PONG) * 100, pings.load());
}

#endif
