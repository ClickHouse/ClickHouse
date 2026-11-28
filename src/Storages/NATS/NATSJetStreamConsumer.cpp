#include <Storages/NATS/NATSJetStreamConsumer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <fmt/ranges.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

/// The size of a pull request. The server also limits the unacknowledged messages by `max_ack_pending` of the consumer.
static constexpr size_t PULL_BATCH = 1000;
/// A pull request expires after this time if it is not filled, then the next read sends a new one.
static constexpr auto PULL_EXPIRES = std::chrono::seconds(1);
/// A pull request is considered finished this long after it expired, even if the server did not say so.
static constexpr auto PULL_EXPIRY_GRACE = std::chrono::seconds(5);

namespace
{

/// The description of the error in a reply of the JetStream API, or empty if it is not an error.
String getAPIError(const NATSClient::Message & reply, const Poco::JSON::Object::Ptr & object)
{
    if (reply.status == 503)
        return "JetStream is not available on the server (no responders)";
    if (reply.status != 0)
        return fmt::format("status {} {}", reply.status, reply.status_description);
    if (!object->has("error"))
        return {};
    const auto error = object->getObject("error");
    return error->has("description") ? error->getValue<String>("description") : "unknown error";
}

}

NATSJetStreamConsumer::NATSJetStreamConsumer(
    const NATSConfiguration & configuration,
    String stream_name_,
    String consumer_name_,
    const std::vector<String> & subjects,
    const String & subscribe_queue_name,
    LoggerPtr log,
    const std::atomic<bool> & stopped)
    : INATSConsumer(configuration, subjects, subscribe_queue_name, log, stopped)
    , stream_name(std::move(stream_name_))
    , consumer_name(std::move(consumer_name_))
{
}

void NATSJetStreamConsumer::subscribeImpl()
{
    if (consumer_name.empty())
        throw Exception(ErrorCodes::INVALID_STATE, "To use NATS jet stream consumers, you must specify `nats_consumer_name` setting");

    /// The consumer must exist: it is created and configured by the user, e.g. with its filter subjects.
    const auto reply = getConnection().request(
        fmt::format("$JS.API.CONSUMER.INFO.{}.{}", stream_name, consumer_name), "", Poco::Timespan(10, 0));
    Poco::JSON::Object::Ptr info = new Poco::JSON::Object;
    if (reply.status == 0)
    {
        Poco::JSON::Parser parser;
        info = parser.parse(reply.data).extract<Poco::JSON::Object::Ptr>();
    }
    if (const auto error = getAPIError(reply, info); !error.empty())
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to subscribe to the NATS JetStream consumer {} of stream {}: {}", consumer_name, stream_name, error);

    pull_batch = PULL_BATCH;
    pull_expires = PULL_EXPIRES;
    if (info->has("config"))
    {
        const auto config = info->getObject("config");

        /// A consumer with filter subjects delivers only their messages, which must be the subjects of the table:
        /// otherwise the table would consume, and acknowledge, messages of other subjects.
        std::vector<String> filters;
        if (config->has("filter_subject") && !config->getValue<String>("filter_subject").empty())
            filters.push_back(config->getValue<String>("filter_subject"));
        if (config->has("filter_subjects"))
            if (const auto array = config->getArray("filter_subjects"))
                for (size_t i = 0; i < array->size(); ++i)
                    filters.push_back(array->getElement<String>(static_cast<unsigned>(i)));
        if (!filters.empty())
        {
            for (const auto & subject : getSubjects())
                if (std::find(filters.begin(), filters.end(), subject) == filters.end())
                    throw Exception(
                        ErrorCodes::CANNOT_CONNECT_NATS,
                        "Subject {} does not match the filter subjects ({}) of the NATS JetStream consumer {} of stream {}",
                        subject, fmt::join(filters, ", "), consumer_name, stream_name);
        }

        if (config->has("max_batch") && config->getValue<Int64>("max_batch") > 0)
            pull_batch = std::min<size_t>(pull_batch, config->getValue<Int64>("max_batch"));
        if (config->has("max_expires") && config->getValue<Int64>("max_expires") > 0)
            pull_expires = std::min(pull_expires, std::chrono::nanoseconds(config->getValue<Int64>("max_expires")));
    }

    inbox = getConnection().newInbox();
    sid = getConnection().subscribe(inbox, "");
    pull_outstanding = false;
    getConnection().flush();

    LOG_DEBUG(getLogger(), "Consumer subscribed to the JetStream consumer {} of stream {}", consumer_name, stream_name);
}

void NATSJetStreamConsumer::unsubscribeImpl()
{
    getConnection().unsubscribe(sid);
    getConnection().flush();
    sid = 0;
    pull_outstanding = false;
}

std::vector<String> NATSJetStreamConsumer::drainBeforeUnsubscribe()
{
    /// A pull request cannot be cancelled, and the server keeps delivering to it, also the messages that are returned
    /// right after this, until it expires. So wait for its end: then the returned messages go to the next pull request.
    std::vector<String> replies;
    while (pull_outstanding)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now > pull_expires_at + PULL_EXPIRY_GRACE)
            break;

        const auto wait = std::chrono::duration_cast<std::chrono::microseconds>(pull_expires_at + PULL_EXPIRY_GRACE - now);
        auto message = getConnection().receive(Poco::Timespan(wait.count()));
        if (!message)
            break;
        if (message->sid != sid)
            continue;

        if (message->status == 0)
        {
            if (!message->reply.empty())
                replies.push_back(message->reply);
            if (--pull_remaining == 0)
                pull_outstanding = false;
        }
        else if (message->status != 100)
            pull_outstanding = false;
    }
    pull_outstanding = false;
    return replies;
}

void NATSJetStreamConsumer::requestPull()
{
    getConnection().publish(
        fmt::format("$JS.API.CONSUMER.MSG.NEXT.{}.{}", stream_name, consumer_name),
        inbox,
        fmt::format(R"({{"batch":{},"expires":{}}})", pull_batch, pull_expires.count()));
    pull_outstanding = true;
    pull_remaining = pull_batch;
    pull_expires_at = std::chrono::steady_clock::now() + std::chrono::duration_cast<std::chrono::steady_clock::duration>(pull_expires);
}

std::optional<NATSClient::Message> NATSJetStreamConsumer::acceptMessage(NATSClient::Message message)
{
    if (message.sid != sid)
        return {};

    if (message.status == 0)
    {
        if (pull_remaining > 0)
        {
            --pull_remaining;
            if (pull_remaining == 0)
                pull_outstanding = false;
        }
        return message;
    }

    /// 100: an idle heartbeat. 404: no messages, 408: the request expired, 409: e.g. the limit of waiting requests.
    if (message.status != 100)
    {
        pull_outstanding = false;
        if (message.status != 404 && message.status != 408)
        {
            LOG_DEBUG(getLogger(), "The JetStream pull request ended with status {} {}", message.status, message.status_description);
            pull_refused = true;
        }
    }
    return {};
}

std::optional<NATSClient::Message> NATSJetStreamConsumer::receiveImpl(UInt64 timeout_ms)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (true)
    {
        const auto now = std::chrono::steady_clock::now();
        if (pull_outstanding && now > pull_expires_at + PULL_EXPIRY_GRACE)
            pull_outstanding = false;
        if (!pull_outstanding)
            requestPull();

        const auto wait = std::chrono::duration_cast<std::chrono::microseconds>(std::max(deadline - now, std::chrono::steady_clock::duration::zero()));
        auto message = getConnection().receive(Poco::Timespan(wait.count()));
        if (!message)
            return {};

        if (auto accepted = acceptMessage(std::move(*message)))
            return accepted;

        /// Not requested again right away, so that a server that keeps refusing is not asked in a loop.
        if (std::exchange(pull_refused, false))
            return {};

        if (std::chrono::steady_clock::now() >= deadline)
            return {};
    }
}

}
