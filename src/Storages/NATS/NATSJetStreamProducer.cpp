#include <Storages/NATS/NATSJetStreamProducer.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <thread>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

/// A publish that found no stream is retried like the official Go client does, to ride out a leader change of the stream.
static constexpr size_t NO_RESPONDERS_RETRIES = 2;
static constexpr auto NO_RESPONDERS_RETRY_WAIT = std::chrono::milliseconds(250);

/// How long to wait for the acknowledgement of the stream, as the default of the client library used before.
static const Poco::Timespan PUBLISH_ACK_TIMEOUT(5, 0);

INATSProducer::PublishStatus NATSJetStreamProducer::publishMessage(const String & message, String & error)
{
    if (message.size() > getConnection().getMaxPayload())
    {
        error = fmt::format("Maximum Payload Exceeded: the message has {} bytes, the server accepts at most {}",
            message.size(), getConnection().getMaxPayload());
        return PublishStatus::Fail;
    }

    NATSClient::Message reply;
    for (size_t retry = 0;; ++retry)
    {
        try
        {
            reply = getConnection().request(getSubject(), message, PUBLISH_ACK_TIMEOUT);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::TIMEOUT_EXCEEDED)
                throw;
            error = "timeout while waiting for the acknowledgement of the stream";
            return PublishStatus::Retry;
        }

        /// No responders: there is no stream for this subject, at least at the moment.
        if (reply.status != 503 || retry == NO_RESPONDERS_RETRIES)
            break;
        std::this_thread::sleep_for(NO_RESPONDERS_RETRY_WAIT);
    }

    if (reply.status == 503)
    {
        error = "there is no JetStream stream for this subject";
        return PublishStatus::Fail;
    }
    if (reply.status != 0)
    {
        error = fmt::format("status {} {}", reply.status, reply.status_description);
        return PublishStatus::Fail;
    }

    Poco::JSON::Parser parser;
    const auto acknowledgement = parser.parse(reply.data).extract<Poco::JSON::Object::Ptr>();
    if (acknowledgement->has("error"))
    {
        const auto stream_error = acknowledgement->getObject("error");
        error = stream_error->has("description") ? stream_error->getValue<String>("description") : "the stream rejected the message";
        return PublishStatus::Fail;
    }

    if (acknowledgement->has("duplicate") && acknowledgement->getValue<bool>("duplicate"))
        LOG_WARNING(log, "Duplicate message during publishing to NATS subject. Message: {}.", message);

    return PublishStatus::Ok;
}

}
