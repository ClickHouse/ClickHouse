#include <Storages/Pulsar/Client/Client.h>

#if USE_PULSAR

#include <Storages/Pulsar/Client/Connection.h>
#include <Storages/Pulsar/Client/Consumer.h>
#include <Storages/Pulsar/Client/ConsumerImpl.h>
#include <Storages/Pulsar/Client/Producer.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
}

namespace Pulsar
{

Client::Client(const String & service_url, ClientConfiguration configuration)
    : state(std::make_shared<ClientState>())
{
    state->service = parseServiceUrl(service_url);
    state->configuration = std::move(configuration);
    state->log = getLogger("PulsarClient");
}

Client::~Client() = default;

std::unique_ptr<Consumer> Client::subscribe(const std::vector<String> & topics, const String & subscription, const ConsumerConfiguration & configuration) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    if (topics.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No Pulsar topics to subscribe to");
    if (state->interrupter.isInterrupted())
        throw Exception(ErrorCodes::ABORTED, "The Pulsar client is closed");

    VectorWithMemoryTracking<String> names;
    for (const auto & topic : topics)
        names.push_back(normalizeTopicName(topic));

    auto impl = std::make_unique<ConsumerImpl>(state, std::move(names), subscription, configuration);
    impl->subscribe();
    return std::make_unique<Consumer>(std::move(impl));
}

std::unique_ptr<Producer> Client::createProducer(const String & topic, const ProducerConfiguration & configuration)
{
    if (state->interrupter.isInterrupted())
        throw Exception(ErrorCodes::ABORTED, "The Pulsar client is closed");
    return Pulsar::createProducer(state, normalizeTopicName(topic), configuration);
}

void Client::close() noexcept
{
    state->interrupter.interrupt();
}

}

}

#endif
