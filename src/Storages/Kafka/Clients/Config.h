#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <chrono>
#include <optional>
#include <vector>

namespace Kafka
{

struct SaslPlainConfig
{
    String username;
    String password;
    /// Usually empty; sent as the authzid field of the PLAIN message.
    String authorization_id;
};

struct ClientConfig
{
    /// "host:port" entries used for the first metadata bootstrap.
    std::vector<String> bootstrap_brokers;
    String client_id = "ClickHouse";
    /// nullopt = PLAINTEXT.
    std::optional<SaslPlainConfig> sasl;

    std::chrono::milliseconds connect_timeout{10'000};
    /// Default per-operation deadline when the caller passes none.
    std::chrono::milliseconds request_timeout{30'000};
    /// Cached metadata older than this refreshes on next use.
    std::chrono::milliseconds metadata_max_age{300'000};
    /// Upper bound of pooled connections per broker. Operations beyond the
    /// bound open a short-lived extra connection rather than block.
    size_t max_connections_per_broker = 3;

    /// The client never writes to stderr on its own; all messages go to this
    /// logger on the thread that runs the operation (the client owns no threads).
    LoggerPtr log = getLogger("KafkaClient");
};

}
