#include <Storages/Kafka/Clients/Config.h>

namespace Kafka
{

const char * securityProtocolName(SecurityProtocol protocol) noexcept
{
    switch (protocol)
    {
        case SecurityProtocol::Plaintext: return "plaintext";
        case SecurityProtocol::Ssl: return "ssl";
        case SecurityProtocol::SaslPlaintext: return "sasl_plaintext";
        case SecurityProtocol::SaslSsl: return "sasl_ssl";
    }
    return "unknown";
}

}
