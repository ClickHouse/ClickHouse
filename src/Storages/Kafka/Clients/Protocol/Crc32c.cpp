#include <Storages/Kafka/Clients/Protocol/Crc32c.h>

#include <crc32c/crc32c.h>

namespace Kafka::Protocol
{

UInt32 crc32c(std::string_view data)
{
    return ::crc32c::Crc32c(data.data(), data.size());
}

}
