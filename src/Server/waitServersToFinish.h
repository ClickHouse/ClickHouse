#pragma once

#include <functional>
#include <mutex>

#include <Core/Types.h>

namespace DB
{
class ProtocolServerAdapter;

using ProtocolServerFilter = std::function<bool(const ProtocolServerAdapter &)>;

size_t waitServersToFinish(std::vector<ProtocolServerAdapter> & servers, std::mutex & mutex, size_t seconds_to_wait);
size_t waitServersToFinish(
    std::vector<ProtocolServerAdapter> & servers,
    std::mutex & mutex,
    size_t seconds_to_wait,
    const ProtocolServerFilter & server_filter);

}
