#pragma once

#include <mutex>

#include <Core/Types.h>

namespace DB
{
class IProtocolServer;

size_t waitServersToFinish(std::vector<IProtocolServer> & servers, std::mutex & mutex, size_t seconds_to_wait);

}
