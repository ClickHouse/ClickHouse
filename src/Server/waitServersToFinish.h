#pragma once

#include <mutex>

#include <Core/Types.h>

namespace DB
{
class ProtocolServerAdapter;

size_t waitServersToFinish(std::vector<ProtocolServerAdapter> & servers, std::mutex & mutex, size_t seconds_to_wait);

}
