#pragma once

#include <mutex>

#include <Core/Types.h>
#include <Server/IProtocolServer.h>

namespace DB
{

size_t waitServersToFinish(std::vector<IProtocolServerPtr> & servers, std::mutex & mutex, size_t seconds_to_wait);

}
