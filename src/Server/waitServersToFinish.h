#pragma once
#include <Core/Types.h>

namespace DB
{
class ProtocolServerAdapter;

size_t waitServersToFinish(std::vector<ProtocolServerAdapter> & servers, size_t seconds_to_wait);

}
