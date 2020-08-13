#include "MySQLProtocol.h"
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <common/logger_useful.h>

#include <random>
#include <sstream>


namespace DB::MySQLProtocol
{
extern const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb
}
