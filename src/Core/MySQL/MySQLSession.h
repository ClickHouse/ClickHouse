#pragma once

#include <common/types.h>
#include <Interpreters/Session.h>

namespace DB
{

class MySQLSession : public DB::Session
{
public:
    using DB::Session::Session;

    uint8_t sequence_id = 0;
    uint32_t client_capabilities = 0;
    size_t max_packet_size = 0;
};

}
