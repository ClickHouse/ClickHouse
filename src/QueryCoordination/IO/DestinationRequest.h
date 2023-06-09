#pragma once

#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

class DestinationRequest
{

public:
    void write(WriteBuffer & out) const
    {
        Coordination::write(fragment_id, out);
        Coordination::write(host, out);
    }

    UInt32 fragment_id;
    String host;
};

}
