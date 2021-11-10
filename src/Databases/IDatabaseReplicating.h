#pragma once

namespace DB
{

class IDatabaseReplicating
{
public:
    virtual void stopReplication() = 0;
    virtual ~IDatabaseReplicating() = default;
};

}
