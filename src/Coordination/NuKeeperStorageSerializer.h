#pragma once
#include <Coordination/NuKeeperStorage.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class NuKeeperStorageSerializer
{
public:
    static void serialize(const NuKeeperStorage & storage, WriteBuffer & out);

    static void deserialize(NuKeeperStorage & storage, ReadBuffer & in);
};

}
