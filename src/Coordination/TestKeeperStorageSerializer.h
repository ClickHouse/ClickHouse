#pragma once
#include <Coordination/TestKeeperStorage.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

class TestKeeperStorageSerializer
{
public:
    static void serialize(const TestKeeperStorage & storage, WriteBuffer & out);

    static void deserialize(TestKeeperStorage & storage, ReadBuffer & in);
};

}
