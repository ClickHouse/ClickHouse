#include <Common/WeakHash.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void WeakHash32::update(const WeakHash32 & other)
{
    size_t size = data.size();
    if (size != other.data.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of WeakHash32 does not match:"
                        "left size is {}, right size is {}", size, other.data.size());

    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<UInt32>(intHashCRC32(other.data[i], data[i]));
}

}
