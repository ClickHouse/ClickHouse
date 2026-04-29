#include <IO/Rope.h>

namespace DB
{

OwnedRopeBuffer::OwnedRopeBuffer(size_t size)
    : data_(static_cast<char *>(::operator new(size)))
    , size_(size)
{
}

OwnedRopeBuffer::~OwnedRopeBuffer()
{
    ::operator delete(data_);
}

void OwnedRopeBuffer::transferTo(MemoryTracker * /* new_tracker */)
{
    /// Will be implemented when PageCacheProvider needs it.
}

}
