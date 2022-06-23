#pragma once

#include <cstdlib>
#include <utility>
#include <boost/noncopyable.hpp>


namespace DB
{

/** Aligned piece of memory.
  * It can only be allocated and destroyed.
  * MemoryTracker is not used. AlignedBuffer is intended for small pieces of memory.
  */
class AlignedBuffer : private boost::noncopyable
{
private:
    void * buf = nullptr;

    void alloc(size_t size, size_t alignment);
    void dealloc();

public:
    AlignedBuffer() {}
    AlignedBuffer(size_t size, size_t alignment);
    AlignedBuffer(AlignedBuffer && old) { std::swap(buf, old.buf); }
    ~AlignedBuffer();

    void reset(size_t size, size_t alignment);

    char * data() { return static_cast<char *>(buf); }
    const char * data() const { return static_cast<const char *>(buf); }
};

}

