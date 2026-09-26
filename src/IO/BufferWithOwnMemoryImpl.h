#pragma once

/// Definitions of Memory<Allocator>::alloc and Memory<Allocator>::resize.
///
/// Kept out of BufferWithOwnMemory.h so that the heavily included declaration header does not pull
/// in ProfileEvents.h. Include this header only from the translation unit that instantiates Memory
/// for a given allocator (BufferWithOwnMemory.cpp for the production allocators, or a test that uses
/// its own allocator type).

#include <IO/BufferWithOwnMemory.h>

#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event IOBufferAllocs;
    extern const Event IOBufferAllocBytes;
}


namespace DB
{

template <typename Allocator>
void Memory<Allocator>::alloc(size_t new_size)
{
    if (!new_size)
    {
        m_data = nullptr;
        return;
    }

    size_t new_capacity = withPadding(new_size);

    ProfileEvents::increment(ProfileEvents::IOBufferAllocs);
    ProfileEvents::increment(ProfileEvents::IOBufferAllocBytes, new_capacity);

    m_data = static_cast<char *>(Allocator::alloc(new_capacity, alignment));
    m_capacity = new_capacity;
    m_size = new_size;
}

template <typename Allocator>
void Memory<Allocator>::resize(size_t new_size, bool deallocate_if_empty)
{
    if (!m_data)
    {
        alloc(new_size);
        return;
    }

    if (new_size == 0 && deallocate_if_empty)
    {
        dealloc();
        m_size = m_capacity = 0;
        return;
    }

    if (new_size <= m_capacity - pad_right)
    {
        m_size = new_size;
        return;
    }

    size_t new_capacity = withPadding(new_size);

    size_t diff = new_capacity - m_capacity;
    ProfileEvents::increment(ProfileEvents::IOBufferAllocBytes, diff);

    m_data = static_cast<char *>(Allocator::realloc(m_data, m_capacity, new_capacity, alignment));
    m_capacity = new_capacity;
    m_size = new_size;
}

}
