#pragma once

#include <boost/noncopyable.hpp>

#include <Common/ProfileEvents.h>
#include <Common/Allocator.h>
#include <Common/GWPAsan.h>

#include <Common/Exception.h>
#include <Core/Defines.h>

#include <base/arithmeticOverflow.h>

#include "config.h"


namespace ProfileEvents
{
    extern const Event IOBufferAllocs;
    extern const Event IOBufferAllocBytes;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}


/** Replacement for std::vector<char> to use in buffers.
  * Differs in that is doesn't do unneeded memset. (And also tries to do as little as possible.)
  * Also allows to allocate aligned piece of memory (to use with O_DIRECT, for example).
  */
template <typename Allocator = Allocator<false>>
struct Memory : boost::noncopyable, Allocator
{
    static constexpr size_t pad_right = PADDING_FOR_SIMD - 1;

    size_t m_capacity = 0;  /// With padding.
    size_t m_size = 0;
    char * m_data = nullptr;
    size_t alignment = 0;

    [[maybe_unused]] bool allow_gwp_asan_force_sample{false};

    Memory() = default;

    /// If alignment != 0, then allocate memory aligned to specified value.
    explicit Memory(size_t size_, size_t alignment_ = 0, bool allow_gwp_asan_force_sample_ = false)
        : alignment(alignment_), allow_gwp_asan_force_sample(allow_gwp_asan_force_sample_)
    {
        alloc(size_);
    }

    ~Memory()
    {
        dealloc();
    }

    void swap(Memory & rhs) noexcept
    {
        std::swap(m_capacity, rhs.m_capacity);
        std::swap(m_size, rhs.m_size);
        std::swap(m_data, rhs.m_data);
        std::swap(alignment, rhs.alignment);
    }

    Memory(Memory && rhs) noexcept
    {
        swap(rhs);
    }

    Memory & operator=(Memory && rhs) noexcept
    {
        swap(rhs);
        return *this;
    }

    size_t size() const { return m_size; }
    const char & operator[](size_t i) const { return m_data[i]; }
    char & operator[](size_t i) { return m_data[i]; }
    const char * data() const { return m_data; }
    char * data() { return m_data; }

    void resize(size_t new_size)
    {
        if (!m_data)
        {
            alloc(new_size);
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

private:
    static size_t withPadding(size_t value)
    {
        size_t res = 0;

        if (common::addOverflow<size_t>(value, pad_right, res))
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "value is too big to apply padding");

        return res;
    }

    void alloc(size_t new_size)
    {
        if (!new_size)
        {
            m_data = nullptr;
            return;
        }

        size_t new_capacity = withPadding(new_size);

        ProfileEvents::increment(ProfileEvents::IOBufferAllocs);
        ProfileEvents::increment(ProfileEvents::IOBufferAllocBytes, new_capacity);

#if USE_GWP_ASAN
        if (unlikely(allow_gwp_asan_force_sample && GWPAsan::shouldForceSample()))
            gwp_asan::getThreadLocals()->NextSampleCounter = 1;
#endif

        m_data = static_cast<char *>(Allocator::alloc(new_capacity, alignment));
        m_capacity = new_capacity;
        m_size = new_size;
    }

    void dealloc()
    {
        if (!m_data)
            return;

        Allocator::free(m_data, m_capacity);
        m_data = nullptr;    /// To avoid double free if next alloc will throw an exception.
    }
};


/** Buffer that could own its working memory.
  * Template parameter: ReadBuffer or WriteBuffer
  */
template <typename Base>
class BufferWithOwnMemory : public Base
{
protected:
    Memory<> memory;
public:
    /// If non-nullptr 'existing_memory' is passed, then buffer will not create its own memory and will use existing_memory without ownership.
    explicit BufferWithOwnMemory(size_t size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0)
        : Base(nullptr, 0), memory(existing_memory ? 0 : size, alignment, /*allow_gwp_asan_force_sample_=*/true)
    {
        Base::set(existing_memory ? existing_memory : memory.data(), size);
        Base::padded = !existing_memory;
    }
};


/** Buffer that could write data to external memory which came from outside
  * Template parameter: ReadBuffer or WriteBuffer
  */
template <typename Base>
class BufferWithOutsideMemory : public Base
{
protected:
    Memory<> & memory;
public:

    explicit BufferWithOutsideMemory(Memory<> & memory_)
        : Base(memory_.data(), memory_.size()), memory(memory_)
    {
        Base::set(memory.data(), memory.size(), 0);
        Base::padded = false;
    }

    size_t getActualSize()
    {
        return Base::count();
    }

private:
    void nextImpl() final
    {
        const size_t prev_size = Base::position() - memory.data();
        memory.resize(2 * prev_size + 1);
        Base::set(memory.data() + prev_size, memory.size() - prev_size, 0);
    }

    void finalizeImpl() final
    {
        /// there is no need to allocate twice more memory at finalize()
        /// So make that call no op, do not call here nextImpl()
    }
};

}
