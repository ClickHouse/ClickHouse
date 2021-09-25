#pragma once

#include <Common/Arena.h>
#include <common/StringRef.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/** Writes data contiguously into Arena.
  * As it will be located in contiguous memory segment, it can be read back with ReadBufferFromMemory.
  *
  * While using this object, no other allocations in arena are possible.
  */
class WriteBufferFromArena final : public WriteBuffer
{
private:
    Arena & arena;
    const char *& begin;

    void nextImpl() override
    {
        /// Allocate more memory. At least same size as used before (this gives 2x growth ratio),
        ///  and at most grab all remaining size in current chunk of arena.
        ///
        /// FIXME this class just doesn't make sense -- WriteBuffer is not
        /// a unified interface for everything, it doesn't work well with
        /// Arena::allocContinue -- we lose the size of data and then use a
        /// heuristic to guess it back? and make a virtual call while we're at it?
        /// I don't even..
        /// Being so ill-defined as it is, no wonder that the following line had
        /// a bug leading to a very rare infinite loop. Just hack around it in
        /// the most stupid way possible, because the real fix for this is to
        /// tear down the entire WriteBuffer thing and implement it again,
        /// properly.
        size_t continuation_size = std::max(size_t(1),
            std::max(count(), arena.remainingSpaceInCurrentMemoryChunk()));

        /// allocContinue method will possibly move memory region to new place and modify "begin" pointer.

        char * continuation = arena.allocContinue(continuation_size, begin);
        char * end = continuation + continuation_size;

        /// internal buffer points to whole memory segment and working buffer - to free space for writing.
        internalBuffer() = Buffer(const_cast<char *>(begin), end);
        buffer() = Buffer(continuation, end);
    }

public:
    /// begin_ - start of previously used contiguous memory segment or nullptr (see Arena::allocContinue method).
    WriteBufferFromArena(Arena & arena_, const char *& begin_)
        : WriteBuffer(nullptr, 0), arena(arena_), begin(begin_)
    {
        nextImpl();
        pos = working_buffer.begin();
    }

    StringRef finish()
    {
        /// Return over-allocated memory back into arena.
        arena.rollback(buffer().end() - position());
        /// Reference to written data.
        return { position() - count(), count() };
    }
};

}

