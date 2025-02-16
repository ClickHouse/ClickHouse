#pragma once

#include <Processors/Chunk.h>
#include <Common/ProfileEvents.h>

namespace DB
{

class IMergingAlgorithm
{
public:
    struct Status
    {
        Chunk chunk;
        bool is_finished = false;
        ssize_t required_source = -1;

        explicit Status(Chunk chunk_) : chunk(std::move(chunk_)) {}
        explicit Status(Chunk chunk_, bool is_finished_) : chunk(std::move(chunk_)), is_finished(is_finished_) {}
        explicit Status(size_t source) : required_source(source) {}
    };

    struct Input
    {
        Chunk chunk;

        /// It is a flag which says that last row from chunk should be ignored in result.
        /// This row is not ignored in sorting and is needed to synchronize required source
        /// between different algorithm objects in parallel FINAL.
        bool skip_last_row = false;

        IColumn::Permutation * permutation = nullptr;

        void swap(Input & other) noexcept
        {
            chunk.swap(other.chunk);
            std::swap(skip_last_row, other.skip_last_row);
        }

        void set(Chunk chunk_)
        {
            chunk = std::move(chunk_);
            skip_last_row = false;
        }
    };

    using Inputs = std::vector<Input>;

    static void removeConstAndSparse(Input & input)
    {
        convertToFullIfConst(input.chunk);
        convertToFullIfSparse(input.chunk);
    }

    static void removeConstAndSparse(Inputs & inputs)
    {
        for (auto & input : inputs)
            removeConstAndSparse(input);
    }

    virtual const char * getName() const = 0;
    virtual void initialize(Inputs inputs) = 0;
    virtual void consume(Input & input, size_t source_num) = 0;
    virtual Status merge() = 0;

    IMergingAlgorithm() = default;
    virtual ~IMergingAlgorithm() = default;

    struct MergedStats
    {
        UInt64 bytes = 0;
        UInt64 rows = 0;
        UInt64 blocks = 0;
    };

    virtual MergedStats getMergedStats() const = 0;
};

// TODO: use when compile with clang which could support it
// template <class T>
// concept MergingAlgorithm = std::is_base_of<IMergingAlgorithm, T>::value;

}
