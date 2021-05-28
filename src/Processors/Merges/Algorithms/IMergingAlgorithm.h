#pragma once

#include <Processors/Chunk.h>
#include <variant>

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

        void swap(Input & other)
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

    virtual void initialize(Inputs inputs) = 0;
    virtual void consume(Input & input, size_t source_num) = 0;
    virtual Status merge() = 0;

    IMergingAlgorithm() = default;
    virtual ~IMergingAlgorithm() = default;
};

// TODO: use when compile with clang which could support it
// template <class T>
// concept MergingAlgorithm = std::is_base_of<IMergingAlgorithm, T>::value;

}
