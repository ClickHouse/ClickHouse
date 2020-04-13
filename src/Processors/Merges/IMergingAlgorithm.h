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

    virtual void initialize(Chunks chunks) = 0;
    virtual void consume(Chunk chunk, size_t source_num) = 0;
    virtual Status merge() = 0;

    IMergingAlgorithm() = default;
    virtual ~IMergingAlgorithm() = default;
};

// TODO: use when compile with clang which could support it
// template <class T>
// concept MergingAlgorithm = std::is_base_of<IMergingAlgorithm, T>::value;

}
