#pragma once

#include <vector>

#include <IO/AutoFinalizedWriteBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

struct AppendModeTag {};

/** Writes data to existing std::vector or similar type. When not enough space, it doubles vector size.
  *
  * In destructor, vector is cut to the size of written data.
  * You can call 'finalize' to resize earlier.
  *
  * The vector should live until this object is destroyed or until the 'finalizeImpl()' method is called.
  */
template <typename VectorType>
class WriteBufferFromVectorImpl : public WriteBuffer
{
public:
    using ValueType = typename VectorType::value_type;
    /// `max_growth_step_` (in elements, 0 = pure doubling) caps how much the backing vector grows at
    /// once: beyond that increment it grows by fixed steps instead of doubling, bounding over-allocation.
    explicit WriteBufferFromVectorImpl(VectorType & vector_, size_t max_growth_step_ = 0)
        : WriteBuffer(reinterpret_cast<Position>(vector_.data()), vector_.size()), vector(vector_), max_growth_step(max_growth_step_)
    {
        if (vector.empty())
        {
            vector.resize(initial_size);
            set(reinterpret_cast<Position>(vector.data()), vector.size());
        }
    }

    /// Append to vector instead of rewrite.
    WriteBufferFromVectorImpl(VectorType & vector_, AppendModeTag, size_t max_growth_step_ = 0)
        : WriteBuffer(nullptr, 0), vector(vector_), max_growth_step(max_growth_step_)
    {
        size_t old_size = vector.size();
        size_t size = (old_size < initial_size) ? initial_size
                                                : ((old_size < vector.capacity()) ? vector.capacity()
                                                                                  : grownSize(vector.capacity()));
        growTo(size);
        set(reinterpret_cast<Position>(vector.data() + old_size), (size - old_size) * sizeof(typename VectorType::value_type));
    }

    void restart(std::optional<size_t> max_capacity = std::nullopt)
    {
        if (max_capacity && vector.capacity() > max_capacity)
            VectorType(initial_size, ValueType()).swap(vector);
        else if (vector.empty())
            vector.resize(initial_size);
        set(reinterpret_cast<Position>(vector.data()), vector.size());
        finalized = false;
        canceled = false;
        bytes = 0;
    }

protected:
    void finalizeImpl() override
    {
        vector.resize(
            ((position() - reinterpret_cast<Position>(vector.data())) /// NOLINT
                + sizeof(ValueType) - 1)  /// Align up. /// NOLINT
            / sizeof(ValueType));

        bytes += offset();

        /// Prevent further writes.
        set(nullptr, 0);
    }

private:
    void nextImpl() override
    {
        if (finalized)
            throw Exception(ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER, "WriteBufferFromVector is finalized");

        size_t old_size = vector.size();
        /// pos may not be equal to vector.data() + old_size, because WriteBuffer::next() can be used to flush data
        size_t pos_offset = pos - reinterpret_cast<Position>(vector.data());
        if (pos_offset == old_size)
        {
            growTo(grownSize(old_size));
        }
        internal_buffer = Buffer(reinterpret_cast<Position>(vector.data() + pos_offset), reinterpret_cast<Position>(vector.data() + vector.size()));
        working_buffer = internal_buffer;
    }

    /// Grow by doubling, but never by more than `max_growth_step` at once (0 = pure doubling).
    /// Bounds over-allocation for large buffers, e.g. a 512 MiB buffer becomes 512 MiB + step, not 1 GiB.
    size_t grownSize(size_t old_size) const
    {
        size_t new_size = old_size * size_multiplier;
        if (max_growth_step != 0 && new_size - old_size > max_growth_step)
            new_size = old_size + max_growth_step;
        return new_size;
    }

    /// Resize to exactly `new_size` when capping is on: `PODArray::resize` rounds the requested size up
    /// to the next power of two (via `reserve`), which would undo the cap. `resize_exact` allocates
    /// exactly. Types without `resize_exact` (`std::string`/`std::vector`) don't pow2-round, so plain
    /// `resize` is fine there.
    void growTo(size_t new_size)
    {
        if constexpr (requires { vector.resize_exact(new_size); })
        {
            if (max_growth_step != 0)
            {
                vector.resize_exact(new_size);
                return;
            }
        }
        vector.resize(new_size);
    }

    VectorType & vector;
    size_t max_growth_step = 0;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;
};

template<typename VectorType>
using WriteBufferFromVector = AutoFinalizedWriteBuffer<WriteBufferFromVectorImpl<VectorType>>;

}
