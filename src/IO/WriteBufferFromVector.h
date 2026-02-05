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
    explicit WriteBufferFromVectorImpl(VectorType & vector_)
        : WriteBuffer(reinterpret_cast<Position>(vector_.data()), vector_.size()), vector(vector_)
    {
        if (vector.empty())
        {
            vector.resize(initial_size);
            set(reinterpret_cast<Position>(vector.data()), vector.size());
        }
    }

    /// Append to vector instead of rewrite.
    WriteBufferFromVectorImpl(VectorType & vector_, AppendModeTag)
        : WriteBuffer(nullptr, 0), vector(vector_)
    {
        size_t old_size = vector.size();
        size_t size = (old_size < initial_size) ? initial_size
                                                : ((old_size < vector.capacity()) ? vector.capacity()
                                                                                  : vector.capacity() * size_multiplier);
        vector.resize(size);
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
            vector.resize(old_size * size_multiplier);
        }
        internal_buffer = Buffer(reinterpret_cast<Position>(vector.data() + pos_offset), reinterpret_cast<Position>(vector.data() + vector.size()));
        working_buffer = internal_buffer;
    }

    VectorType & vector;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;
};

template<typename VectorType>
using WriteBufferFromVector = AutoFinalizedWriteBuffer<WriteBufferFromVectorImpl<VectorType>>;

}
