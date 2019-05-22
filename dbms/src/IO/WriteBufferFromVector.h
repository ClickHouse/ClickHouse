#pragma once

#include <vector>

#include <IO/WriteBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

/** Writes data to existing std::vector or similar type. When not enough space, it doubles vector size.
  *
  * In destructor, vector is cutted to the size of written data.
  * You can call to 'finish' to resize earlier.
  *
  * The vector should live until this object is destroyed or until the 'finish' method is called.
  */
template <typename VectorType>
class WriteBufferFromVector : public WriteBuffer
{
private:
    VectorType & vector;
    bool is_finished = false;

    void nextImpl() override
    {
        if (is_finished)
            throw Exception("WriteBufferFromVector is finished", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

        size_t old_size = vector.size();
        vector.resize(old_size * 2);
        internal_buffer = Buffer(reinterpret_cast<Position>(vector.data() + old_size), reinterpret_cast<Position>(vector.data() + vector.size()));
        working_buffer = internal_buffer;
    }

    static constexpr size_t initial_size = 32;

public:
    WriteBufferFromVector(VectorType & vector_)
        : WriteBuffer(reinterpret_cast<Position>(vector_.data()), vector_.size()), vector(vector_)
    {
        if (vector.empty())
        {
            vector.resize(initial_size);
            set(reinterpret_cast<Position>(vector.data()), vector.size());
        }
    }

    /// Append to vector instead of rewrite.
    struct AppendModeTag {};
    WriteBufferFromVector(VectorType & vector_, AppendModeTag)
        : WriteBuffer(nullptr, 0), vector(vector_)
    {
        size_t old_size = vector.size();
        vector.resize(vector.capacity() < initial_size ? initial_size : vector.capacity());
        set(reinterpret_cast<Position>(vector.data() + old_size), (vector.size() - old_size) * sizeof(typename VectorType::value_type));
    }

    void finish()
    {
        if (is_finished)
            return;
        is_finished = true;
        vector.resize(
            ((position() - reinterpret_cast<Position>(vector.data()))
                + sizeof(typename VectorType::value_type) - 1)  /// Align up.
            / sizeof(typename VectorType::value_type));

        /// Prevent further writes.
        set(nullptr, 0);
    }

    bool isFinished() const { return is_finished; }

    void restart()
    {
        set(reinterpret_cast<Position>(vector.data()), vector.size());
        is_finished = false;
    }

    ~WriteBufferFromVector() override
    {
        if (!is_finished)
            finish();
    }
};

}
