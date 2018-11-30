#pragma once

#include <vector>

#include <IO/WriteBuffer.h>


namespace DB
{

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
        size_t old_size = vector.size();
        vector.resize(old_size * 2);
        internal_buffer = Buffer(reinterpret_cast<Position>(&vector[old_size]), reinterpret_cast<Position>(vector.data() + vector.size()));
        working_buffer = internal_buffer;
    }

public:
    WriteBufferFromVector(VectorType & vector_)
        : WriteBuffer(reinterpret_cast<Position>(vector_.data()), vector_.size()), vector(vector_)
    {
        if (vector.empty())
        {
            static constexpr size_t initial_size = 32;
            vector.resize(initial_size);
            set(reinterpret_cast<Position>(vector.data()), vector.size());
        }
    }

    void finish()
    {
        is_finished = true;
        vector.resize(
            ((position() - reinterpret_cast<Position>(vector.data()))
                + sizeof(typename VectorType::value_type) - 1)  /// Align up.
            / sizeof(typename VectorType::value_type));
    }

    ~WriteBufferFromVector() override
    {
        if (!is_finished)
            finish();
    }
};

}
