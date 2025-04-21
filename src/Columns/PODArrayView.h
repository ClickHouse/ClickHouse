#pragma once

#include <cstddef>
#include <memory>
#include "IBuffer.h"

#include <IO/BufferWithOwnMemory.h>

namespace DB
{

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_> // NOLINT
class PODArrayView : public IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>
{
protected:
    using Base = IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>;
    static constexpr size_t ELEMENT_SIZE = sizeof(T);
public:
    using value_type = T;
    using iterator = T *;
    using const_iterator = const T *;
private:
    std::shared_ptr<Memory<>> memory_ptr; // points to memory to prolong its lifetime

public:
    // The duty to guarantee left padding is on code writer
    PODArrayView(std::shared_ptr<Memory<>> memory, char * pos, size_t n) : memory_ptr(std::move(memory)) {
        Base::c_start = pos;
        Base::c_end = reinterpret_cast<char *>(reinterpret_cast<T*>(pos) + n);
        Base::c_end_of_storage = Base::c_end;
    }

    ~PODArrayView() override {
        Base::c_start = Base::null;
        Base::c_end = Base::null;
        Base::c_end_of_storage = Base::null;
    }

    std::shared_ptr<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>> getOwningBuffer() final {
        auto owning_buffer = std::make_shared<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>>();
        auto & x = *owning_buffer;
        const size_t initial_size = x.size();
        x.resize(initial_size);
        read(reinterpret_cast<char*>(x.begin()), sizeof(T) * initial_size);
        // x.resize(initial_size + size / sizeof(typename ColumnVector<T>::ValueType));

        if constexpr (std::endian::native == std::endian::big && sizeof(T) >= 2)
            for (size_t i = initial_size; i < x.size(); ++i)
                transformEndianness<std::endian::big, std::endian::little>(x[i]);
        return owning_buffer;
    }

private:
    size_t read(char * to, size_t n)
    {
        size_t bytes_copied = 0;
        auto pos = Base::c_start;

        while (bytes_copied < n)
        {
            size_t bytes_to_copy = n - bytes_copied;
            ::memcpy(to + bytes_copied, pos, bytes_to_copy);
            pos += bytes_to_copy;
            bytes_copied += bytes_to_copy;
        }

        return bytes_copied;
    }
};

}
