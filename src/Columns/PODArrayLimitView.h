#pragma once

#include <cstddef>
#include <cstdio>
#include <memory>
#include "Columns/BufferFWD.h"
#include "IBuffer.h"

#include <IO/BufferWithOwnMemory.h>

namespace DB
{

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_, size_t pad_left_> // NOLINT
class PODArrayLimitView : public IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>
{
protected:
    using Base = IBuffer<T, initial_bytes, TAllocator, pad_right_, pad_left_>;
    static constexpr size_t ELEMENT_SIZE = sizeof(T);
public:
    using value_type = T;
    using iterator = T *;
    using const_iterator = const T *;
private:
    std::shared_ptr<PaddedBuffer<T>> memory_ptr; // points to memory to prolong its lifetime

public:
    // The duty to guarantee left padding is on code writer
    PODArrayLimitView(std::shared_ptr<PaddedBuffer<T>> memory, size_t start, size_t length) : memory_ptr(std::move(memory)) {
        Base::c_start = reinterpret_cast<char *>(memory_ptr->begin()) + start * ELEMENT_SIZE;
        Base::c_end = Base::c_start + length * ELEMENT_SIZE;
        Base::c_end_of_storage = Base::c_end;
        (void)printf("Done cut, length: %zu, current size: %zu\n", length, this->size());
    }

    ~PODArrayLimitView() override {
        Base::c_start = Base::null;
        Base::c_end = Base::null;
        Base::c_end_of_storage = Base::null;
    }

    std::shared_ptr<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>> getOwningBuffer() final {
        auto owning_buffer = std::make_shared<PODArrayOwning<T, initial_bytes, TAllocator, pad_right_, pad_left_>>();
        owning_buffer->resize(this->size());
        owning_buffer->insert_assume_reserved(this->begin(), this->end());
        return owning_buffer;
    }
};

}
