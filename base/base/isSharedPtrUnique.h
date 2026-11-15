#pragma once

#include <memory>

template <typename T>
bool isSharedPtrUnique(const std::shared_ptr<T> & ptr)
{
    return ptr.use_count() == 1;
}
