#pragma once

#include <cstddef>

template <typename A, typename B, typename ResultType>
extern void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size);
