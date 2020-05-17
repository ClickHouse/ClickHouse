#pragma once

template <bool ClearMemory>
class Allocator;

template <typename Base, size_t N = 64, size_t Alignment = 1>
class AllocatorWithStackMemory;
