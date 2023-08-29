#pragma once
/**
  * This file provides forward declarations for Allocator.
  */

template <bool clear_memory_, bool mmap_populate = false>
class Allocator;

template <typename Base, size_t N = 64, size_t Alignment = 1>
class AllocatorWithStackMemory;
