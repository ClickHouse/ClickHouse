#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <string>

/// String that can throw MEMORY_LIMIT_EXCEEDED
/// Use it when the string may require significant memory.
class StrictString : public std::basic_string<char, std::char_traits<char>, AllocatorWithMemoryTracking<char>>
{
    using std::basic_string<char, std::char_traits<char>, AllocatorWithMemoryTracking<char>>::basic_string;
};
