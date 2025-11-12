#pragma once

#include <string>
#include <Common/AllocatorWithMemoryTracking.h>

/// String that can throw MEMORY_LIMIT_EXCEEDED
/// Use it when the string may require significant memory.
class TrackedString : public std::basic_string<char, std::char_traits<char>, AllocatorWithMemoryTracking<char>>
{
    using std::basic_string<char, std::char_traits<char>, AllocatorWithMemoryTracking<char>>::basic_string;
};
