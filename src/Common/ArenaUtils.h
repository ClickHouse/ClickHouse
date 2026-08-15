#pragma once

#include <string.h>
#include <string>
#include <base/StringRef.h>

/** Copy string value into Arena.
  * Arena should support method:
  * char * alloc(size_t size).
  */
template <typename Arena>
inline StringRef copyStringInArena(Arena & arena, StringRef value)
{
    size_t value_size = value.size;
    char * place_for_key = arena.alloc(value_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(value.data), value_size);
    StringRef result{place_for_key, value_size};

    return result;
}
