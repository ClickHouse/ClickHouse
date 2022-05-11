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
    size_t key_size = value.size;
    char * place_for_key = arena.alloc(key_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(value.data), key_size);
    StringRef result{place_for_key, key_size};

    return result;
}
