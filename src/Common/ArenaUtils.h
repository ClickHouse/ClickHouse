#pragma once

#include <string.h>
#include <string>

/** Copy string value into Arena.
  * Arena should support method:
  * char * alloc(size_t size).
  */
template <typename Arena>
inline std::string_view copyStringInArena(Arena & arena, std::string_view value)
{
    size_t value_size = value.size();
    char * place_for_key = arena.alloc(value_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(value.data()), value_size);
    return std::string_view{place_for_key, value_size};
}
