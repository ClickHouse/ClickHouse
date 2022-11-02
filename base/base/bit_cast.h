#pragma once

#include <cstring>
#include <algorithm>
#include <type_traits>
#include <base/unaligned.h>


/** Returns value `from` converted to type `To` while retaining bit representation.
  * `To` and `From` must satisfy `CopyConstructible`.
  * In contrast to std::bit_cast can cast types of different width.
  */
template <typename To, typename From>
std::decay_t<To> bit_cast(const From & from)
{
    To res {};
    if constexpr (std::endian::native == std::endian::little || !std::is_arithmetic_v<To>)
      memcpy(static_cast<void*>(&res), &from, std::min(sizeof(res), sizeof(from)));
    else
    {
      uint32_t offset = (sizeof(res) > sizeof(from)) ? (sizeof(res) - sizeof(from)) : 0;
      reverseMemcpy(reinterpret_cast<char *>(&res) + offset, &from, std::min(sizeof(res), sizeof(from)));
    }
    return res;
}
