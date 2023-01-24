#pragma once

#include <string.h>
#include <algorithm>
#include <type_traits>


/** Returns value `from` converted to type `To` while retaining bit representation.
  * `To` and `From` must satisfy `CopyConstructible`.
  * In contrast to std::bit_cast can cast types of different width.
  */
template <typename To, typename From>
std::decay_t<To> bit_cast(const From & from)
{
    To res {};
    memcpy(static_cast<void*>(&res), &from, std::min(sizeof(res), sizeof(from)));
    return res;
}
