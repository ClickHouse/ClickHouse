#pragma once

#include <cstring>
#include <algorithm>
#include <type_traits>


/** Returns value `from` converted to type `To` while retaining bit representation.
  * `To` and `From` must satisfy `CopyConstructible`.
  * In contrast to std::bit_cast can cast types of different width.
  */
template <typename To, typename From>
std::decay_t<To> bit_cast(const From & from)
{
    /**
     * Assume the source value is 0xAABBCCDD (i.e. sizeof(from) == 4). 
     * Its BE representation is 0xAABBCCDD, the LE representation is 0xDDCCBBAA. 
     * Further assume, sizeof(res) == 8 and that res is initially zeroed out.
     * With LE, the result after bit_cast is 0xDDCCBBAA00000000 (no change to the previous behavior).
     * With BE, we previously produced 0xAABBCCDD00000000 and we now produce 0x00000000AABBCCDD
     */
    To res {};
    static_assert(sizeof(From) <= sizeof(To));
    if constexpr (std::endian::native == std::endian::little)
      memcpy(static_cast<void*>(&res), &from, std::min(sizeof(res), sizeof(from)));
    else
    {
      uint32_t offset = sizeof(res) - sizeof(to);
      memcpy(reinterpret_cast<char *>(&res) + offset, &from, std::min(sizeof(res), sizeof(from)));
    }
    return res;
}
