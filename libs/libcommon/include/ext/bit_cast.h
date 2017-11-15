#pragma once

#include <string.h>
#include <algorithm>
#include <type_traits>


namespace ext
{
    /** \brief Returns value `from` converted to type `To` while retaining bit representation.
      *    `To` and `From` must satisfy `CopyConstructible`.
      */
    template <typename To, typename From>
    std::decay_t<To> bit_cast(const From & from)
    {
        To res {};
        memcpy(&res, &from, std::min(sizeof(res), sizeof(from)));
        return res;
    };

    /** \brief Returns value `from` converted to type `To` while retaining bit representation.
      *    `To` and `From` must satisfy `CopyConstructible`.
      */
    template <typename To, typename From>
    std::decay_t<To> safe_bit_cast(const From & from)
    {
        static_assert(sizeof(To) == sizeof(From), "bit cast on types of different width");
        return bit_cast<To, From>(from);
    };
}
