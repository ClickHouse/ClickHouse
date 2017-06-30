#pragma once

#include <string.h>


template <typename T>
inline T unalignedLoad(const void * address)
{
    T res {};
    memcpy(&res, address, sizeof(res));
    return res;
}

template <typename T>
inline void unalignedStore(void * address, const T & src)
{
    memcpy(address, &src, sizeof(src));
}
