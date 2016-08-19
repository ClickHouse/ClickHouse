#pragma once

#include <DB/Core/Defines.h>
#include <string.h>


template <typename T>
inline T unalignedLoad(const void * address)
{
	T res {};
	memcpy(&res, address, sizeof(res));
	return res;
}
