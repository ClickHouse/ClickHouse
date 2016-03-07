#pragma once


#if !defined(__x86_64__)

inline unsigned int _bit_scan_reverse(unsigned int x)
{
	return sizeof(unsigned int) * 8 - 1 - __builtin_clz(x);
}

#endif
