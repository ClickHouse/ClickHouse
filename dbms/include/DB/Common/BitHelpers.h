#pragma once

inline unsigned int bit_scan_reverse(unsigned int x)
{
	return sizeof(unsigned int) * 8 - 1 - __builtin_clz(x);
}
