// testvector.h
//
// The MIT License (MIT)
//
// Copyright (c) 2015 J. Andrew Rogers
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#ifndef METROHASH_TESTVECTOR_H
#define METROHASH_TESTVECTOR_H

#include "metrohash.h"


typedef void (*HashFunction) (const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * hash);

struct TestVectorData 
{
	HashFunction function;
	uint32_t bits;
	const char * key;
	uint32_t seed;
	uint8_t  hash[64];
};

// The test vector string is selected such that it will properly exercise every
// internal branch of the hash function. Currently that requires a string with
// a length of (at least) 63 bytes.

static const char * test_key_63 = "012345678901234567890123456789012345678901234567890123456789012";

const TestVectorData TestVector [] = 
{
	// seed = 0
	{ metrohash64_1,      64, test_key_63, 0, "658F044F5C730E40" },
	{ metrohash64_2,      64, test_key_63, 0, "073CAAB960623211" },
	{ metrohash128_1,    128, test_key_63, 0, "ED9997ED9D0A8B0FF3F266399477788F" },
	{ metrohash128_2,    128, test_key_63, 0, "7BBA6FE119CF35D45507EDF3505359AB" },
	{ metrohash128crc_1, 128, test_key_63, 0, "B329ED67831604D3DFAC4E4876D8262F" },
	{ metrohash128crc_2, 128, test_key_63, 0, "0502A67E257BBD77206BBCA6BBEF2653" },

	// seed = 1
	{ metrohash64_1,      64, test_key_63, 1, "AE49EBB0A856537B" },
	{ metrohash64_2,      64, test_key_63, 1, "CF518E9CF58402C0" },
	{ metrohash128_1,    128, test_key_63, 1, "DDA6BA67F7DE755EFDF6BEABECCFD1F4" },
	{ metrohash128_2,    128, test_key_63, 1, "2DA6AF149A5CDBC12B09DB0846D69EF0" },
	{ metrohash128crc_1, 128, test_key_63, 1, "E8FAB51AF19F18A7B10D0A57D4276DF2" },
	{ metrohash128crc_2, 128, test_key_63, 1, "2D54F87181A0CF64B02C50D95692BC19" },
};



#endif // #ifndef METROHASH_TESTVECTOR_H
