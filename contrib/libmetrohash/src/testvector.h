// testvector.h
//
// Copyright 2015-2018 J. Andrew Rogers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// The hash assumes a little-endian architecture. Treating the hash results
// as an array of uint64_t should enable conversion for big-endian implementations.
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
