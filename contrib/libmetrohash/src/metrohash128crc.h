// metrohash128crc.h
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

#ifndef METROHASH_METROHASH_128_CRC_H
#define METROHASH_METROHASH_128_CRC_H

#include <stdint.h>

// Legacy 128-bit hash functions
void metrohash128crc_1(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out);
void metrohash128crc_2(const uint8_t * key, uint64_t len, uint32_t seed, uint8_t * out);


#endif // #ifndef METROHASH_METROHASH_128_CRC_H
