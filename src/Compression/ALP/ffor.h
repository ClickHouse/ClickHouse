#pragma once

// Copyright (c) 2024 Azim Afroozeh, CWI Database Architectures Group
// Licensed under the MIT License.
// See LICENSE file in the original project for details.
//
// This file is based on code from:
// https://github.com/cwida/FastLanes
//
// Modifications: refactoring to be compliant with the CH codebase and style.

#include <cassert>
#include <cstdint>

namespace DB::ALP::FFOR {

void ffor(const uint64_t* __restrict in, uint64_t* __restrict out, uint8_t bw, const uint64_t* __restrict a_base_p);
void unffor(const uint64_t* __restrict a_in_p, uint64_t* __restrict a_out_p, uint8_t bw, const uint64_t* __restrict a_base_p);

inline uint32_t calculateBitpackedSize(uint8_t bit_width)
{
    assert(bit_width <= 64);
    return bit_width * 1024 / 8;
}

}
