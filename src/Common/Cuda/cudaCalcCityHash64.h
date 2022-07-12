#pragma once

#include <cstdint>
#include <cuda.h>
#include <cuda_runtime.h>

#include <base/types.h>

void cudaCalcCityHash64(
    DB::UInt32 str_num,
    char * buf,
    bool interpret_as_lengths,
    DB::UInt32 * lens,
    DB::UInt32 * offsets,
    DB::UInt64 * res_hash,
    cudaStream_t stream = 0);
