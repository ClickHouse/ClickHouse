#pragma once

#include <cuda.h>
#include <cuda_runtime.h>
#include <cstdint>

#include <Core/Cuda/Types.h>

void cudaCalcMurmurHash64(DB::UInt32 str_num, char *buf, bool interpret_as_lengths, DB::UInt32 *lens, 
                          DB::UInt32 *offsets, unsigned int seed, DB::UInt64 *res_hash, 
                          cudaStream_t stream = 0);
