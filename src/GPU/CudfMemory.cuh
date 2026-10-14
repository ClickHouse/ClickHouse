#pragma once

#include <cuda_runtime_api.h>

namespace DB::GPU
{

__host__ void setCudfMemoryPool(cudaMemPool_t pool);

__host__ void resetCudfMemoryPool();

}
