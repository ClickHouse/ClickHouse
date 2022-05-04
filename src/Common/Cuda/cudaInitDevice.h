#pragma once

#include <cuda.h>

#include <Common/Cuda/CudaSafeCall.h>

void cudaInitDevice(int dev_number, size_t pinned_pool_size);
