#include <Common/Cuda/cudaInitDevice.h>
#include <Common/Cuda/CudaHostPinnedMemPool.h>

#ifdef _DEBUG
#include <iostream>
#endif

void cudaInitDevice(int dev_number, size_t pinned_pool_size)
{
#ifdef _DEBUG
    std::cout << "cudaInitDevice: dev_number = " << dev_number << ", pinned_pool_size = " << pinned_pool_size << std::endl;
#endif
    CUDA_SAFE_CALL(cudaSetDevice(dev_number));
    CudaHostPinnedMemPool::instance().init(pinned_pool_size);
}
