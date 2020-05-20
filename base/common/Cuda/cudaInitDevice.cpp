#include <Common/Cuda/cudaInitDevice.h>
#include <Common/Cuda/CudaHostPinnedMemPool.h>

void cudaInitDevice(int dev_number, size_t pinned_pool_size)
{   
    printf("cudaInitDevice: dev_number = %u, pinned_pool_size = %u", dev_number, pinned_pool_size);
    fflush(stdout);
    CUDA_SAFE_CALL(cudaSetDevice(dev_number));
    CudaHostPinnedMemPool::instance().init(pinned_pool_size);
}
