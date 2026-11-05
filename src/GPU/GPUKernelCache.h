#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <cuda_runtime_api.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>
#include <vector>

namespace DB::GPU
{

struct GPUKernel : private boost::noncopyable
{
    std::string name;
    cudaLibrary_t library = nullptr;
    cudaKernel_t kernel = nullptr;
    uint32_t block_size = 0;
    std::vector<GPUElementType> column_types;
    size_t ptx_size = 0;

    static std::shared_ptr<GPUKernel> load(const std::string & name, const std::string & ptx, std::vector<GPUElementType> column_types);

    ~GPUKernel();
};

struct GPUKernelWeight
{
    size_t operator()(const GPUKernel & kernel) const { return kernel.ptx_size; }
};

class GPUKernelCache : public CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>
{
public:
    GPUKernelCache();
};

}

#endif
