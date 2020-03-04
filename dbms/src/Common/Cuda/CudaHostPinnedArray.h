#pragma once

#include <memory>
#include <stdexcept>
#include <cuda.h>
#include <cuda_runtime.h>
#include <boost/noncopyable.hpp>

#include <Common/Cuda/CudaSafeCall.h>
#include <Common/Cuda/CudaHostPinnedAllocator.h>

//TODO make it non-copyable
template<class T, typename TAllocator = CudaHostPinnedAllocator>
class CudaHostPinnedArray : private boost::noncopyable, private TAllocator
{
public:
    typedef T   ValueType;

    CudaHostPinnedArray(size_t sz_);

    bool    empty()const { return sz == 0; }
    size_t  getSize()const { return sz; }
    size_t  getMemSize()const { return sz*sizeof(T); }
    T       *getData()const { return d; }
    T       &operator[](size_t i) { return d[i]; }
    const T &operator[](size_t i)const { return d[i]; }

    ~CudaHostPinnedArray();
protected:
    size_t  sz;
    T       *d;
};

template<class T, typename TAllocator>
CudaHostPinnedArray<T,TAllocator>::CudaHostPinnedArray(size_t sz_) : sz(sz_)
{
    d = (T*)TAllocator::alloc(sz*sizeof(T));
    //CUDA_SAFE_CALL( cudaMallocHost((void**)&d, sz*sizeof(T)) );
}

template<class T, typename TAllocator>
CudaHostPinnedArray<T,TAllocator>::~CudaHostPinnedArray()
{
    TAllocator::free(d, sz*sizeof(T));
    //CUDA_SAFE_CALL_NOTHROW( cudaFreeHost(d) );
}

template<class T, typename TAllocator = CudaHostPinnedAllocator>
using CudaHostPinnedArrayPtr = std::shared_ptr<CudaHostPinnedArray<T,TAllocator>>;
