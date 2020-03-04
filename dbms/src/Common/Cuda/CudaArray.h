#pragma once

#include <memory>
#include <stdexcept>
#include <boost/noncopyable.hpp>
#include <cuda.h>
#include <cuda_runtime.h>

#include <Common/Cuda/CudaSafeCall.h>

template<class T>
class CudaArray : private boost::noncopyable
{
public:
    typedef T   ValueType;

    CudaArray(size_t sz_);

    //ISSUE __device__?
    bool    empty()const { return sz == 0; }
    size_t  getSize()const { return sz; }
    size_t  getMemSize()const { return sz*sizeof(T); }
    T       *getData()const { return d; }

    ~CudaArray();
protected:
    size_t  sz;
    T       *d;
};

template<class T>
CudaArray<T>::CudaArray(size_t sz_) : sz(sz_)
{
    CUDA_SAFE_CALL( cudaMalloc((void**)&d, sz*sizeof(T)) );
}

template<class T>
CudaArray<T>::~CudaArray()
{
    CUDA_SAFE_CALL_NOTHROW( cudaFree(d) );
}

template<class T>
using CudaArrayPtr = std::shared_ptr<CudaArray<T>>;
