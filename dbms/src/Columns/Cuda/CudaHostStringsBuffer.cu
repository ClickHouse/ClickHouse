
#define USE_PARALLEL_MEMCPY

#include <iostream>
#include <chrono>
//#include "cuda_safe_call.h"
//#include "ompMemcpy.h"
#include "parallelMemcpy.h"
#include "CudaHostStringsBuffer.h"

namespace DB
{

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

CudaHostStringsBuffer::CudaHostStringsBuffer(size_t max_str_num_, size_t max_sz_,
    bool has_lens_, bool has_offsets_, bool has_offsets64_) : 
    max_str_num(max_str_num_), max_sz(max_sz_), str_num(0), sz(0),
    has_lens(has_lens_), has_offsets(has_offsets_), has_offsets64(has_offsets64_)
{
    buf = CudaHostPinnedArrayPtr<char>(new CudaHostPinnedArray<char>(max_sz));
    if (has_lens)
        lens = CudaHostPinnedArrayPtr<UInt32>(new CudaHostPinnedArray<UInt32>(max_str_num));
    if (has_offsets)
        offsets = CudaHostPinnedArrayPtr<UInt32>(new CudaHostPinnedArray<UInt32>(max_str_num));
    if (has_offsets64)
        offsets64 = CudaHostPinnedArrayPtr<UInt64>(new CudaHostPinnedArray<UInt64>(max_str_num));
}

bool    CudaHostStringsBuffer::hasSpace(size_t str_num_, size_t str_buf_sz_)const
{
    //std::cout << "hasSpace check" << std::endl;
    //std::cout << "str_num = " << str_num << " str_num_ = " << str_num_ << " max_str_num = " << max_str_num << std::endl;
    //std::cout << "sz = " << sz << " str_buf_sz_ = " << str_buf_sz_ << " max_sz = " << max_sz << std::endl;
    if (str_num + str_num_ > max_str_num) return false;
    if (sz + str_buf_sz_ > max_sz) return false;
    return true;
}

void    CudaHostStringsBuffer::addData(size_t str_num_, size_t str_buf_sz_, const char *str_buf_, const UInt64 *offsets_, size_t memcpy_threads_num_)
{
    auto                                        host_e1 = steady_clock::now();
#ifdef USE_PARALLEL_MEMCPY
    parallelMemcpy( (char*)(buf->getData() + sz), (const char*)str_buf_, str_buf_sz_, memcpy_threads_num_ );
    parallelMemcpy( (char*)(offsets64->getData() + str_num), (const char*)offsets_, str_num_*sizeof(UInt64), memcpy_threads_num_ );
#else
    memcpy( buf->getData() + sz, str_buf_, str_buf_sz_ );
    memcpy( offsets64->getData() + str_num, offsets_, str_num_*sizeof(UInt64) );
#endif
    auto                                        host_e2 = steady_clock::now();
    auto                                        host_t = duration_cast<milliseconds>(host_e2 - host_e1);
    //std::cout << "CudaHostStringsBuffer::addData: memcpy time " << host_t.count() << "ms" << std::endl;
    str_num += str_num_; sz += str_buf_sz_;
    blocks_sizes.push_back(str_num_);
    blocks_buf_sizes.push_back(str_buf_sz_);
}

void    CudaHostStringsBuffer::setSize(size_t str_num_, size_t sz_)
{
    str_num = str_num_; sz = sz_;
}

void    CudaHostStringsBuffer::reset()
{
    str_num = 0; sz = 0;
    blocks_sizes.clear(); 
    blocks_buf_sizes.clear();
}

}

