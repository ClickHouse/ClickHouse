#pragma once

#include <cuda.h>
#include <cuda_runtime.h>

#include <Core/Cuda/Types.h>

/** This function is highly machine dependent. 
  * First, it uses explicitly type sizes. Second, it exploits little endian
  */
inline __device__ DB::UInt64     cudaReadStringUnaligned64(bool &is_first_read, DB::UInt64 &tmp_buf, const char *&ptr_curr, const char *ptr_end)
{
    uintptr_t   uint_ptr_curr = (uintptr_t)ptr_curr;
    if (is_first_read) 
        tmp_buf = *((const DB::UInt64*)(uint_ptr_curr & ~(uintptr_t(7))));
    
    DB::UInt64      res = tmp_buf >> 8*(uint_ptr_curr & uintptr_t(7));
    uintptr_t   uint_ptr_end_m1 = (uintptr_t)(ptr_end-1);
    if ((uint_ptr_curr & ~(uintptr_t(7))) != (uint_ptr_end_m1 & ~(uintptr_t(7)))) {
        tmp_buf = *(((const DB::UInt64*)(uint_ptr_curr & ~(uintptr_t(7))))+1);
        res |= tmp_buf << 8*(8-(uint_ptr_curr & uintptr_t(7)));
    }
    if (ptr_end - ptr_curr < 8) {
        res &= (DB::UInt64(1) << 8*(ptr_end - ptr_curr))-1;
    }
    is_first_read = false;
    if (ptr_curr+8 < ptr_end) 
        ptr_curr = ptr_curr+8; 
    else 
        ptr_curr = ptr_end;
    return res;
}