
#include <cstdio>

#include <Common/Cuda/cudaReadUnaligned.cuh>
#include <Common/Cuda/City_Hash/cudaCityHash.cuh>
#include <Common/Cuda/cudaCalcCityHash64.h>

__global__ void kerCalcHash(DB::UInt32 str_num, char *arr, DB::UInt32 *begs, bool interpret_as_lengths, DB::UInt32 *lens, DB::UInt64 *res_hash)
{
    DB::UInt32 i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < str_num)) return;

    DB::UInt32 len = lens[i], beg = begs[i];
    if (!interpret_as_lengths) --len;

    DB::UInt64 h = CityHash_v1_0_2_cuda::cudaCityHash64(&(arr[beg]), len);

    /// TODO make it optional
    if (h == 0xFFFFFFFFFFFFFFFF) h = 0x0000000000000000;

    res_hash[i] = h;
}

void cudaCalcCityHash64(DB::UInt32 str_num, char *buf, bool interpret_as_lengths, DB::UInt32 *lens, 
                        DB::UInt32 *offsets, DB::UInt64 *res_hash, cudaStream_t stream)
{
    kerCalcHash<<<(str_num/256)+1,256,0,stream>>>(str_num, buf, offsets, interpret_as_lengths, lens, res_hash);
}