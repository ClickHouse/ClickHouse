#include <stdexcept>
#include <cub/cub.cuh>

#include <Common/Cuda/cudaReadUnaligned.cuh>
#include <Common/Cuda/cudaMurmurHash64.cuh>
#include <Common/HashTable/Cuda/CudaStringsHashTable.h>

typedef CudaStringsHashTable::SizeType  SizeType;
typedef CudaStringsHashTable::HashType  HashType;
typedef CudaStringsHashTable::Pointer   Pointer;

CudaStringsHashTable::CudaStringsHashTable(SizeType buckets_num_, SizeType str_buf_max_sz_) : 
    buckets_num(buckets_num_), str_buf_max_sz(str_buf_max_sz_), str_buf_sz(0)
{
    hashes = CudaArrayPtr<HashType>(new CudaArray<HashType>(buckets_num));
    lens = CudaArrayPtr<SizeType>(new CudaArray<SizeType>(buckets_num));
    offsets = CudaArrayPtr<SizeType>(new CudaArray<SizeType>(buckets_num));
    pointers = CudaArrayPtr<Pointer>(new CudaArray<Pointer>(buckets_num));
    str_buf = CudaArrayPtr<char>(new CudaArray<char>(str_buf_max_sz));
    buf1 = CudaArrayPtr<SizeType>(new CudaArray<SizeType>(buckets_num));
    buf2 = CudaArrayPtr<SizeType>(new CudaArray<SizeType>(buckets_num));
    buf4_sz = 0;
    cub::DeviceScan::ExclusiveSum(nullptr, buf4_sz, buf1->getData(), buf2->getData(), buckets_num);
    buf4 = CudaArrayPtr<char>(new CudaArray<char>(buf4_sz));
    cuda_total_added_num = CudaArrayPtr<SizeType>(new CudaArray<SizeType>(1));
    host_total_added_num = CudaHostPinnedArrayPtr<SizeType>(new CudaHostPinnedArray<SizeType>(1));
    host_table_is_full_flag = CudaHostPinnedArrayPtr<bool>(new CudaHostPinnedArray<bool>(1));
}

__global__ void kerErase(SizeType buckets_num, HashType *hashes, SizeType *table_lens, Pointer *ptrs)
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < buckets_num)) return;
    hashes[i] = DBMS_CUDA_EMPTY_HASH_VAL;
    table_lens[i] = DBMS_CUDA_EMPTY_LEN_VAL;
    ptrs[i] = nullptr;
}

__global__ void kerFillZero(SizeType n, SizeType *arr)
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < n)) return;   
    arr[i] = 0;
}

/// TODO it's just temporal!!
__device__ bool table_is_full = false;

/// TODO try to store own string (or part of it) in registers
__global__ void kerAddData(SizeType str_num, char *buf, SizeType *offsets, SizeType *lens, unsigned int seed,
                           SizeType buckets_num, HashType *hashes, SizeType *table_lens, Pointer *ptrs,
                           SizeType *res_buckets, SizeType *new_strings_lens)
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < str_num)) return;

    SizeType len = lens[i], offset = offsets[i];
    if (len == DBMS_CUDA_EMPTY_LEN_VAL) return;

    HashType h = cudaMurmurHash64(&(buf[offset]), len, seed);

    /// empty hash value collision
    if (h == DBMS_CUDA_EMPTY_HASH_VAL) h++;

    SizeType    bucket_i = h%buckets_num;
    bool        hit = false, our_string = false;
    /// TODO some kind of total fill check
    while (!hit) {
        HashType   x = hashes[bucket_i];
        if (x == DBMS_CUDA_EMPTY_HASH_VAL) {
            HashType    old = atomicCAS((unsigned long long int*)&hashes[bucket_i], (unsigned long long int)DBMS_CUDA_EMPTY_HASH_VAL, (unsigned long long int)h);
            hit = ((old == DBMS_CUDA_EMPTY_HASH_VAL)||(old == h));
        } else {
            hit = (x == h);
        } 

        /// here hit==true only hash coincidence, not strings
        if (hit) {
            SizeType    len_in_bucket = table_lens[bucket_i];
            if (len_in_bucket == DBMS_CUDA_EMPTY_LEN_VAL) {
                SizeType    old_len_in_bucket = atomicCAS(&table_lens[bucket_i], DBMS_CUDA_EMPTY_LEN_VAL, len);
                if (old_len_in_bucket == DBMS_CUDA_EMPTY_LEN_VAL) {
                    len_in_bucket = len;
                } else {
                    len_in_bucket = old_len_in_bucket;
                }
            }
            if (len_in_bucket != len) hit = false;
        }

        /// here hit==true only hash and length coincidence, not strings        
        if (hit) {
            /// get current bucket string
            Pointer     ptr = ptrs[bucket_i];
            if (ptr == nullptr) {
                Pointer                 old_ptr;
                old_ptr = reinterpret_cast<Pointer>(atomicCAS((unsigned long long int*)&ptrs[bucket_i], 
                    reinterpret_cast<unsigned long long int>(nullptr),
                    reinterpret_cast<unsigned long long int>(&(buf[offset]))));
                if (old_ptr == nullptr) {
                    /// ISSUE should we optimize this case somehow? i.e. we will compare strings with our selves
                    ptr = (Pointer)&(buf[offset]);
                    our_string = true;
                } else {
                    ptr = old_ptr;
                }
            }

            /// compare strings
            const char      *data = &(buf[offset]);
            const char      *end = data + len;
            const char      *data_next = ptr;
            const char      *end_next = data_next + len;
            bool            is_first_read = true,
                            is_first_read_next = true;
            DB::UInt64      tmp_buf, tmp_buf_next;

            while(data != end)
            {
                DB::UInt64  v = cudaReadStringUnaligned64(is_first_read, tmp_buf, data, end),
                            v_next = cudaReadStringUnaligned64(is_first_read_next, tmp_buf_next, data_next, end_next);
                if (v != v_next) hit = false;   //ISSUE break?   
            }
        }

        /// linear search, if failed
        if (!hit) {
            bucket_i = (bucket_i+1)%buckets_num;
            /// TEST
            if (bucket_i == h%buckets_num) {
                //printf("table filled; fail;\n");
                table_is_full = true;
                return;
            }
            /// TEST END
        }
    }

    res_buckets[i] = bucket_i;

    if (our_string) new_strings_lens[bucket_i] = (((len-1)/8)+1)*8;
}

__global__ void kerCopyAddedStrings( SizeType str_num, char *buf, SizeType *offsets, SizeType *lens, SizeType *buckets, 
                                     SizeType old_str_buf_sz, SizeType *new_strings_offsets, 
                                     Pointer *hash_table_ptrs, char *hash_table_str_buf )
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < str_num)) return;

    SizeType    len = lens[i], offset = offsets[i];
    if (len == DBMS_CUDA_EMPTY_LEN_VAL) return;
    SizeType    bucket_i = buckets[i];
    Pointer     hash_table_old_ptr = hash_table_ptrs[bucket_i],
                my_ptr = (Pointer)&(buf[offset]);
    if (hash_table_old_ptr == my_ptr) {
        SizeType    hash_table_offset = old_str_buf_sz + new_strings_offsets[bucket_i];

        const char     *data = &(buf[offset]);
        const char     *end = data + len;
        bool            is_first_read = true;
        DB::UInt64      tmp_buf;
        DB::UInt64     *data_res = (DB::UInt64 *)&(hash_table_str_buf[hash_table_offset]);

        while(data != end)
        {
            *data_res = cudaReadStringUnaligned64(is_first_read, tmp_buf, data, end);
            data_res++;
        }

        hash_table_ptrs[bucket_i] = &(hash_table_str_buf[hash_table_offset]);
    }
}

/// kind of stupid 1 thread kernal to calculate exclusive_scan total sum
__global__ void    kerCalcTotalAddedLen(SizeType buckets_num, SizeType *new_strings_offsets, SizeType *new_strings_lens,
                                        SizeType *res_total_added_len)
{
    *res_total_added_len = new_strings_offsets[buckets_num-1] + new_strings_lens[buckets_num-1];
}

__global__ void    kerCalcOffsets(SizeType buckets_num, Pointer str_buf, Pointer *ptrs, SizeType *res_offsets )
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < buckets_num)) return;
    res_offsets[i] = (SizeType)(ptrs[i] - str_buf);
}

__global__ void    kerFillEmptyResBucket(SizeType elements_num, SizeType *res_buckets )
{
    SizeType i = blockIdx.x * blockDim.x + threadIdx.x;
    if (!(i < elements_num)) return;
    res_buckets[i] = ~((SizeType)0);
} 

void    CudaStringsHashTable::erase(cudaStream_t stream)
{
    kerErase<<<(buckets_num/256)+1,256,0,stream>>>(buckets_num, hashes->getData(), lens->getData(), pointers->getData());
    str_buf_sz = 0;
}

/// TODO offsets and lens interfere with class fields
void    CudaStringsHashTable::addData(SizeType str_num, char *buf, SizeType *offsets, SizeType *lens,
                                      SizeType *res_buckets, cudaStream_t stream)
{
    kerFillZero<<<(buckets_num/256)+1,256,0,stream>>>(buckets_num, buf1->getData());
    kerAddData<<<(str_num/256)+1,256,0,stream>>>(str_num, buf, offsets, lens, 0, 
        buckets_num, 
        hashes->getData(), 
        this->lens->getData(), 
        pointers->getData(), 
        res_buckets, 
        buf1->getData());
    CUDA_SAFE_CALL( cudaMemcpyFromSymbolAsync ( host_table_is_full_flag->getData(), table_is_full, 
        sizeof(bool), 0, cudaMemcpyDeviceToHost, stream ) );
    /// no need to call cub::DeviceScan::ExclusiveSum to know buffer size
    /// because we already called it first time in initialization
    cub::DeviceScan::ExclusiveSum(buf4->getData(), buf4_sz, buf1->getData(), buf2->getData(), buckets_num, stream);

    kerCopyAddedStrings<<<(str_num/256)+1,256,0,stream>>>( str_num, buf, offsets, lens, res_buckets, 
        str_buf_sz, buf2->getData(), pointers->getData(), str_buf->getData() );

    kerCalcTotalAddedLen<<<1,1,0,stream>>>( buckets_num, buf2->getData(), buf1->getData(), cuda_total_added_num->getData() );
    CUDA_SAFE_CALL( cudaMemcpyAsync ( host_total_added_num->getData(), cuda_total_added_num->getData(),
            sizeof(SizeType), cudaMemcpyDeviceToHost, stream ) );
    CUDA_SAFE_CALL( cudaStreamSynchronize ( stream ) );
    if ((*host_table_is_full_flag)[0]) {
        throw std::runtime_error("CudaStringsHashTable::addData: table is full");
    }
    if (str_buf_sz + (*host_total_added_num)[0] > str_buf_max_sz) throw std::runtime_error("CudaStringsHashTable::addData: table string buffer is full");
    str_buf_sz += (*host_total_added_num)[0];
    //std::cout << "CudaStringsHashTable::addData: calced total new strings length = " << (*host_total_added_num)[0]  << std::endl;
}

void    CudaStringsHashTable::calcOffsets(cudaStream_t stream)
{
    kerCalcOffsets<<<(buckets_num/256)+1,256,0,stream>>>(buckets_num, str_buf->getData(), pointers->getData(), offsets->getData() );
}

void    CudaStringsHashTable::mergeToOtherTable(CudaStringsHashTablePtr table, SizeType *res_buckets, cudaStream_t stream)
{
    calcOffsets(stream);
    kerFillEmptyResBucket<<<(buckets_num/256)+1,256,0,stream>>>(buckets_num, res_buckets );
    table->addData(buckets_num, str_buf->getData(), offsets->getData(), lens->getData(), res_buckets, stream);
}