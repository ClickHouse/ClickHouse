#include <stdexcept>
#include <chrono>

#include <Common/Cuda/CudaSafeCall.h>
#include <Common/Cuda/cudaCalcMurmurHash64.h>
#include <Common/Cuda/cudaCalcMurmurHash64.h>

#include <Interpreters/Cuda/CudaStringsAggregator.h>
//#include "hash_sort_alg_magic_numbers.h"

using std::chrono::steady_clock;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace DB
{

CudaStringsAggregator::CudaStringsAggregator(int dev_number_, size_t chunks_num_, 
    UInt32 hash_table_max_size_, UInt32 hash_table_str_buffer_max_size_,
    UInt32 buffer_max_str_num_, UInt32 buffer_max_size_,
    CudaAggregateFunctionPtr aggregate_function_) : dev_number(dev_number_), aggregate_function(aggregate_function_)
{
    CUDA_SAFE_CALL(cudaSetDevice(dev_number));
    chunks.resize(chunks_num_);
    /// create cuda streams, allocate host and cuda buffers
    for (size_t i = 0;i < chunks.size();++i) {
        chunks[i] = WorkChunkInfoPtr(new WorkChunkInfo());
        CUDA_SAFE_CALL(cudaStreamCreate( &chunks[i]->stream ));
        //chunks[i]->stream = cudaStreamPerThread;
        chunks[i]->cuda_hash_table = CudaStringsHashTablePtr(
            new CudaStringsHashTable(hash_table_max_size_, hash_table_str_buffer_max_size_));
        chunks[i]->cuda_buffer_keys = CudaColumnStringPtr(
            new CudaColumnString(buffer_max_str_num_, buffer_max_size_));
        chunks[i]->cuda_buffer_vals = CudaColumnStringPtr(
            new CudaColumnString(buffer_max_str_num_, buffer_max_size_));
        chunks[i]->host_buffer_agg_res_keys = CudaHostStringsBufferPtr(
            new CudaHostStringsBuffer(buffer_max_str_num_, buffer_max_size_));
        chunks[i]->group_nums.resize(buffer_max_str_num_);
        chunks[i]->group_agg_res = CudaArrayPtr<char>(
            new CudaArray<char>(hash_table_max_size_ * aggregate_function->cudaSizeOfData()));
        chunks[i]->host_group_agg_res = CudaHostPinnedArrayPtr<char>(
            new CudaHostPinnedArray<char>(hash_table_max_size_ * aggregate_function->cudaSizeOfData()));
        chunks[i]->agg_tmp_buf = CudaArrayPtr<char>(
            new CudaArray<char>( aggregate_function->cudaSizeOfAddBulkInternalBuf(buffer_max_str_num_) ));
    }
    CUDA_SAFE_CALL(cudaStreamCreate( &copy_stream ));
    std::cout << "CudaStringsAggregator created" << std::endl;

    /*chunks.resize(1);
    chunks[0] = WorkChunkInfoPtr(new WorkChunkInfo());
    chunks[0]->host_group_agg_res = CudaHostPinnedArrayPtr<char>(
        new CudaHostPinnedArray<char>(10 * aggregate_function->cudaSizeOfData()));*/
}

struct ProcessChunkParams
{
    ProcessChunkParams(CudaStringsAggregator *agg_, size_t i_) : agg(agg_),i(i_) 
    {
    }

    CudaStringsAggregator   *agg;
    size_t                   i;
};

void callProcessChunk(ProcessChunkParams params)
{
    params.agg->processChunk(params.i);
}

void CudaStringsAggregator::startProcessing()
{
    /// start processing threads
    for (size_t i = 0;i < chunks.size();++i) {
        chunks[i]->cuda_processing_state = false;
        //chunks[i]->t = std::thread(&CudaStringsAggregator::processChunk, this, i);
        chunks[i]->t = std::thread(callProcessChunk, ProcessChunkParams(this, i));
    }
    /// set current buffer for data appending
    curr_filling_chunk = 0;
    is_vals_needed = aggregate_function->isDataNeeded();
}


void CudaStringsAggregator::queueData(size_t str_num, size_t str_buf_sz, const char *str_buf, const OffsetType *offsets,
                                      size_t vals_str_buf_sz, const char *vals_str_buf, const OffsetType *vals_offsets)
{
    //printf("CudaStringsAggregator::queueData: params %d %d %d %d\n", str_num, str_buf_sz, vals_str_buf_sz, memcpy_threads_num_);
    while(1) {
        if (tryQueueData(str_num, str_buf_sz, str_buf, offsets, vals_str_buf_sz, vals_str_buf, vals_offsets)) return;
    }
}

void CudaStringsAggregator::waitQueueData()const
{
    CUDA_SAFE_CALL( cudaStreamSynchronize ( copy_stream ) );
}

void CudaStringsAggregator::waitProcessed()
{
    {
        std::unique_lock<std::mutex> lck( chunks[curr_filling_chunk]->cuda_buffer_mtx );
        chunks[curr_filling_chunk]->cv_cuda_processing_end.wait( lck, [this]{return !chunks[curr_filling_chunk]->cuda_processing_state;} );

        if (!chunks[curr_filling_chunk]->cuda_buffer_keys->empty()) {
            chunks[curr_filling_chunk]->cuda_processing_state = true;
            chunks[curr_filling_chunk]->cv_buffer_append_end.notify_one();
        }
    }

    /// wait till host to gpu copy ends, 'send' empty buffer to signal end of data
    for (size_t i = 0;i < chunks.size();++i) {
        std::unique_lock<std::mutex> lck( chunks[i]->cuda_buffer_mtx );
        chunks[i]->cv_cuda_processing_end.wait( lck, [this,i]{return !chunks[i]->cuda_processing_state;} );
        if (!chunks[i]->cuda_buffer_keys->empty()) throw std::logic_error("CudaStringsAggregator: host buffer is not empty after transfer");
        //setting cuda_processing_state with empty buffer means end of processing
        chunks[i]->cuda_processing_state = true;
        chunks[i]->cv_buffer_append_end.notify_one();
    }

    /// wait for processes for termination
    for (size_t i = 0;i < chunks.size();++i) {
        chunks[i]->t.join();
    }

    /// combine data from different chunks
    for (size_t i = 1;i < chunks.size();++i) {
        chunks[i]->cuda_hash_table->mergeToOtherTable(chunks[0]->cuda_hash_table, 
            thrust::raw_pointer_cast(chunks[i]->group_nums.data()), chunks[0]->stream);

        aggregate_function->cudaMergeBulk(chunks[0]->group_agg_res->getData(), 
            chunks[i]->cuda_hash_table->getBucketsNum(), chunks[i]->group_agg_res->getData(), 
            thrust::raw_pointer_cast(chunks[i]->group_nums.data()), chunks[0]->stream);
    }

    CUDA_SAFE_CALL( cudaMemcpyAsync ( chunks[0]->host_buffer_agg_res_keys->getBuf(), chunks[0]->cuda_hash_table->getStrBuf(), 
        chunks[0]->cuda_hash_table->getStrBufSz(), cudaMemcpyDeviceToHost, chunks[0]->stream ) );
    /// TODO get rid of sizeof(UInt32) in all following cudaMemcpyAsync!!
    CUDA_SAFE_CALL( cudaMemcpyAsync ( chunks[0]->host_buffer_agg_res_keys->getLens(), chunks[0]->cuda_hash_table->getLens(), 
        chunks[0]->cuda_hash_table->getBucketsNum()*sizeof(UInt32), 
        cudaMemcpyDeviceToHost, chunks[0]->stream ) );
    CUDA_SAFE_CALL( cudaMemcpyAsync ( chunks[0]->host_buffer_agg_res_keys->getOffsets(), chunks[0]->cuda_hash_table->getOffsets(), 
        chunks[0]->cuda_hash_table->getBucketsNum()*sizeof(UInt32), 
        cudaMemcpyDeviceToHost, chunks[0]->stream ) );
    CUDA_SAFE_CALL( cudaMemcpyAsync ( chunks[0]->host_group_agg_res->getData(), chunks[0]->group_agg_res->getData(),
        chunks[0]->cuda_hash_table->getBucketsNum()*aggregate_function->cudaSizeOfData(), 
        cudaMemcpyDeviceToHost, chunks[0]->stream ) );

    CUDA_SAFE_CALL( cudaStreamSynchronize ( chunks[0]->stream ) ); 

    auto                                        host_e1 = steady_clock::now();
    for (size_t j = 0;j < chunks[0]->cuda_hash_table->getBucketsNum();++j) 
    {
        UInt32                  len = chunks[0]->host_buffer_agg_res_keys->getLens()[j],
                                offset = chunks[0]->host_buffer_agg_res_keys->getOffsets()[j];
        if (len == DBMS_CUDA_EMPTY_LEN_VAL) continue;
        std::string             key_str(chunks[0]->host_buffer_agg_res_keys->getBuf() + offset, len-1);
        CudaAggregateDataPtr    res = chunks[0]->host_group_agg_res->getData() + j*aggregate_function->cudaSizeOfData();
        
        auto it = chunks[0]->agg_result.find(key_str);
        if (it == chunks[0]->agg_result.end())
        {
            chunks[0]->agg_result[key_str] = res;
        }
        else
        {
            throw std::logic_error("CudaStringsAggregator::waitProcessed: seems there are duplicates in GPU table");
        }
    }

    auto                                        host_e2 = steady_clock::now();
    auto                                        host_t = duration_cast<milliseconds>(host_e2 - host_e1);
    std::cout << "CudaStringsAggregator::waitProcessed: time for placing data into cpu hash table " << host_t.count() << "ms" << std::endl;
}


bool CudaStringsAggregator::tryQueueData(size_t str_num, size_t str_buf_sz, const char *str_buf, const OffsetType *offsets,
                                         size_t vals_str_buf_sz, const char *vals_str_buf, const OffsetType *vals_offsets)
{
    std::unique_lock<std::mutex> lck( chunks[curr_filling_chunk]->cuda_buffer_mtx );
    chunks[curr_filling_chunk]->cv_cuda_processing_end.wait( lck, [this]{return !chunks[curr_filling_chunk]->cuda_processing_state;} );
    
    if (chunks[curr_filling_chunk]->cuda_buffer_keys->hasSpace(str_num, str_buf_sz) &&
        chunks[curr_filling_chunk]->cuda_buffer_vals->hasSpace(str_num, vals_str_buf_sz) ) {
        chunks[curr_filling_chunk]->cuda_buffer_keys->addData(str_num, str_buf_sz, str_buf, offsets, copy_stream);
        if (is_vals_needed)
            chunks[curr_filling_chunk]->cuda_buffer_vals->addData(str_num, vals_str_buf_sz, vals_str_buf, vals_offsets, copy_stream);
        return true;
    } else {
        if (chunks[curr_filling_chunk]->cuda_buffer_keys->empty()) throw std::runtime_error("CudaStringsAggregator: seems there is not enough space in buffer");
        waitQueueData();
        chunks[curr_filling_chunk]->cuda_processing_state = true;
        chunks[curr_filling_chunk]->cv_buffer_append_end.notify_one();
        curr_filling_chunk = (curr_filling_chunk+1)%chunks.size();
        return false;
    }
}


__global__ void kerFillMaxHash( UInt32 str_num, UInt32 max_str_num, UInt64 *hashes )
{
    UInt32 i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < str_num) return;
    if (!(i < max_str_num)) return;

    hashes[i] = 0xFFFFFFFFFFFFFFFF;
}


void CudaStringsAggregator::processChunk(size_t i)
{
    CUDA_SAFE_CALL(cudaSetDevice(dev_number));

    chunks[i]->cuda_hash_table->erase(chunks[i]->stream);
    aggregate_function->cudaInitAggregateData(chunks[i]->cuda_hash_table->getBucketsNum(), 
        chunks[i]->group_agg_res->getData(), chunks[i]->stream);

    while (1) {
        {
            std::cout << "CudaStringsAggregator::processChunk(i = " << i << "): waiting data..." << std::endl;
            std::unique_lock<std::mutex> lck( chunks[i]->cuda_buffer_mtx );
            chunks[i]->cv_buffer_append_end.wait( lck, [this,i]{return chunks[i]->cuda_processing_state;} );
            /// we agreed that empty buffer means end of processing
            if (chunks[i]->cuda_buffer_keys->empty()) break;

            std::cout << "CudaStringsAggregator::processChunk(i = " << i << "): calc Lengths" << std::endl;
            chunks[i]->cuda_buffer_keys->calcLengths( chunks[i]->stream );
            if (is_vals_needed)
                chunks[i]->cuda_buffer_vals->calcLengths( chunks[i]->stream );                

            size_t str_num = chunks[i]->cuda_buffer_keys->getStrNum();
            chunks[i]->cuda_hash_table->addData(str_num, chunks[i]->cuda_buffer_keys->getBuf(), 
                chunks[i]->cuda_buffer_keys->getOffsets(), chunks[i]->cuda_buffer_keys->getLens(), 
                thrust::raw_pointer_cast(chunks[i]->group_nums.data()), chunks[i]->stream);
            aggregate_function->cudaAddBulk(chunks[i]->group_agg_res->getData(), chunks[i]->cuda_buffer_vals,
                str_num, thrust::raw_pointer_cast(chunks[i]->group_nums.data()), 
                chunks[i]->agg_tmp_buf->getData(), chunks[i]->stream);

            chunks[i]->cuda_buffer_keys->reset();
            chunks[i]->cuda_buffer_vals->reset();
            chunks[i]->cuda_processing_state = false;
            chunks[i]->cv_cuda_processing_end.notify_one();
        }
    }

    chunks[i]->cuda_hash_table->calcOffsets(chunks[i]->stream);
    CUDA_SAFE_CALL( cudaStreamSynchronize ( chunks[i]->stream ) );      
}


CudaStringsAggregator::~CudaStringsAggregator()
{
}


}
