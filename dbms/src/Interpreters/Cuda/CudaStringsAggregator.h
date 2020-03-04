#pragma once

#include <memory>
#include <vector>
#include <thread>
#include <condition_variable>
#include <string>
#include <unordered_map>
#include <cuda.h>
#include <cuda_runtime.h>
#include <thrust/device_vector.h>

#include <Core/Cuda/Types.h>
#include <Common/Cuda/CudaArray.h>
#include <Common/Cuda/CudaHostPinnedArray.h>
#include <Common/HashTable/Cuda/CudaStringsHashTable.h>

#include <Columns/Cuda/CudaColumnString.h>
#include <Columns/Cuda/CudaHostStringsBuffer.h>

#include <AggregateFunctions/Cuda/ICudaAggregateFunction.h>

/// NOTE in  sake of simplicity we don't use any specaial memory pools for aggreagte states on host
/// In real CH enviroment we won't use std::unordered_map, but we will use CH HashTables instead

namespace DB
{

/** Analog of Aggregator for CUDA, but only works with String keys and values
  */
class CudaStringsAggregator
{
public:
    typedef UInt32                                                  SizeType;
    typedef UInt64                                                  OffsetType;
    typedef std::unordered_map<std::string,CudaAggregateDataPtr>    ResultType;

    CudaStringsAggregator(int dev_number_, size_t chunks_num_, 
        UInt32 hash_table_max_size_, UInt32 hash_table_str_buffer_max_size_,
        UInt32 buffer_max_str_num_, UInt32 buffer_max_size_,
        CudaAggregateFunctionPtr aggregate_function_);

    /*void            setAggregateFunction(CudaAggregateFunctionPtr aggregate_function_) 
    {
        aggregate_function = aggregate_function_;
    }*/

    void            startProcessing();
    void            queueData(size_t str_num, size_t str_buf_sz, const char *str_buf, const OffsetType *offsets,
                              size_t vals_str_buf_sz, const char *vals_str_buf, const OffsetType *vals_offsets);
    void            waitQueueData()const;
    void            waitProcessed();

    const ResultType &getResult()const { return chunks[0]->agg_result; }

    ~CudaStringsAggregator();
protected:
    int                                         dev_number;
    CudaAggregateFunctionPtr                    aggregate_function;
    bool                                        is_vals_needed;
    int                                         curr_filling_chunk;
    cudaStream_t                                copy_stream;

    /// TODO turn it into full incapsulated object
    struct WorkChunkInfo
    {
        std::thread                                 t;
        cudaStream_t                                stream;

        /// cuda_processing_state means that aggregation is in progress
        /// and perhaps not ended yet; !cuda_processing_state means we 
        /// can append data to cuda buffers
        bool                                        cuda_processing_state;
        std::mutex                                  cuda_buffer_mtx;
        std::condition_variable                     cv_cuda_processing_end;
        std::condition_variable                     cv_buffer_append_end;

        CudaColumnStringPtr                         cuda_buffer_keys, cuda_buffer_vals;
        std::vector<UInt32>                         blocks_sizes, blocks_buf_keys_sizes, 
                                                    blocks_buf_vals_sizes;

        std::shared_ptr<CudaStringsHashTable>       cuda_hash_table;

        thrust::device_vector<SizeType>             group_nums;
        /// we don't know type of aggregate result until runtime, so we use byte array
        CudaArrayPtr<char>                          group_agg_res;
        CudaArrayPtr<char>                          agg_tmp_buf;
        /// this one is stupid one element vector to return single value
        thrust::device_vector<UInt32>               group_agg_res_total_len;
        CudaHostPinnedArrayPtr<UInt32>              host_group_agg_res_total_len;

        CudaHostStringsBufferPtr                    host_buffer_agg_res_keys;
        /// we don't know type of aggregate result until runtime, so we use byte array
        CudaHostPinnedArrayPtr<char>                host_group_agg_res;
        ResultType                                  agg_result;
    };
    using WorkChunkInfoPtr = std::shared_ptr<WorkChunkInfo>;

    std::vector<WorkChunkInfoPtr>                   chunks; 

    /// ISSUE maybe move it inside WorkChunkInfo?
    bool        tryQueueData(size_t str_num, size_t str_buf_sz, const char *str_buf, const OffsetType *offsets,
                             size_t vals_str_buf_sz, const char *vals_str_buf, const OffsetType *vals_offsets);
public:     /// TODO FUCKING WRONG that it is public!!
    void        processChunk(size_t i);
};

using CudaStringsAggregatorPtr = std::shared_ptr<CudaStringsAggregator>;


}
