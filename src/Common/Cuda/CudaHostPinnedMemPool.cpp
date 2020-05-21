#include <stdlib.h>
#include <cassert>
#include <limits>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <cstring>

#include <cuda.h>

#include <Common/Cuda/CudaHostPinnedMemPool.h>
#include <Common/Cuda/CudaHostPinnedMemPoolUtils.h>

#ifdef _DEBUG
#include <iostream>
#endif

/// TODO throw exceptions

void CudaHostPinnedMemPool::init(size_t total_size_)
{
    if (start_ptr)
    {
        // TODO ERROR
        free(start_ptr);
        start_ptr = nullptr;
    }
    total_size = total_size_;
    used = 0;
    CUDA_SAFE_CALL(cudaMallocHost(reinterpret_cast<void **>(&start_ptr), total_size));
    //start_ptr = malloc(total_size);

    this->reset();
}

CudaHostPinnedMemPool::~CudaHostPinnedMemPool()
{
    total_size = 0;
    //free(start_ptr);
    CUDA_SAFE_CALL_NOTHROW(cudaFreeHost(start_ptr));
    start_ptr = nullptr;
}

void * CudaHostPinnedMemPool::alloc(size_t size, size_t alignment)
{
    std::lock_guard<std::mutex> lock(mtx);

    const size_t allocation_header_size = sizeof(CudaHostPinnedMemPool::AllocationHeader);
    size = std::max(size, sizeof(Node));
    size = ((size/16)+1)*16;
    alignment = std::max(alignment, size_t(16));

    // Search through the free list for a free block that has enough space to allocate our data
    size_t padding{0};
    Node * affected_node{nullptr};
    Node * previous_node{nullptr};
    find(size, alignment, padding, previous_node, affected_node);
    if (!affected_node)
        throw std::runtime_error(std::string("CudaHostPinnedMemPool::alloc: Not enough memory:") +
             " size = " + std::to_string(size) +
             " used = " + std::to_string(used) +
             " total_size = " + std::to_string(total_size));


    const size_t alignment_padding = padding - allocation_header_size;
    const size_t required_size = size + padding;

    const size_t rest = affected_node->data.block_size - required_size;

    if (rest > 0)
    {
        // We have to split the block into the data block and a free block of size 'rest'
        Node * new_free_node = reinterpret_cast<Node *>(reinterpret_cast<uint8_t *>(affected_node) + required_size);
        new_free_node->data.block_size = rest;
        free_list.insert(affected_node, new_free_node);
    }
    free_list.remove(previous_node, affected_node);

    // Setup data block
    void * header_address = reinterpret_cast<uint8_t *>(affected_node) + alignment_padding;
    void * data_address = reinterpret_cast<uint8_t *>(header_address) + allocation_header_size;
    reinterpret_cast<CudaHostPinnedMemPool::AllocationHeader *>(header_address)->block_size = required_size;
    reinterpret_cast<CudaHostPinnedMemPool::AllocationHeader *>(header_address)->padding = alignment_padding;

    used += required_size;
    peak = std::max(peak, used);

#ifdef _DEBUG
    std::cout << "A" << "\t@H " << header_address << "\tD@ " << data_address << "\tS "
        << ((CudaHostPinnedMemPool::AllocationHeader *) header_address)->block_size <<  "\tAP "
        << alignment_padding << "\tP " << padding << "\tM " << used << "\tR " << rest << std::endl;
#endif

    return data_address;
}

void CudaHostPinnedMemPool::find(size_t size, size_t alignment, size_t & padding, Node *& previous_node, Node *& found_node)
{
    //Iterate list and return the first free block with a size >= than given size
    Node * it = free_list.head;
    Node * it_prev = nullptr;

    while (it)
    {
        padding = CudaHostPinnedMemPoolUtils::CalculatePaddingWithHeader(it, alignment, sizeof(CudaHostPinnedMemPool::AllocationHeader));
        const size_t requiredSpace = size + padding;
        if (it->data.block_size >= requiredSpace)
        {
            break;
        }
        it_prev = it;
        it = it->next;
    }
    previous_node = it_prev;
    found_node = it;
}

void CudaHostPinnedMemPool::free(void * ptr)
{
    std::lock_guard<std::mutex> lock(mtx);

    // Insert it in a sorted position by the address number
    void * header_address = reinterpret_cast<uint8_t *>(ptr) - sizeof(CudaHostPinnedMemPool::AllocationHeader);
    auto * allocationHeader = reinterpret_cast<CudaHostPinnedMemPool::AllocationHeader *>(header_address);

    Node * free_node = reinterpret_cast<Node *>(header_address);
    free_node->data.block_size = allocationHeader->block_size + allocationHeader->padding;
    free_node->next = nullptr;

    Node * it = free_list.head;
    Node * itPrev = nullptr;
    while (it)
    {
        if (ptr < it)
        {
            free_list.insert(itPrev, free_node);
            break;
        }
        itPrev = it;
        it = it->next;
    }

    used -= free_node->data.block_size;

    // Merge contiguous nodes
    coalescence(itPrev, free_node);

#ifdef _DEBUG
    std::cout << "F" << "\t@ptr " <<  ptr <<"\tH@ " << (void*) free_node << "\tS " << free_node->data.block_size << "\tM " << used << std::endl;
#endif
}

/// TODO there are no special optimizations for enlargement (realloc without memcpy case)
/// TODO are we sure that new_size >= old_size??
void * CudaHostPinnedMemPool::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    if (old_size == new_size)
    {
        /// nothing to do.
    }
    else
    {
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, old_size);
        free(buf);
        buf = new_buf;
    }

    return buf;
}

CudaHostPinnedMemPool::Node * CudaHostPinnedMemPool::neighbourNode(Node * node)
{
    return reinterpret_cast<Node *>(reinterpret_cast<uint8_t *>(node) + node->data.block_size);
}

void CudaHostPinnedMemPool::coalescence(Node * previous_node, Node * free_node)
{
    if (free_node->next && neighbourNode(free_node) == free_node->next)
    {
        free_node->data.block_size += free_node->next->data.block_size;
        free_list.remove(free_node, free_node->next);
#ifdef _DEBUG
    std::cout << "\tMerging(n) " << (void*) free_node << " & " << (void*) free_node->next << "\tS " << free_node->data.block_size << std::endl;
#endif
    }

    if (previous_node && neighbourNode(previous_node) == free_node)
    {
        previous_node->data.block_size += free_node->data.block_size;
        free_list.remove(previous_node, free_node);
#ifdef _DEBUG
    std::cout << "\tMerging(p) " << (void*) previous_node << " & " << (void*) free_node << "\tS " << previous_node->data.block_size << std::endl;
#endif
    }
}

void CudaHostPinnedMemPool::reset()
{
    used = 0;
    peak = 0;
    Node * first_node = reinterpret_cast<Node *>(start_ptr);
    first_node->data.block_size = total_size;
    first_node->next = nullptr;
    free_list.head = nullptr;
    free_list.insert(nullptr, first_node);
}
