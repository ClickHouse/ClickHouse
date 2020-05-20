
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

CudaHostPinnedMemPool::CudaHostPinnedMemPool()
{

}

void CudaHostPinnedMemPool::init(std::size_t totalSize) 
{
    if (m_start_ptr != nullptr) 
    {
        // TODO ERROR
        free(m_start_ptr);
        m_start_ptr = nullptr;
    }
    m_totalSize = totalSize;
    m_used = 0;
    CUDA_SAFE_CALL( cudaMallocHost((void**)&m_start_ptr, m_totalSize) );
    //m_start_ptr = malloc(m_totalSize);

    this->reset();
}

CudaHostPinnedMemPool::~CudaHostPinnedMemPool() 
{
    m_totalSize = 0;
    //free(m_start_ptr);
    CUDA_SAFE_CALL_NOTHROW( cudaFreeHost(m_start_ptr) );
    m_start_ptr = nullptr;
}

void* CudaHostPinnedMemPool::alloc(std::size_t size, std::size_t alignment) 
{
    std::unique_lock<std::mutex> lck( mtx );

    const std::size_t allocationHeaderSize = sizeof(CudaHostPinnedMemPool::AllocationHeader);
    const std::size_t freeHeaderSize = sizeof(CudaHostPinnedMemPool::FreeHeader);
    size = std::max(size, sizeof(Node));
    size = ((size/16)+1)*16;
    alignment = std::max(alignment, size_t(16));

    // Search through the free list for a free block that has enough space to allocate our data
    std::size_t padding;
    Node * affectedNode, 
         * previousNode;
    this->find(size, alignment, padding, previousNode, affectedNode);
    if (!(affectedNode != nullptr)) 
        throw std::runtime_error(std::string("CudaHostPinnedMemPool::alloc: Not enough memory:") +
             " size = " + std::to_string(size) + 
             " m_used = " + std::to_string(m_used) + 
             " m_totalSize = " + std::to_string(m_totalSize));


    const std::size_t alignmentPadding =  padding - allocationHeaderSize;
    const std::size_t requiredSize = size + padding;    

    const std::size_t rest = affectedNode->data.blockSize - requiredSize;

    if (rest > 0) 
    {
        // We have to split the block into the data block and a free block of size 'rest'
        Node * newFreeNode = (Node *)((std::size_t) affectedNode + requiredSize);
        newFreeNode->data.blockSize = rest;
        m_freeList.insert(affectedNode, newFreeNode);
    }
    m_freeList.remove(previousNode, affectedNode);

    // Setup data block
    const std::size_t headerAddress = (std::size_t) affectedNode + alignmentPadding;
    const std::size_t dataAddress = headerAddress + allocationHeaderSize;
    ((CudaHostPinnedMemPool::AllocationHeader *) headerAddress)->blockSize = requiredSize;
    ((CudaHostPinnedMemPool::AllocationHeader *) headerAddress)->padding = alignmentPadding;

    m_used += requiredSize;
    m_peak = std::max(m_peak, m_used);

#ifdef _DEBUG
    std::cout << "A" << "\t@H " << (void*) headerAddress << "\tD@ " <<(void*) dataAddress << "\tS " << ((CudaHostPinnedMemPool::AllocationHeader *) headerAddress)->blockSize <<  "\tAP " << alignmentPadding << "\tP " << padding << "\tM " << m_used << "\tR " << rest << std::endl;
#endif

    return (void*) dataAddress;
}

void CudaHostPinnedMemPool::find(std::size_t size, std::size_t alignment, std::size_t& padding, Node *& previousNode, Node *& foundNode) 
{
    //Iterate list and return the first free block with a size >= than given size
    Node * it = m_freeList.head,
         * itPrev = nullptr;
    
    while (it != nullptr) 
    {
        padding = CudaHostPinnedMemPoolUtils::CalculatePaddingWithHeader((std::size_t)it, alignment, sizeof (CudaHostPinnedMemPool::AllocationHeader));
        const std::size_t requiredSpace = size + padding;
        if (it->data.blockSize >= requiredSpace) 
        {
            break;
        }
        itPrev = it;
        it = it->next;
    }
    previousNode = itPrev;
    foundNode = it;
}

void CudaHostPinnedMemPool::free(void* ptr) 
{
    std::unique_lock<std::mutex> lck( mtx );

    // Insert it in a sorted position by the address number
    const std::size_t currentAddress = (std::size_t) ptr;
    const std::size_t headerAddress = currentAddress - sizeof (CudaHostPinnedMemPool::AllocationHeader);
    const CudaHostPinnedMemPool::AllocationHeader * allocationHeader{ (CudaHostPinnedMemPool::AllocationHeader *) headerAddress};

    Node * freeNode = (Node *) (headerAddress);
    freeNode->data.blockSize = allocationHeader->blockSize + allocationHeader->padding;
    freeNode->next = nullptr;

    Node * it = m_freeList.head;
    Node * itPrev = nullptr;
    while (it != nullptr) 
    {
        if (ptr < it) 
        {
            m_freeList.insert(itPrev, freeNode);
            break;
        }
        itPrev = it;
        it = it->next;
    }
    
    m_used -= freeNode->data.blockSize;

    // Merge contiguous nodes
    coalescence(itPrev, freeNode);  

#ifdef _DEBUG
    std::cout << "F" << "\t@ptr " <<  ptr <<"\tH@ " << (void*) freeNode << "\tS " << freeNode->data.blockSize << "\tM " << m_used << std::endl;
#endif
}

/// TODO there are no special optimizations for enlargement (realloc without memcpy case)
/// TODO are we sure that new_size >= old_size??
void *CudaHostPinnedMemPool::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
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

void CudaHostPinnedMemPool::coalescence(Node* previousNode, Node * freeNode) 
{   
    if (freeNode->next != nullptr && 
            (std::size_t) freeNode + freeNode->data.blockSize == (std::size_t) freeNode->next) 
    {
        freeNode->data.blockSize += freeNode->next->data.blockSize;
        m_freeList.remove(freeNode, freeNode->next);
#ifdef _DEBUG
    std::cout << "\tMerging(n) " << (void*) freeNode << " & " << (void*) freeNode->next << "\tS " << freeNode->data.blockSize << std::endl;
#endif
    }
    
    if (previousNode != nullptr &&
            (std::size_t) previousNode + previousNode->data.blockSize == (std::size_t) freeNode) 
    {
        previousNode->data.blockSize += freeNode->data.blockSize;
        m_freeList.remove(previousNode, freeNode);
#ifdef _DEBUG
    std::cout << "\tMerging(p) " << (void*) previousNode << " & " << (void*) freeNode << "\tS " << previousNode->data.blockSize << std::endl;
#endif
    }
}

void CudaHostPinnedMemPool::reset() 
{
    m_used = 0;
    m_peak = 0;
    Node * firstNode = (Node *) m_start_ptr;
    firstNode->data.blockSize = m_totalSize;
    firstNode->next = nullptr;
    m_freeList.head = nullptr;
    m_freeList.insert(nullptr, firstNode);
}