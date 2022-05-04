#pragma once

#include <cstddef>
#include <mutex>

#include <Common/Cuda/CudaSafeCall.h>
#include <Common/Cuda/SinglyLinkedList.h>

class CudaHostPinnedMemPool
{
public:
    static CudaHostPinnedMemPool & instance()
    {
        static CudaHostPinnedMemPool instance;
        return instance;
    }

private:
    struct FreeHeader
    {
        std::size_t blockSize;
    };
    struct AllocationHeader
    {
        std::size_t blockSize;
        char padding;
    };

    typedef SinglyLinkedList<FreeHeader>::Node Node;

    std::size_t m_totalSize;
    std::size_t m_used;
    std::size_t m_peak;

    void * m_start_ptr = nullptr;
    SinglyLinkedList<FreeHeader> m_freeList;

    /// protects whole structure during alloc free realloc
    std::mutex mtx;

public:
    CudaHostPinnedMemPool();

    ~CudaHostPinnedMemPool();

    void * alloc(std::size_t size, std::size_t alignment = 8);
    void free(void * ptr);
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 8);

    void init(std::size_t totalSize);
    void reset();

private:
    void coalescence(Node * prevBlock, Node * freeBlock);

    void find(std::size_t size, std::size_t alignment, std::size_t & padding, Node *& previousNode, Node *& foundNode);
};
