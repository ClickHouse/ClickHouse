#pragma once

#include <cstddef>
#include <mutex>

#include <Common/Cuda/SinglyLinkedList.h>
#include <Common/Cuda/CudaSafeCall.h>

class CudaHostPinnedMemPool final
{
private:
    struct FreeHeader
    {
        size_t block_size;
    };

    struct AllocationHeader
    {
        size_t block_size;
        char padding;
    };

    using Node = SinglyLinkedList<FreeHeader>::Node;

    size_t total_size;
    size_t used;
    size_t peak;

    void * start_ptr = nullptr;
    SinglyLinkedList<FreeHeader> free_list;

    /// protects whole structure during alloc free realloc
    std::mutex mtx;

public:
    CudaHostPinnedMemPool() = default;
    ~CudaHostPinnedMemPool();

    static CudaHostPinnedMemPool & instance()
    {
        static std::unique_ptr<CudaHostPinnedMemPool> inst = std::make_unique<CudaHostPinnedMemPool>();
        return *inst;
    }

    void * alloc(size_t size, size_t alignment = 8);
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 8);
    void free(void * ptr);

    void init(size_t totalSize);
    void reset();

private:
    static Node * neighbourNode(Node *);
    void coalescence(Node * prevBlock, Node * freeBlock);
    void find(size_t size, size_t alignment, size_t& padding, Node *& previousNode, Node *& foundNode);
};
