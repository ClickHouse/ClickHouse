#include <cstddef>

namespace DB 
{

template <size_t element_size, typename TAllocator, size_t pad_right, size_t pad_left>
class IBuffer : private TAllocator
{
public:
    char* data = nullptr;
    size_t size = 0;

    virtual ~IBuffer() = default;
    IBuffer() = default;
    IBuffer(const IBuffer &) = delete;

    virtual void allocForNumElements(size_t num_elements);
    virtual void alloc(size_t bytes);
    virtual void realloc(size_t bytes);
    virtual void dealloc();

};

}
