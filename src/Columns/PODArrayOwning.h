#include "IBuffer.h"

namespace DB
{

template <typename T, typename TAllocator, size_t pad_right, size_t pad_left>
class PODArrayOwning : public IBuffer<sizeof(T), TAllocator, pad_right, pad_left>
{
protected:
    T * t_start()                      { return reinterpret_cast<T *>(this->data); } /// NOLINT
    T * t_end()                        { return reinterpret_cast<T *>(this->data + this->size); } /// NOLINT

    const T * t_start() const          { return reinterpret_cast<T *>(this->data); } /// NOLINT
    const T * t_end() const            { return reinterpret_cast<T *>(this->data + this->size); } /// NOLINT
public:
    using value_type = T;
    using iterator = T *;
    using const_iterator = const T *;

    T & front()             { return t_start()[0]; }
    T & back()              { return t_end()[-1]; }
    const T & front() const { return t_start()[0]; }
    const T & back() const  { return t_end()[-1]; }

    iterator begin()              { return t_start(); }
    iterator end()                { return t_end(); }
    const_iterator begin() const  { return t_start(); }
    const_iterator end() const    { return t_end(); }
    const_iterator cbegin() const { return t_start(); }
    const_iterator cend() const   { return t_end(); }

};

}
