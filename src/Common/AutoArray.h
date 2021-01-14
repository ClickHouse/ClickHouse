#pragma once

#include <cstddef>
#include <cstdlib>

#include <Common/Exception.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

/** An array of (almost) unchangeable size:
  *  the size is specified in the constructor;
  *  `resize` method removes old data, and necessary only for
  *  so that you can first create an empty object using the default constructor,
  *  and then decide on the size.
  *
  * There is a possibility to not initialize elements by default, but create them inplace.
  * Member destructors are called automatically.
  *
  * `sizeof` is equal to the size of one pointer.
  *
  * Not exception-safe.
  *
  * Copying is supported via assign() method. Moving empties the original object.
  * That is, it is inconvenient to use this array in many cases.
  *
  * Designed for situations in which many arrays of the same small size are created,
  *  but the size is not known at compile time.
  * Also gives a significant advantage in cases where it is important that `sizeof` is minimal.
  * For example, if arrays are put in an open-addressing hash table with inplace storage of values (like HashMap)
  *
  * In this case, compared to std::vector:
  * - for arrays of 1 element size - an advantage of about 2 times;
  * - for arrays of 5 elements - an advantage of about 1.5 times
  *   (DB::Field, containing UInt64 and String, used as T);
  */

const size_t empty_auto_array_helper = 0;

template <typename T>
class AutoArray
{
public:
    /// For deferred creation.
    AutoArray()
    {
        setEmpty();
    }

    explicit AutoArray(size_t size_)
    {
        init(size_, false);
    }

    /** Initializes all elements with a copy constructor with the `value` parameter.
      */
    AutoArray(size_t size_, const T & value)
    {
        init(size_, true);

        for (size_t i = 0; i < size_; ++i)
        {
            new (place(i)) T(value);
        }
    }

    /** `resize` removes all existing items.
      */
    void resize(size_t size_, bool dont_init_elems = false)
    {
        uninit();
        init(size_, dont_init_elems);
    }

    /** Move operations.
      */
    AutoArray(AutoArray && src)
    {
        if (this == &src)
            return;
        setEmpty();
        data_ptr = src.data_ptr;
        src.setEmpty();
    }

    AutoArray & operator= (AutoArray && src)
    {
        if (this == &src)
            return *this;
        uninit();
        data_ptr = src.data_ptr;
        src.setEmpty();

        return *this;
    }

    ~AutoArray()
    {
        uninit();
    }

    size_t size() const
    {
        return m_size();
    }

    bool empty() const
    {
        return size() == 0;
    }

    void clear()
    {
        uninit();
        setEmpty();
    }

    template <typename It>
    void assign(It from_begin, It from_end)
    {
        uninit();

        size_t size = from_end - from_begin;
        init(size, /* dont_init_elems = */ true);

        It it = from_begin;
        for (size_t i = 0; i < size; ++i, ++it)
            new (place(i)) T(*it);
    }

    void assign(const AutoArray & from)
    {
        assign(from.begin(), from.end());
    }

    /** You can read and modify elements using the [] operator
      *  only if items were initialized
      *  (that is, into the constructor was not passed DontInitElemsTag,
      *   or you initialized them using `place` and `placement new`).
      */
    T & operator[](size_t i)
    {
        return elem(i);
    }

    const T & operator[](size_t i) const
    {
        return elem(i);
    }

    T * data()
    {
        return elemPtr(0);
    }

    const T * data() const
    {
        return elemPtr(0);
    }

    /** Get the piece of memory in which the element should be located.
      * The function is intended to initialize an element,
      *  which has not yet been initialized
      * new (arr.place(i)) T(args);
      */
    char * place(size_t i)
    {
        return data_ptr + sizeof(T) * i;
    }

    using iterator = T *;
    using const_iterator = const T *;

    iterator begin() { return elemPtr(0); }
    iterator end() { return elemPtr(size()); }

    const_iterator begin() const { return elemPtr(0); }
    const_iterator end() const { return elemPtr(size()); }

    bool operator== (const AutoArray<T> & rhs) const
    {
        size_t s = size();

        if (s != rhs.size())
            return false;

        for (size_t i = 0; i < s; ++i)
            if (elem(i) != rhs.elem(i))
                return false;

        return true;
    }

    bool operator!= (const AutoArray<T> & rhs) const
    {
        return !(*this == rhs);
    }

    bool operator< (const AutoArray<T> & rhs) const
    {
        size_t s = size();
        size_t rhs_s = rhs.size();

        if (s < rhs_s)
            return true;
        if (s > rhs_s)
            return false;

        for (size_t i = 0; i < s; ++i)
        {
            if (elem(i) < rhs.elem(i))
                return true;
            if (elem(i) > rhs.elem(i))
                return false;
        }

        return false;
    }

private:
    static constexpr size_t alignment = alignof(T);
    /// Bytes allocated to store size of array before data. It is padded to have minimum size as alignment.
    /// Padding is at left and the size is stored at right (just before the first data element).
    static constexpr size_t prefix_size = std::max(sizeof(size_t), alignment);

    char * data_ptr;

    size_t & m_size()
    {
        return reinterpret_cast<size_t *>(data_ptr)[-1];
    }

    size_t m_size() const
    {
        return reinterpret_cast<const size_t *>(data_ptr)[-1];
    }

    T * elemPtr(size_t i)
    {
        return reinterpret_cast<T *>(data_ptr) + i;
    }

    const T * elemPtr(size_t i) const
    {
        return reinterpret_cast<const T *>(data_ptr) + i;
    }

    T & elem(size_t i)
    {
        return *elemPtr(i);
    }

    const T & elem(size_t i) const
    {
        return *elemPtr(i);
    }

    void setEmpty()
    {
        data_ptr = const_cast<char *>(reinterpret_cast<const char *>(&empty_auto_array_helper)) + sizeof(size_t);
    }

    void init(size_t new_size, bool dont_init_elems)
    {
        if (!new_size)
        {
            setEmpty();
            return;
        }

        void * new_data = nullptr;
        int res = posix_memalign(&new_data, alignment, prefix_size + new_size * sizeof(T));
        if (0 != res)
            throwFromErrno(fmt::format("Cannot allocate memory (posix_memalign) {}.", ReadableSize(new_size)),
                ErrorCodes::CANNOT_ALLOCATE_MEMORY, res);

        data_ptr = static_cast<char *>(new_data);
        data_ptr += prefix_size;

        m_size() = new_size;

        if (!dont_init_elems)
            for (size_t i = 0; i < new_size; ++i)
                new (place(i)) T();
    }

    void uninit()
    {
        size_t s = size();

        if (s)
        {
            for (size_t i = 0; i < s; ++i)
                elem(i).~T();

            data_ptr -= prefix_size;
            free(data_ptr);
        }
    }
};

}
