#pragma once

#include <cstddef>

#include <common/likely.h>


namespace DB
{

/** An array of (almost) unchangable size:
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
  * Copying is not supported. Moving empties the original object.
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

struct DontInitElemsTag {};

template <typename T>
class AutoArray
{
public:
    /// For deferred creation.
    AutoArray()
    {
        setEmpty();
    }

    AutoArray(size_t size_)
    {
        init(size_, false);
    }

    /** The default constructors for elements will not be called.
      * In this case, you must insert all elements using the `place` and `placement new` functions,
      *  since destructors are then called for them.
      */
    AutoArray(size_t size_, const DontInitElemsTag & tag)
    {
        init(size_, true);
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
        data = src.data;
        src.setEmpty();
    }

    AutoArray & operator= (AutoArray && src)
    {
        if (this == &src)
            return *this;
        uninit();
        data = src.data;
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

    /** Get the piece of memory in which the element should be located.
      * The function is intended to initialize an element,
      *  which has not yet been initialized
      * new (arr.place(i)) T(args);
      */
    char * place(size_t i)
    {
        return data + sizeof(T) * i;
    }

    using iterator = T *;
    using const_iterator = const T *;

    iterator begin() { return &elem(0); }
    iterator end() { return &elem(size()); }

    const_iterator begin() const { return &elem(0); }
    const_iterator end() const { return &elem(size()); }

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
    char * data;

    size_t & m_size()
    {
        return reinterpret_cast<size_t *>(data)[-1];
    }

    size_t m_size() const
    {
        return reinterpret_cast<const size_t *>(data)[-1];
    }

    T & elem(size_t i)
    {
        return reinterpret_cast<T *>(data)[i];
    }

    const T & elem(size_t i) const
    {
        return reinterpret_cast<const T *>(data)[i];
    }

    void setEmpty()
    {
        data = const_cast<char *>(reinterpret_cast<const char *>(&empty_auto_array_helper)) + sizeof(size_t);
    }

    void init(size_t size_, bool dont_init_elems)
    {
        if (!size_)
        {
            setEmpty();
            return;
        }

        data = new char[size_ * sizeof(T) + sizeof(size_t)];
        data += sizeof(size_t);
        m_size() = size_;

        if (!dont_init_elems)
            for (size_t i = 0; i < size_; ++i)
                new (place(i)) T();
    }

    void uninit()
    {
        size_t s = size();

        if (likely(s))
        {
            for (size_t i = 0; i < s; ++i)
                elem(i).~T();

            data -= sizeof(size_t);
            delete[] data;
        }
    }
};

}
