#pragma once

#include <string.h>
#include <cstddef>
#include <algorithm>
#include <memory>

#include <boost/noncopyable.hpp>
#include <boost/iterator_adaptors.hpp>

#include <Yandex/likely.h>
#include <Yandex/strong_typedef.h>

#include <DB/Common/Allocator.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

/** Динамический массив для POD-типов.
  * Предназначен для небольшого количества больших массивов (а не большого количества маленьких).
  * А точнее - для использования в ColumnVector.
  * Отличается от std::vector тем, что не инициализирует элементы.
  *
  * Сделан некопируемым, чтобы не было случайных копий. Скопировать данные можно с помощью метода assign.
  *
  * Поддерживается только часть интерфейса std::vector.
  *
  * Конструктор по-умолчанию создаёт пустой объект, который не выделяет память.
  * Затем выделяется память минимум под POD_ARRAY_INITIAL_SIZE элементов.
  *
  * Если вставлять элементы push_back-ом, не делая reserve, то PODArray примерно в 2.5 раза быстрее std::vector.
  */
#define POD_ARRAY_INITIAL_SIZE 4096UL

template <typename T>
class PODArray : private boost::noncopyable, private Allocator	/// empty base optimization
{
private:
	char * c_start;
	char * c_end;
	char * c_end_of_storage;

	T * t_start() 						{ return reinterpret_cast<T *>(c_start); }
	T * t_end() 						{ return reinterpret_cast<T *>(c_end); }
	T * t_end_of_storage() 				{ return reinterpret_cast<T *>(c_end_of_storage); }

	const T * t_start() const 			{ return reinterpret_cast<const T *>(c_start); }
	const T * t_end() const 			{ return reinterpret_cast<const T *>(c_end); }
	const T * t_end_of_storage() const 	{ return reinterpret_cast<const T *>(c_end_of_storage); }

	static size_t byte_size(size_t n) { return n * sizeof(T); }

	static size_t round_up_to_power_of_two(size_t n)
	{
		--n;
		n |= n >> 1;
		n |= n >> 2;
		n |= n >> 4;
		n |= n >> 8;
		n |= n >> 16;
		n |= n >> 32;
		++n;

		return n;
	}

	static size_t to_size(size_t n) { return byte_size(std::max(POD_ARRAY_INITIAL_SIZE, round_up_to_power_of_two(n))); }

	void alloc(size_t n)
	{
		if (n == 0)
		{
			c_start = c_end = c_end_of_storage = nullptr;
			return;
		}

		size_t bytes_to_alloc = to_size(n);

		c_start = c_end = reinterpret_cast<char *>(Allocator::alloc(bytes_to_alloc));
		c_end_of_storage = c_start + bytes_to_alloc;
	}

	void dealloc()
	{
		if (c_start == nullptr)
			return;

		Allocator::free(c_start, storage_size());
	}

	void realloc(size_t n)
	{
		if (c_start == nullptr)
		{
			alloc(n);
			return;
		}

		ptrdiff_t end_diff = c_end - c_start;
		size_t bytes_to_alloc = to_size(n);

		c_start = reinterpret_cast<char *>(Allocator::realloc(c_start, storage_size(), bytes_to_alloc));

		c_end = c_start + end_diff;
		c_end_of_storage = c_start + bytes_to_alloc;
	}

public:
	typedef T value_type;

	size_t storage_size() const { return c_end_of_storage - c_start; }

	/// Просто typedef нельзя, так как возникает неоднозначность для конструкторов и функций assign.
	struct iterator : public boost::iterator_adaptor<iterator, T*>
	{
		iterator() {}
		iterator(T * ptr_) : iterator::iterator_adaptor_(ptr_) {}
	};

	struct const_iterator : public boost::iterator_adaptor<const_iterator, const T*>
	{
		const_iterator() {}
        const_iterator(const T * ptr_) : const_iterator::iterator_adaptor_(ptr_) {}
	};


	PODArray() { alloc(0); }
    PODArray(size_t n) { alloc(n); c_end += byte_size(n); }
    PODArray(size_t n, const T & x) { alloc(n); assign(n, x); }
    PODArray(const_iterator from_begin, const_iterator from_end) { alloc(from_end - from_begin); insert(from_begin, from_end); }
    ~PODArray() { dealloc(); }

	PODArray(PODArray && other) { *this = std::move(other); }
	PODArray & operator=(PODArray && other)
	{
		std::swap(c_start, other.c_start);
		std::swap(c_end, other.c_end);
		std::swap(c_end_of_storage, other.c_end_of_storage);

		return *this;
	}

	T * data() { return t_start(); }
	const T * data() const { return t_start(); }

    size_t size() const { return t_end() - t_start(); }
    bool empty() const { return t_end() == t_start(); }
    size_t capacity() const { return t_end_of_storage() - t_start(); }

	T & operator[] (size_t n) 				{ return t_start()[n]; }
	const T & operator[] (size_t n) const 	{ return t_start()[n]; }

	T & front() 			{ return t_start()[0]; }
	T & back() 				{ return t_end()[-1]; }
	const T & front() const { return t_start()[0]; }
	const T & back() const  { return t_end()[-1]; }

	iterator begin() 				{ return t_start(); }
	iterator end() 					{ return t_end(); }
	const_iterator begin() const	{ return t_start(); }
	const_iterator end() const		{ return t_end(); }

	void reserve(size_t n)
	{
		if (n > capacity())
			realloc(n);
	}

	void reserve()
	{
		if (size() == 0)
			realloc(POD_ARRAY_INITIAL_SIZE);
		else
			realloc(size() * 2);
	}

	void resize(size_t n)
	{
		reserve(n);
		resize_assume_reserved(n);
	}

	void resize_assume_reserved(const size_t n)
	{
		c_end = c_start + byte_size(n);
	}

	/// Как resize, но обнуляет новые элементы.
	void resize_fill(size_t n)
	{
		size_t old_size = size();
		if (n > old_size)
		{
			reserve(n);
			memset(c_end, 0, n - old_size);
		}
		c_end = c_start + byte_size(n);
	}

	void resize_fill(size_t n, const T & value)
	{
		size_t old_size = size();
		if (n > old_size)
		{
			reserve(n);
			std::fill(t_end(), t_end() + n - old_size, value);
		}
		c_end = c_start + byte_size(n);
	}

	void clear()
	{
		c_end = c_start;
	}

	void push_back(const T & x)
	{
		if (unlikely(c_end == c_end_of_storage))
			reserve();

		*t_end() = x;
		c_end += byte_size(1);
	}

	/// Не вставляйте в массив кусок самого себя. Потому что при ресайзе, итераторы на самого себя могут инвалидироваться.
	template <typename It1, typename It2>
	void insert(It1 from_begin, It2 from_end)
	{
		size_t required_capacity = size() + (from_end - from_begin);
		if (required_capacity > capacity())
			reserve(round_up_to_power_of_two(required_capacity));

		insert_assume_reserved(from_begin, from_end);
	}

	template <typename It1, typename It2>
	void insert_assume_reserved(It1 from_begin, It2 from_end)
	{
		size_t bytes_to_copy = byte_size(from_end - from_begin);
		memcpy(c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
		c_end += bytes_to_copy;
	}

	void swap(PODArray<T> & rhs)
	{
		std::swap(c_start, rhs.c_start);
		std::swap(c_end, rhs.c_end);
		std::swap(c_end_of_storage, rhs.c_end_of_storage);
	}

	void assign(size_t n, const T & x)
	{
		resize(n);
		std::fill(begin(), end(), x);
	}

	template <typename It1, typename It2>
	void assign(It1 from_begin, It2 from_end)
	{
		size_t required_capacity = from_end - from_begin;
		if (required_capacity > capacity())
			reserve(round_up_to_power_of_two(required_capacity));

		size_t bytes_to_copy = byte_size(required_capacity);
		memcpy(c_start, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
		c_end = c_start + bytes_to_copy;
	}

	void assign(const PODArray<T> & from)
	{
		assign(from.begin(), from.end());
	}


	bool operator== (const PODArray<T> & other) const
	{
		if (size() != other.size())
			return false;

		const_iterator this_it = begin();
		const_iterator that_it = other.begin();

		while (this_it != end())
		{
			if (*this_it != *that_it)
				return false;

			++this_it;
			++that_it;
		}

		return true;
	}

	bool operator!= (const PODArray<T> & other) const
	{
		return !operator==(other);
	}
};


}
