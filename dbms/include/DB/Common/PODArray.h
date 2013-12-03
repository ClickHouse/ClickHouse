#pragma once

#include <string.h>
#include <malloc.h>
#include <cstddef>
#include <algorithm>
#include <memory>

#include <boost/noncopyable.hpp>

#include <Yandex/likely.h>
#include <Yandex/strong_typedef.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>


namespace DB
{

/** Динамический массив для POD-типов.
  * Предназначен для небольшого количества больших массивов (а не большого количества маленьких).
  * А точнее - для использования в ColumnVector.
  * Отличается от std::vector тем, что не инициализирует элементы.
  * Сделано noncopyable, чтобы не было случайных копий. Скопировать данные можно с помощью метода assign.
  * Поддерживается только часть интерфейса std::vector.
  *
  * Если вставлять элементы push_back-ом, не делая reserve, то PODArray примерно в 2.5 раза быстрее std::vector.
  */
template <typename T>
class PODArray : private boost::noncopyable, private std::allocator<char>	/// empty base optimization
{
private:
	typedef std::allocator<char> Allocator;
	static const size_t initial_size;

	char * c_start;
	char * c_end;
	char * c_end_of_storage;

	bool use_libc_realloc;

	T * t_start() 						{ return reinterpret_cast<T *>(c_start); }
	T * t_end() 						{ return reinterpret_cast<T *>(c_end); }
	T * t_end_of_storage() 				{ return reinterpret_cast<T *>(c_end_of_storage); }
	
	const T * t_start() const 			{ return reinterpret_cast<const T *>(c_start); }
	const T * t_end() const 			{ return reinterpret_cast<const T *>(c_end); }
	const T * t_end_of_storage() const 	{ return reinterpret_cast<const T *>(c_end_of_storage); }

	size_t storage_size() const { return c_end_of_storage - c_start; }
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
	
	static size_t to_size(size_t n) { return byte_size(std::max(initial_size, round_up_to_power_of_two(n))); }
	
	void alloc(size_t n)
	{
		size_t bytes_to_alloc = to_size(n);
		c_start = c_end = Allocator::allocate(bytes_to_alloc);
		c_end_of_storage = c_start + bytes_to_alloc;

		//memset(c_start, 0, bytes_to_alloc);
	}

	void dealloc()
	{
		if (use_libc_realloc)
			::free(c_start);
		else
			Allocator::deallocate(c_start, storage_size());
	}

	void realloc(size_t n)
	{
//		std::cerr << "realloc" << std::endl;
		
		ptrdiff_t end_diff = c_end - c_start;
		size_t bytes_to_alloc = to_size(n);

		char * old_c_start = c_start;
		char * old_c_end_of_storage = c_end_of_storage;

		if (use_libc_realloc)
		{
			c_start = reinterpret_cast<char *>(::realloc(c_start, bytes_to_alloc));

			if (NULL == c_start)
				throwFromErrno("PODArray: cannot realloc", ErrorCodes::CANNOT_ALLOCATE_MEMORY);
		}
		else
		{
			c_start = reinterpret_cast<char *>(malloc(bytes_to_alloc));

			if (NULL == c_start)
				throwFromErrno("PODArray: cannot realloc", ErrorCodes::CANNOT_ALLOCATE_MEMORY);

			memcpy(c_start, old_c_start, old_c_end_of_storage - old_c_start);
			Allocator::deallocate(old_c_start, old_c_end_of_storage - old_c_start);
		}
		
		//memset(c_start + (old_c_end_of_storage - old_c_start), 0, bytes_to_alloc - (old_c_end_of_storage - old_c_start));
				
		c_end = c_start + end_diff;
		c_end_of_storage = c_start + bytes_to_alloc;

		use_libc_realloc = true;
	}

public:
	typedef T value_type;


	/// Просто typedef нельзя, так как возникает неоднозначность для конструкторов и функций assign.
	class iterator
	{
	private:
		T * ptr;
	public:
		iterator() {}
		iterator(T * ptr_) : ptr(ptr_) {}

		bool operator== (const iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const iterator & rhs) const { return ptr != rhs.ptr; }

		iterator & operator++() { ++ptr; return *this; }

		T & operator* () const { return *ptr; }
		T * operator->() const { return ptr; }

		iterator operator+ (ptrdiff_t diff) const { return ptr + diff; }
		iterator operator- (ptrdiff_t diff) const { return ptr - diff; }
		ptrdiff_t operator- (const iterator & rhs) const { return ptr - rhs.ptr; }
	};

	class const_iterator
	{
	private:
		const T * ptr;
	public:
		const_iterator() {}
		const_iterator(const T * ptr_) : ptr(ptr_) {}

		bool operator== (const const_iterator & rhs) const { return ptr == rhs.ptr; }
		bool operator!= (const const_iterator & rhs) const { return ptr != rhs.ptr; }

		const_iterator & operator++() { ++ptr; return *this; }

		const T & operator* () const { return *ptr; }
		const T * operator->() const { return ptr; }

		const_iterator operator+ (ptrdiff_t diff) const { return ptr + diff; }
		const_iterator operator- (ptrdiff_t diff) const { return ptr - diff; }
		ptrdiff_t operator- (const const_iterator & rhs) const { return ptr - rhs.ptr; }
	};


	PODArray() : use_libc_realloc(false) { alloc(initial_size); }
    PODArray(size_t n) : use_libc_realloc(false) { alloc(n); c_end += byte_size(n); }
    PODArray(size_t n, const T & x) : use_libc_realloc(false) { alloc(n); assign(n, x); }
    PODArray(const_iterator from_begin, const_iterator from_end) : use_libc_realloc(false) { alloc(from_end - from_begin); insert(from_begin, from_end); }
    ~PODArray() { dealloc(); }

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
		realloc(size() * 2);
	}

	void resize(size_t n)
	{
		reserve(n);
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
	void insert(const_iterator from_begin, const_iterator from_end)
	{
		size_t required_capacity = size() + (from_end - from_begin);
		if (required_capacity > capacity())
			reserve(round_up_to_power_of_two(required_capacity));

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

	void assign(const_iterator from_begin, const_iterator from_end)
	{
		size_t required_capacity = from_end - from_begin;
		if (required_capacity > capacity())
			reserve(round_up_to_power_of_two(required_capacity));

		size_t bytes_to_copy = byte_size(required_capacity);
		memcpy(c_start, reinterpret_cast<const void *>(from_begin), bytes_to_copy);
		c_end = c_start + bytes_to_copy;
	}

	void assign(const PODArray<T> & from)
	{
		assign(from.begin(), from.end());
	}
};

template <typename T>
const size_t PODArray<T>::initial_size = 4096;



}
