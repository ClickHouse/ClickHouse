#pragma once

#include <string.h>
#include <cstddef>
#include <sys/mman.h>
#include <algorithm>

#include <boost/noncopyable.hpp>

#include <Yandex/optimization.h>

#include <DB/Core/Exception.h>


namespace DB
{

/** Динамический массив для POD-типов.
  * Предназначен для небольшого количества больших массивов (а не большого количества маленьких).
  * А точнее - для использования в ColumnVector.
  * Отличается от std::vector тем, что использует mremap для ресайза, а также не инициализирует элементы.
  *  (впрочем, mmap даёт память, инициализированную нулями, так что можно считать, что элементы инициализируются)
  * Сделано noncopyable, чтобы не было случайных копий. Скопировать данные можно с помощью метода assign.
  * Поддерживается только часть интерфейса std::vector.
  *
  * Если вставлять элементы push_back-ом, не делая reserve, то PODArray примерно в 2.5 раза быстрее std::vector.
  */
template <typename T>
class PODArray : private boost::noncopyable
{
private:
	static const size_t initial_size = 4096;

	char * c_start;
	char * c_end;
	char * c_end_of_storage;

	T * t_start() 						{ return reinterpret_cast<T *>(c_start); }
	T * t_end() 						{ return reinterpret_cast<T *>(c_end); }
	T * t_end_of_storage() 				{ return reinterpret_cast<T *>(c_end_of_storage); }
	
	const T * t_start() const 			{ return reinterpret_cast<const T *>(c_start); }
	const T * t_end() const 			{ return reinterpret_cast<const T *>(c_end); }
	const T * t_end_of_storage() const 	{ return reinterpret_cast<const T *>(c_end_of_storage); }

	size_t storage_size() const { return c_end_of_storage - c_start; }
	static size_t byte_size(size_t n) { return n * sizeof(T); }

	static size_t round_up_to_power_of_two(size_t n) { return n == 0 ? 0 : (0x8000000000000000ULL >> (__builtin_clzl(n - 1) - 1)); }
	static size_t to_size(size_t n) { return byte_size(std::max(initial_size, round_up_to_power_of_two(n))); }
	
	void alloc(size_t n)
	{
		size_t bytes_to_alloc = to_size(n);
		c_start = c_end = reinterpret_cast<char *>(mmap(NULL, bytes_to_alloc, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
		c_end_of_storage = c_start + bytes_to_alloc;

		if (MAP_FAILED == c_start)
			throwFromErrno("PODArray: Cannot mmap.");
	}

	void dealloc()
	{
		if (0 != munmap(c_start, storage_size()))
			throwFromErrno("PODArray: Cannot munmap.");
	}

	void realloc(size_t n)
	{
		ptrdiff_t end_diff = c_end - c_start;
		size_t bytes_to_alloc = to_size(n);
		c_start = reinterpret_cast<char *>(mremap(c_start, storage_size(), bytes_to_alloc, MREMAP_MAYMOVE));
		c_end = c_start + end_diff;
		c_end_of_storage = c_start + bytes_to_alloc;

		if (MAP_FAILED == c_start)
			throwFromErrno("PODArray: Cannot mremap.");
	}

public:
	typedef T value_type;
	typedef T * iterator;
	typedef const T * const_iterator;
	
	PODArray() { alloc(initial_size); }
    PODArray(size_t n) { alloc(n); c_end += byte_size(n); }
    PODArray(size_t n, const T & x) { alloc(n); assign(n, x); }
    PODArray(const_iterator from_begin, const_iterator from_end) { alloc(from_end - from_begin); insert(from_end - from_begin); }
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
		size_t required_capacity = size() + from_end - from_begin;
		if (required_capacity > capacity())
			reserve(round_up_to_power_of_two(required_capacity));

		size_t bytes_to_copy = byte_size(from_end - from_begin);
		memcpy(c_end, reinterpret_cast<const void *>(from_begin), bytes_to_copy);
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


}
