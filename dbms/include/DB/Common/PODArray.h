#pragma once

#include <string.h>
#include <cstddef>
#include <algorithm>
#include <memory>

#include <boost/noncopyable.hpp>
#include <boost/iterator_adaptors.hpp>

#include <common/likely.h>
#include <common/strong_typedef.h>

#include <DB/Common/Allocator.h>
#include <DB/Common/Exception.h>
#include <DB/Common/BitHelpers.h>


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
  * Затем выделяется память минимум в INITIAL_SIZE байт.
  *
  * Если вставлять элементы push_back-ом, не делая reserve, то PODArray примерно в 2.5 раза быстрее std::vector.
  *
  * Шаблонный параметр pad_right - всегда выделять в конце массива столько неиспользуемых байт.
  * Может использоваться для того, чтобы делать оптимистичное чтение, запись, копирование невыровненными SIMD-инструкциями.
  */
template <typename T, size_t INITIAL_SIZE = 4096, typename TAllocator = Allocator<false>, size_t pad_right_ = 0>
class PODArray : private boost::noncopyable, private TAllocator	/// empty base optimization
{
private:
	/// Округление padding-а вверх до целого количества элементов, чтобы упростить арифметику.
	static constexpr size_t pad_right = (pad_right_ + sizeof(T) - 1) / sizeof(T) * sizeof(T);

	char * c_start 			= nullptr;
	char * c_end 			= nullptr;
	char * c_end_of_storage = nullptr;	/// Не включает в себя pad_right.

	T * t_start() 						{ return reinterpret_cast<T *>(c_start); }
	T * t_end() 						{ return reinterpret_cast<T *>(c_end); }
	T * t_end_of_storage() 				{ return reinterpret_cast<T *>(c_end_of_storage); }

	const T * t_start() const 			{ return reinterpret_cast<const T *>(c_start); }
	const T * t_end() const 			{ return reinterpret_cast<const T *>(c_end); }
	const T * t_end_of_storage() const 	{ return reinterpret_cast<const T *>(c_end_of_storage); }

	/// Количество памяти, занимаемое num_elements элементов.
	static size_t byte_size(size_t num_elements) { return num_elements * sizeof(T); }

	/// Минимальное количество памяти, которое нужно выделить для num_elements элементов, включая padding.
	static size_t minimum_memory_for_elements(size_t num_elements) { return byte_size(num_elements) + pad_right; }

	void alloc_for_num_elements(size_t num_elements)
	{
		alloc(roundUpToPowerOfTwoOrZero(minimum_memory_for_elements(num_elements)));
	}

	void alloc(size_t bytes)
	{
		c_start = c_end = reinterpret_cast<char *>(TAllocator::alloc(bytes));
		c_end_of_storage = c_start + bytes - pad_right;
	}

	void dealloc()
	{
		if (c_start == nullptr)
			return;

		TAllocator::free(c_start, allocated_size());
	}

	void realloc(size_t bytes)
	{
		if (c_start == nullptr)
		{
			alloc(bytes);
			return;
		}

		ptrdiff_t end_diff = c_end - c_start;

		c_start = reinterpret_cast<char *>(TAllocator::realloc(c_start, allocated_size(), bytes));

		c_end = c_start + end_diff;
		c_end_of_storage = c_start + bytes - pad_right;
	}

	bool isInitialized() const
	{
		return (c_start != nullptr) && (c_end != nullptr) && (c_end_of_storage != nullptr);
	}

	bool isAllocatedFromStack() const
	{
		constexpr size_t stack_threshold = TAllocator::getStackThreshold();
		return (stack_threshold > 0) && (allocated_size() <= stack_threshold);
	}

public:
	using value_type = T;

	size_t allocated_size() const { return c_end_of_storage - c_start + pad_right; }

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


	PODArray() {}

	PODArray(size_t n)
	{
		alloc_for_num_elements(n);
		c_end += byte_size(n);
	}

	PODArray(size_t n, const T & x)
	{
		alloc_for_num_elements(n);
		assign(n, x);
	}

	PODArray(const_iterator from_begin, const_iterator from_end)
	{
		alloc_for_num_elements(from_end - from_begin);
		insert(from_begin, from_end);
	}

	~PODArray()
	{
		dealloc();
	}

	PODArray(PODArray && other)
	{
		this->swap(other);
	}

	PODArray & operator=(PODArray && other)
	{
		this->swap(other);
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
	const_iterator cbegin() const	{ return t_start(); }
	const_iterator cend() const		{ return t_end(); }

	void reserve(size_t n)
	{
		if (n > capacity())
			realloc(roundUpToPowerOfTwoOrZero(minimum_memory_for_elements(n)));
	}

	void reserve()
	{
		if (size() == 0)
			realloc(std::max(INITIAL_SIZE, minimum_memory_for_elements(1)));
		else
			realloc(allocated_size() * 2);
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
			memset(c_end, 0, byte_size(n - old_size));
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

	template <typename... Args>
	void emplace_back(Args &&... args)
	{
		if (unlikely(c_end == c_end_of_storage))
			reserve();

		new (t_end()) T(std::forward<Args>(args)...);
		c_end += byte_size(1);
	}

	void pop_back()
	{
		c_end -= byte_size(1);
	}

	/// Не вставляйте в массив кусок самого себя. Потому что при ресайзе, итераторы на самого себя могут инвалидироваться.
	template <typename It1, typename It2>
	void insert(It1 from_begin, It2 from_end)
	{
		size_t required_capacity = size() + (from_end - from_begin);
		if (required_capacity > capacity())
			reserve(roundUpToPowerOfTwoOrZero(required_capacity));

		insert_assume_reserved(from_begin, from_end);
	}

	template <typename It1, typename It2>
	void insert(iterator it, It1 from_begin, It2 from_end)
	{
		size_t required_capacity = size() + (from_end - from_begin);
		if (required_capacity > capacity())
			reserve(roundUpToPowerOfTwoOrZero(required_capacity));

		size_t bytes_to_copy = byte_size(from_end - from_begin);
		size_t bytes_to_move = (end() - it) * sizeof(T);

		if (unlikely(bytes_to_move))
			memcpy(c_end + bytes_to_copy - bytes_to_move, c_end - bytes_to_move, bytes_to_move);

		memcpy(c_end - bytes_to_move, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
		c_end += bytes_to_copy;
	}

	template <typename It1, typename It2>
	void insert_assume_reserved(It1 from_begin, It2 from_end)
	{
		size_t bytes_to_copy = byte_size(from_end - from_begin);
		memcpy(c_end, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
		c_end += bytes_to_copy;
	}

	void swap(PODArray & rhs)
	{
		/// Swap two PODArray objects, arr1 and arr2, that satisfy the following conditions:
		/// - The elements of arr1 are stored on stack.
		/// - The elements of arr2 are stored on heap.
		auto swap_stack_heap = [](PODArray & arr1, PODArray & arr2)
		{
			size_t stack_size = arr1.size();
			size_t stack_allocated = arr1.allocated_size();

			size_t heap_size = arr2.size();
			size_t heap_allocated = arr2.allocated_size();

			/// Keep track of the stack content we have to copy.
			char * stack_c_start = arr1.c_start;

			/// arr1 takes ownership of the heap memory of arr2.
			arr1.c_start = arr2.c_start;
			arr1.c_end_of_storage = arr1.c_start + heap_allocated - arr1.pad_right;
			arr1.c_end = arr1.c_start + byte_size(heap_size);

			/// Allocate stack space for arr2.
			arr2.alloc(stack_allocated);
			/// Copy the stack content.
			memcpy(arr2.c_start, stack_c_start, byte_size(stack_size));
			arr2.c_end = arr2.c_start + byte_size(stack_size);
		};

		auto do_move = [](PODArray & src, PODArray & dest)
		{
			if (src.isAllocatedFromStack())
			{
				dest.dealloc();
				dest.alloc(src.allocated_size());
				memcpy(dest.c_start, src.c_start, byte_size(src.size()));
				dest.c_end = dest.c_start + (src.c_end - src.c_start);

				src.c_start = nullptr;
				src.c_end = nullptr;
				src.c_end_of_storage = nullptr;
			}
			else
			{
				std::swap(dest.c_start, src.c_start);
				std::swap(dest.c_end, src.c_end);
				std::swap(dest.c_end_of_storage, src.c_end_of_storage);
			}
		};

		if (!isInitialized() && !rhs.isInitialized())
			return;
		else if (!isInitialized() && rhs.isInitialized())
		{
			do_move(rhs, *this);
			return;
		}
		else if (isInitialized() && !rhs.isInitialized())
		{
			do_move(*this, rhs);
			return;
		}

		if (isAllocatedFromStack() && rhs.isAllocatedFromStack())
		{
			size_t min_size = std::min(size(), rhs.size());
			size_t max_size = std::max(size(), rhs.size());

			for (size_t i = 0; i < min_size; ++i)
				std::swap(this->operator[](i), rhs[i]);

			if (size() == max_size)
			{
				for (size_t i = min_size; i < max_size; ++i)
					rhs[i] = this->operator[](i);
			}
			else
			{
				for (size_t i = min_size; i < max_size; ++i)
					this->operator[](i) = rhs[i];
			}

			size_t lhs_size = size();
			size_t lhs_allocated = allocated_size();

			size_t rhs_size = rhs.size();
			size_t rhs_allocated = rhs.allocated_size();

			c_end_of_storage = c_start + rhs_allocated - pad_right;
			rhs.c_end_of_storage = rhs.c_start + lhs_allocated - pad_right;

			c_end = c_start + byte_size(rhs_size);
			rhs.c_end = rhs.c_start + byte_size(lhs_size);
		}
		else if (isAllocatedFromStack() && !rhs.isAllocatedFromStack())
			swap_stack_heap(*this, rhs);
		else if (!isAllocatedFromStack() && rhs.isAllocatedFromStack())
			swap_stack_heap(rhs, *this);
		else
		{
			std::swap(c_start, rhs.c_start);
			std::swap(c_end, rhs.c_end);
			std::swap(c_end_of_storage, rhs.c_end_of_storage);
		}
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
			reserve(roundUpToPowerOfTwoOrZero(required_capacity));

		size_t bytes_to_copy = byte_size(required_capacity);
		memcpy(c_start, reinterpret_cast<const void *>(&*from_begin), bytes_to_copy);
		c_end = c_start + bytes_to_copy;
	}

	void assign(const PODArray & from)
	{
		assign(from.begin(), from.end());
	}


	bool operator== (const PODArray & other) const
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

	bool operator!= (const PODArray & other) const
	{
		return !operator==(other);
	}
};

template <typename T, size_t INITIAL_SIZE, typename TAllocator, size_t pad_right_>
void swap(PODArray<T, INITIAL_SIZE, TAllocator, pad_right_> & lhs, PODArray<T, INITIAL_SIZE, TAllocator, pad_right_> & rhs)
{
	lhs.swap(rhs);
}

/** Для столбцов. Padding-а хватает, чтобы читать и писать xmm-регистр по адресу последнего элемента. */
template <typename T, size_t INITIAL_SIZE = 4096, typename TAllocator = Allocator<false>>
using PaddedPODArray = PODArray<T, INITIAL_SIZE, TAllocator, 15>;


inline constexpr size_t integerRound(size_t value, size_t dividend)
{
	return ((value + dividend - 1) / dividend) * dividend;
}

template <typename T, size_t stack_size_in_bytes>
using PODArrayWithStackMemory = PODArray<T, 0, AllocatorWithStackMemory<Allocator<false>, integerRound(stack_size_in_bytes, sizeof(T))>>;

}
