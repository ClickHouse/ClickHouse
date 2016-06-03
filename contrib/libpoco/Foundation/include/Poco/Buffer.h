//
// Buffer.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Buffer.h#2 $
//
// Library: Foundation
// Package: Core
// Module:  Buffer
//
// Definition of the Buffer class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Buffer_INCLUDED
#define Foundation_Buffer_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include <cstring>
#include <cstddef>


namespace Poco {


template <class T>
class Buffer
	/// A buffer class that allocates a buffer of a given type and size 
	/// in the constructor and deallocates the buffer in the destructor.
	///
	/// This class is useful everywhere where a temporary buffer
	/// is needed.
{
public:
	Buffer(std::size_t capacity):
		_capacity(capacity),
		_used(capacity),
		_ptr(0),
		_ownMem(true)
		/// Creates and allocates the Buffer.
	{
		if (capacity > 0)
		{
			_ptr = new T[capacity];
		}
	}

	Buffer(T* pMem, std::size_t length):
		_capacity(length),
		_used(length),
		_ptr(pMem),
		_ownMem(false)
		/// Creates the Buffer. Length argument specifies the length
		/// of the supplied memory pointed to by pMem in the number
		/// of elements of type T. Supplied pointer is considered
		/// blank and not owned by Buffer, so in this case Buffer 
		/// only acts as a wrapper around externally supplied 
		/// (and lifetime-managed) memory.
	{
	}

	Buffer(const T* pMem, std::size_t length):
		_capacity(length),
		_used(length),
		_ptr(0),
		_ownMem(true)
		/// Creates and allocates the Buffer; copies the contents of
		/// the supplied memory into the buffer. Length argument specifies
		/// the length of the supplied memory pointed to by pMem in the
		/// number of elements of type T.
	{
		if (_capacity > 0)
		{
			_ptr = new T[_capacity];
			std::memcpy(_ptr, pMem, _used * sizeof(T));
		}
	}

	Buffer(const Buffer& other):
		/// Copy constructor.
		_capacity(other._used),
		_used(other._used),
		_ptr(0),
		_ownMem(true)
	{
		if (_used)
		{
			_ptr = new T[_used];
			std::memcpy(_ptr, other._ptr, _used * sizeof(T));
		}
	}

	Buffer& operator = (const Buffer& other)
		/// Assignment operator.
	{
		if (this != &other)
		{
			Buffer tmp(other);
			swap(tmp);
		}

		return *this;
	}

	~Buffer()
		/// Destroys the Buffer.
	{
		if (_ownMem) delete [] _ptr;
	}
	
	void resize(std::size_t newCapacity, bool preserveContent = true)
		/// Resizes the buffer capacity and size. If preserveContent is true,
		/// the content of the old buffer is copied over to the
		/// new buffer. The new capacity can be larger or smaller than
		/// the current one; if it is smaller, capacity will remain intact.
		/// Size will always be set to the new capacity.
		///  
		/// Buffers only wrapping externally owned storage can not be 
		/// resized. If resize is attempted on those, IllegalAccessException
		/// is thrown.
	{
		if (!_ownMem) throw Poco::InvalidAccessException("Cannot resize buffer which does not own its storage.");

		if (newCapacity > _capacity)
		{
			T* ptr = new T[newCapacity];
			if (preserveContent)
			{
				std::memcpy(ptr, _ptr, _used * sizeof(T));
			}
			delete [] _ptr;
			_ptr = ptr;
			_capacity = newCapacity;
		}
		
		_used = newCapacity;
	}
	
	void setCapacity(std::size_t newCapacity, bool preserveContent = true)
		/// Sets the buffer capacity. If preserveContent is true,
		/// the content of the old buffer is copied over to the
		/// new buffer. The new capacity can be larger or smaller than
		/// the current one; size will be set to the new capacity only if 
		/// new capacity is smaller than the current size, otherwise it will
		/// remain intact.
		/// 
		/// Buffers only wrapping externally owned storage can not be 
		/// resized. If resize is attempted on those, IllegalAccessException
		/// is thrown.
	{
		if (!_ownMem) throw Poco::InvalidAccessException("Cannot resize buffer which does not own its storage.");

		if (newCapacity != _capacity)
		{
			T* ptr = 0;
			if (newCapacity > 0)
			{
				ptr = new T[newCapacity];
				if (preserveContent)
				{
					std::size_t newSz = _used < newCapacity ? _used : newCapacity;
					std::memcpy(ptr, _ptr, newSz * sizeof(T));
				}
			}
			delete [] _ptr;
			_ptr = ptr;
			_capacity = newCapacity;

			if (newCapacity < _used) _used = newCapacity;
		}
	}

	void assign(const T* buf, std::size_t sz)
		/// Assigns the argument buffer to this buffer.
		/// If necessary, resizes the buffer.
	{
		if (0 == sz) return;
		if (sz > _capacity) resize(sz, false);
		std::memcpy(_ptr, buf, sz * sizeof(T));
		_used = sz;
	}

	void append(const T* buf, std::size_t sz)
		/// Resizes this buffer and appends the argument buffer.
	{
		if (0 == sz) return;
		resize(_used + sz, true);
		std::memcpy(_ptr + _used - sz, buf, sz * sizeof(T));
	}

	void append(T val)
		/// Resizes this buffer by one element and appends the argument value.
	{
		resize(_used + 1, true);
		_ptr[_used - 1] = val;
	}

	void append(const Buffer& buf)
		/// Resizes this buffer and appends the argument buffer.
	{
		append(buf.begin(), buf.size());
	}

	std::size_t capacity() const
		/// Returns the allocated memory size in elements.
	{
		return _capacity;
	}

	std::size_t capacityBytes() const
		/// Returns the allocated memory size in bytes.
	{
		return _capacity * sizeof(T);
	}

	void swap(Buffer& other)
	/// Swaps the buffer with another one.
	{
		using std::swap;

		swap(_ptr, other._ptr);
		swap(_capacity, other._capacity);
		swap(_used, other._used);
	}

	bool operator == (const Buffer& other) const
		/// Compare operator.
	{
		if (this != &other)
		{
			if (_used == other._used)
			{
				if (std::memcmp(_ptr, other._ptr, _used * sizeof(T)) == 0)
				{
					return true;
				}
			}
			return false;
		}

		return true;
	}

	bool operator != (const Buffer& other) const
		/// Compare operator.
	{
		return !(*this == other);
	}

	void clear()
		/// Sets the contents of the buffer to zero.
	{
		std::memset(_ptr, 0, _used * sizeof(T));
	}

	std::size_t size() const
		/// Returns the used size of the buffer in elements.
	{
		return _used;
	}

	std::size_t sizeBytes() const
		/// Returns the used size of the buffer in bytes.
	{
		return _used * sizeof(T);
	}
	
	T* begin()
		/// Returns a pointer to the beginning of the buffer.
	{
		return _ptr;
	}
	
	const T* begin() const
		/// Returns a pointer to the beginning of the buffer.
	{
		return _ptr;
	}

	T* end()
		/// Returns a pointer to end of the buffer.
	{
		return _ptr + _used;
	}
	
	const T* end() const
		/// Returns a pointer to the end of the buffer.
	{
		return _ptr + _used;
	}
	
	bool empty() const
		/// Return true if buffer is empty.
	{
		return 0 == _used;
	}

	T& operator [] (std::size_t index)
	{
		poco_assert (index < _used);
		
		return _ptr[index];
	}

	const T& operator [] (std::size_t index) const
	{
		poco_assert (index < _used);
		
		return _ptr[index];
	}

private:
	Buffer();

	std::size_t _capacity;
	std::size_t _used;
	T*          _ptr;
	bool        _ownMem;
};


} // namespace Poco


#endif // Foundation_Buffer_INCLUDED
