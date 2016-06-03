//
// FIFOBuffer.h
//
// $Id: //poco/1.4/Foundation/include/Poco/FIFOBuffer.h#2 $
//
// Library: Foundation
// Package: Core
// Module:  FIFOBuffer
//
// Definition of the FIFOBuffer class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FIFOBuffer_INCLUDED
#define Foundation_FIFOBuffer_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/Buffer.h"
#include "Poco/BasicEvent.h"
#include "Poco/Mutex.h"
#include "Poco/Format.h"


namespace Poco {


template <class T>
class BasicFIFOBuffer
	/// A simple buffer class with support for re-entrant,
	/// FIFO-style read/write operations, as well as (optional)
	/// empty/non-empty/full (i.e. writable/readable) transition
	/// notifications. Buffer can be flagged with end-of-file and
	/// error flags, which renders it un-readable/writable.
	///
	/// Critical portions of code are protected by a recursive mutex.
	/// However, to achieve thread-safety in cases where multiple
	/// member function calls are involved and have to be atomic,
	/// the mutex must be locked externally.
	/// 
	/// Buffer size, as well as amount of unread data and
	/// available space introspections are supported as well.
	///
	/// This class is useful anywhere where a FIFO functionality
	/// is needed.
{
public:
	typedef T Type;

	mutable Poco::BasicEvent<bool> writable;
		/// Event indicating "writability" of the buffer,
		/// triggered as follows:
		///
		///	* when buffer transitions from non-full to full, 
		///	  Writable event observers are notified, with 
		///	  false value as the argument
		///
		///	* when buffer transitions from full to non-full,
		///	  Writable event observers are notified, with 
		///	  true value as the argument

	mutable Poco::BasicEvent<bool> readable;
		/// Event indicating "readability" of the buffer,
		/// triggered as follows:
		///
		///	* when buffer transitions from non-empty to empty,
		///	  Readable event observers are notified, with false  
		///	  value as the argument
		///
		///	* when FIFOBuffer transitions from empty to non-empty,
		///	  Readable event observers are notified, with true value
		///	  as the argument

	BasicFIFOBuffer(std::size_t size, bool notify = false):
		_buffer(size),
		_begin(0),
		_used(0),
		_notify(notify),
		_eof(false),
		_error(false)
		/// Creates the FIFOBuffer.
	{
	}

	BasicFIFOBuffer(T* pBuffer, std::size_t size, bool notify = false):
		_buffer(pBuffer, size),
		_begin(0),
		_used(0),
		_notify(notify),
		_eof(false),
		_error(false)
		/// Creates the FIFOBuffer.
	{
	}

	BasicFIFOBuffer(const T* pBuffer, std::size_t size, bool notify = false):
		_buffer(pBuffer, size),
		_begin(0),
		_used(size),
		_notify(notify),
		_eof(false),
		_error(false)
		/// Creates the FIFOBuffer.
	{
	}

	~BasicFIFOBuffer()
		/// Destroys the FIFOBuffer.
	{
	}
	
	void resize(std::size_t newSize, bool preserveContent = true)
		/// Resizes the buffer. If preserveContent is true,
		/// the content of the old buffer is preserved.
		/// New size can be larger or smaller than
		/// the current size, but it must not be 0.
		/// Additionally, if the new length is smaller
		/// than currently used length and preserveContent
		/// is true, InvalidAccessException is thrown.
	{
		Mutex::ScopedLock lock(_mutex);

		if (preserveContent && (newSize < _used))
			throw InvalidAccessException("Can not resize FIFO without data loss.");
		
		std::size_t usedBefore = _used;
		_buffer.resize(newSize, preserveContent);
		if (!preserveContent) _used = 0;
		if (_notify) notify(usedBefore);
	}
	
	std::size_t peek(T* pBuffer, std::size_t length) const
		/// Peeks into the data currently in the FIFO
		/// without actually extracting it.
		/// If length is zero, the return is immediate.
		/// If length is greater than used length,
		/// it is substituted with the the current FIFO 
		/// used length.
		/// 
		/// Returns the number of elements copied in the 
		/// supplied buffer.
	{
		if (0 == length) return 0;
		Mutex::ScopedLock lock(_mutex);
		if (!isReadable()) return 0;
		if (length > _used) length = _used;
		std::memcpy(pBuffer, _buffer.begin() + _begin, length * sizeof(T));
		return length;
	}
	
	std::size_t peek(Poco::Buffer<T>& buffer, std::size_t length = 0) const
		/// Peeks into the data currently in the FIFO
		/// without actually extracting it.
		/// Resizes the supplied buffer to the size of
		/// data written to it. If length is not
		/// supplied by the caller or is greater than length 
		/// of currently used data, the current FIFO used 
		/// data length is substituted for it.
		/// 
		/// Returns the number of elements copied in the 
		/// supplied buffer.
	{
		Mutex::ScopedLock lock(_mutex);
		if (!isReadable()) return 0;
		if (0 == length || length > _used) length = _used;
		buffer.resize(length);
		return peek(buffer.begin(), length);
	}
	
	std::size_t read(T* pBuffer, std::size_t length)
		/// Copies the data currently in the FIFO
		/// into the supplied buffer, which must be
		/// preallocated to at least the length size
		/// before calling this function.
		/// 
		/// Returns the size of the copied data.
	{
		if (0 == length) return 0;
		Mutex::ScopedLock lock(_mutex);
		if (!isReadable()) return 0;
		std::size_t usedBefore = _used;
		std::size_t readLen = peek(pBuffer, length);
		poco_assert (_used >= readLen);
		_used -= readLen;
		if (0 == _used) _begin = 0;
		else _begin += length;

		if (_notify) notify(usedBefore);

		return readLen;
	}
	
	std::size_t read(Poco::Buffer<T>& buffer, std::size_t length = 0)
		/// Copies the data currently in the FIFO
		/// into the supplied buffer.
		/// Resizes the supplied buffer to the size of
		/// data written to it.
		/// 
		/// Returns the size of the copied data.
	{
		Mutex::ScopedLock lock(_mutex);
		if (!isReadable()) return 0;
		std::size_t usedBefore = _used;
		std::size_t readLen = peek(buffer, length);
		poco_assert (_used >= readLen);
		_used -= readLen;
		if (0 == _used) _begin = 0;
		else _begin += length;

		if (_notify) notify(usedBefore);

		return readLen;
	}

	std::size_t write(const T* pBuffer, std::size_t length)
		/// Writes data from supplied buffer to the FIFO buffer.
		/// If there is no sufficient space for the whole
		/// buffer to be written, data up to available 
		/// length is written.
		/// The length of data to be written is determined from the
		/// length argument. Function does nothing and returns zero
		/// if length argument is equal to zero.
		/// 
		/// Returns the length of data written.
	{
		if (0 == length) return 0;

		Mutex::ScopedLock lock(_mutex);
		
		if (!isWritable()) return 0;
		
		if (_buffer.size() - (_begin + _used) < length)
		{
			std::memmove(_buffer.begin(), begin(), _used * sizeof(T));
			_begin = 0;
		}

		std::size_t usedBefore = _used;
		std::size_t available =  _buffer.size() - _used - _begin;
		std::size_t len = length > available ? available : length;
		std::memcpy(begin() + _used, pBuffer, len * sizeof(T));
		_used += len;
		poco_assert (_used <= _buffer.size());
		if (_notify) notify(usedBefore);

		return len;
	}

	std::size_t write(const Buffer<T>& buffer, std::size_t length = 0)
		/// Writes data from supplied buffer to the FIFO buffer.
		/// If there is no sufficient space for the whole
		/// buffer to be written, data up to available 
		/// length is written.
		/// The length of data to be written is determined from the
		/// length argument or buffer size (when length argument is
		/// default zero or greater than buffer size).
		/// 
		/// Returns the length of data written.
	{
		if (length == 0 || length > buffer.size())
			length = buffer.size();

		return write(buffer.begin(), length);
	}

	std::size_t size() const
		/// Returns the size of the buffer.
	{
		return _buffer.size();
	}
	
	std::size_t used() const
		/// Returns the size of the used portion of the buffer.
	{
		return _used;
	}
	
	std::size_t available() const
		/// Returns the size of the available portion of the buffer.
	{
		return size() - _used;
	}

	void drain(std::size_t length = 0)
		/// Drains length number of elements from the buffer.
		/// If length is zero or greater than buffer current
		/// content length, buffer is emptied.
	{
		Mutex::ScopedLock lock(_mutex);

		std::size_t usedBefore = _used;

		if (0 == length || length >= _used)
		{
			_begin = 0;
			_used = 0;
		}
		else
		{
			_begin += length;
			_used -= length;
		}

		if (_notify) notify(usedBefore);
	}

	void copy(const T* ptr, std::size_t length)
		/// Copies the supplied data to the buffer and adjusts
		/// the used buffer size.
	{
		poco_check_ptr(ptr);
		if (0 == length) return;

		Mutex::ScopedLock lock(_mutex);
		
		if (length > available())
			throw Poco::InvalidAccessException("Cannot extend buffer.");
		
		if (!isWritable())
			throw Poco::InvalidAccessException("Buffer not writable.");

		std::memcpy(&_buffer[_used], ptr, length * sizeof(T));
		std::size_t usedBefore = _used;
		_used += length;
		if (_notify) notify(usedBefore);
	}

	void advance(std::size_t length)
		/// Advances buffer by length elements.
		/// Should be called AFTER the data 
		/// was copied into the buffer.
	{
		Mutex::ScopedLock lock(_mutex);

		if (length > available())
			throw Poco::InvalidAccessException("Cannot extend buffer.");
		
		if (!isWritable())
			throw Poco::InvalidAccessException("Buffer not writable.");

		if (_buffer.size() - (_begin + _used) < length)
		{
			std::memmove(_buffer.begin(), begin(), _used * sizeof(T));
			_begin = 0;
		}

		std::size_t usedBefore = _used;
		_used += length;
		if (_notify) notify(usedBefore);
	}

	T* begin()
		/// Returns the pointer to the beginning of the buffer.
	{
		Mutex::ScopedLock lock(_mutex);
		if (_begin != 0)
		{
			// Move the data to the start of the buffer so begin() and next()
			// always return consistent pointers with each other and allow writing
			// to the end of the buffer.
			std::memmove(_buffer.begin(), _buffer.begin() + _begin, _used * sizeof(T));
			_begin = 0;
		}
		return _buffer.begin();
	}

	T* next()
		/// Returns the pointer to the next available position in the buffer.
	{
		Mutex::ScopedLock lock(_mutex);
		return begin() + _used;
	}

	T& operator [] (std::size_t index)
		/// Returns value at index position.
		/// Throws InvalidAccessException if index is larger than 
		/// the last valid (used) buffer position.
	{
		Mutex::ScopedLock lock(_mutex);
		if (index >= _used)
			throw InvalidAccessException(format("Index out of bounds: %z (max index allowed: %z)", index, _used - 1));

		return _buffer[_begin + index];
	}

	const T& operator [] (std::size_t index) const
		/// Returns value at index position.
		/// Throws InvalidAccessException if index is larger than 
		/// the last valid (used) buffer position.
	{
		Mutex::ScopedLock lock(_mutex);
		if (index >= _used)
			throw InvalidAccessException(format("Index out of bounds: %z (max index allowed: %z)", index, _used - 1));

		return _buffer[_begin + index];
	}

	const Buffer<T>& buffer() const
		/// Returns const reference to the underlying buffer.
	{
		return _buffer;
	}
	
	void setError(bool error = true)
		/// Sets the error flag on the buffer and empties it.
		/// If notifications are enabled, they will be triggered 
		/// if appropriate.
		/// 
		/// Setting error flag to true prevents reading and writing
		/// to the buffer; to re-enable FIFOBuffer for reading/writing,
		/// the error flag must be set to false.
	{
		if (error)
		{
			bool f = false;
			Mutex::ScopedLock lock(_mutex);
			if (error && isReadable() && _notify) readable.notify(this, f);
			if (error && isWritable() && _notify) writable.notify(this, f);
			_error = error;
			_used = 0;
		}
		else
		{
			bool t = true;
			Mutex::ScopedLock lock(_mutex);
			_error = false;
			if (_notify && !_eof) writable.notify(this, t);
		}
	}

	bool isValid() const
		/// Returns true if error flag is not set on the buffer,
		/// otherwise returns false.
	{
		return !_error;
	}

	void setEOF(bool eof = true)
		/// Sets end-of-file flag on the buffer.
		/// 
		/// Setting EOF flag to true prevents writing to the
		/// buffer; reading from the buffer will still be
		/// allowed until all data present in the buffer at the 
		/// EOF set time is drained. After that, to re-enable 
		/// FIFOBuffer for reading/writing, EOF must be
		/// set to false.
		/// 
		/// Setting EOF flag to false clears EOF state if it
		/// was previously set. If EOF was not set, it has no
		/// effect.
	{
		Mutex::ScopedLock lock(_mutex);
		bool flag = !eof;
		if (_notify) writable.notify(this, flag);
		_eof = eof;
	}

	bool hasEOF() const
		/// Returns true if EOF flag has been set.
	{
		return _eof;
	}

	bool isEOF() const
		/// Returns true if EOF flag has been set and buffer is empty.
	{
		return isEmpty() && _eof;
	}

	bool isEmpty() const
		/// Returns true is buffer is empty, false otherwise.
	{
		return 0 == _used;
	}

	bool isFull() const
		/// Returns true is buffer is full, false otherwise.
	{
		return size() == _used;
	}

	bool isReadable() const
		/// Returns true if buffer contains data and is not
		/// in error state.
	{
		return !isEmpty() && isValid();
	}

	bool isWritable() const
		/// Returns true if buffer is not full and is not
		/// in error state.
	{
		return !isFull() && isValid() && !_eof;
	}

	void setNotify(bool notify = true)
		/// Enables/disables notifications.
	{
		_notify = notify;
	}

	bool getNotify() const
		/// Returns true if notifications are enabled, false otherwise.
	{
		return _notify;
	}

	Mutex& mutex()
		/// Returns reference to mutex.
	{
		return _mutex;
	}

private:
	void notify(std::size_t usedBefore)
	{
		bool t = true, f = false;
		if (usedBefore == 0 && _used > 0)
			readable.notify(this, t);
		else if (usedBefore > 0 && 0 == _used)
			readable.notify(this, f);
		
		if (usedBefore == _buffer.size() && _used < _buffer.size())
			writable.notify(this, t);
		else if (usedBefore < _buffer.size() && _used == _buffer.size())
			writable.notify(this, f);
	}

	BasicFIFOBuffer();
	BasicFIFOBuffer(const BasicFIFOBuffer&);
	BasicFIFOBuffer& operator = (const BasicFIFOBuffer&);

	Buffer<T>     _buffer;
	std::size_t   _begin;
	std::size_t   _used;
	bool          _notify;
	mutable Mutex _mutex;
	bool          _eof;
	bool          _error;
};


//
// We provide an instantiation for char
//
typedef BasicFIFOBuffer<char> FIFOBuffer;


} // namespace Poco


#endif // Foundation_FIFOBuffer_INCLUDED
