//
// BufferedStreamBuf.h
//
// Library: Foundation
// Package: Streams
// Module:  StreamBuf
//
// Definition of template BasicBufferedStreamBuf and class BufferedStreamBuf.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_BufferedStreamBuf_INCLUDED
#define Foundation_BufferedStreamBuf_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/BufferAllocator.h"
#include "Poco/StreamUtil.h"
#include <streambuf>
#include <iosfwd>
#include <ios>


namespace Poco {


template <typename ch, typename tr, typename ba = BufferAllocator<ch> > 
class BasicBufferedStreamBuf: public std::basic_streambuf<ch, tr>
	/// This is an implementation of a buffered streambuf
	/// that greatly simplifies the implementation of
	/// custom streambufs of various kinds.
	/// Derived classes only have to override the methods
	/// readFromDevice() or writeToDevice().
	///
	/// This streambuf only supports unidirectional streams.
	/// In other words, the BasicBufferedStreamBuf can be
	/// used for the implementation of an istream or an
	/// ostream, but not for an iostream.
{
protected:
	typedef std::basic_streambuf<ch, tr> Base;
	typedef std::basic_ios<ch, tr> IOS;
	typedef ch char_type;
	typedef tr char_traits;
	typedef ba Allocator;
	typedef typename Base::int_type int_type;
	typedef typename Base::pos_type pos_type;
	typedef typename Base::off_type off_type;
	typedef typename IOS::openmode openmode;

public:
	BasicBufferedStreamBuf(std::streamsize bufferSize, openmode mode):
		_bufsize(bufferSize),
		_pBuffer(Allocator::allocate(_bufsize)),
		_mode(mode)
	{
		this->setg(_pBuffer + 4, _pBuffer + 4, _pBuffer + 4);
		this->setp(_pBuffer, _pBuffer + _bufsize);
	}

	~BasicBufferedStreamBuf()
	{
		Allocator::deallocate(_pBuffer, _bufsize);
	}

	virtual int_type overflow(int_type c)
	{
		if (!(_mode & IOS::out)) return char_traits::eof();

		if (flushBuffer() == std::streamsize(-1)) return char_traits::eof();
		if (c != char_traits::eof()) 
		{
			*this->pptr() = char_traits::to_char_type(c);
			this->pbump(1);
		}

		return c;
	}

	virtual int_type underflow()
	{
		if (!(_mode & IOS::in)) return char_traits::eof();

		if (this->gptr() && (this->gptr() < this->egptr()))
			return char_traits::to_int_type(*this->gptr());

		int putback = int(this->gptr() - this->eback());
		if (putback > 4) putback = 4;

		char_traits::move(_pBuffer + (4 - putback), this->gptr() - putback, putback);

		int n = readFromDevice(_pBuffer + 4, _bufsize - 4);
		if (n <= 0) return char_traits::eof();

		this->setg(_pBuffer + (4 - putback), _pBuffer + 4, _pBuffer + 4 + n);

		// return next character
		return char_traits::to_int_type(*this->gptr());    
	}

	virtual int sync()
	{
		if (this->pptr() && this->pptr() > this->pbase()) 
		{
			if (flushBuffer() == -1) return -1;
		}
		return 0;
	}

protected:
	void setMode(openmode mode)
	{
		_mode = mode;
	}
	
	openmode getMode() const
	{
		return _mode;
	}

private:
	virtual int readFromDevice(char_type* /*buffer*/, std::streamsize /*length*/)
	{
		return 0;
	}

	virtual int writeToDevice(const char_type* /*buffer*/, std::streamsize /*length*/)
	{
		return 0;
	}

	int flushBuffer()
	{
		int n = int(this->pptr() - this->pbase());
		if (writeToDevice(this->pbase(), n) == n) 
		{
			this->pbump(-n);
			return n;
		}
		return -1;
	}

	std::streamsize _bufsize;
	char_type*      _pBuffer;
	openmode        _mode;

	BasicBufferedStreamBuf(const BasicBufferedStreamBuf&);
	BasicBufferedStreamBuf& operator = (const BasicBufferedStreamBuf&);
};


//
// We provide an instantiation for char.
//
// Visual C++ needs a workaround - explicitly importing the template
// instantiation - to avoid duplicate symbols due to multiple
// instantiations in different libraries.
//
#if defined(_MSC_VER) && defined(POCO_DLL) && !defined(Foundation_EXPORTS)
template class Foundation_API BasicBufferedStreamBuf<char, std::char_traits<char> >;
#endif
typedef BasicBufferedStreamBuf<char, std::char_traits<char> > BufferedStreamBuf;


} // namespace Poco


#endif // Foundation_BufferedStreamBuf_INCLUDED
