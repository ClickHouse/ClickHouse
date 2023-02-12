//
// LOBStream.h
//
// Library: Data
// Package: DataCore
// Module:  LOBStream
//
// Definition of the LOBStream class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_LOBStream_INCLUDED
#define Data_LOBStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include "Poco/Data/LOB.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace Data {


template <typename T>
class LOBStreamBuf: public BasicUnbufferedStreamBuf<T, std::char_traits<T> >
	/// This is the streambuf class used for reading from and writing to a LOB.
{
public:	
	LOBStreamBuf(LOB<T>& lob): _lob(lob), _it(_lob.begin())
		/// Creates LOBStreamBuf.
	{
	}


	~LOBStreamBuf()
		/// Destroys LOBStreamBuf.
	{
	}

protected:
	typedef std::char_traits<T> TraitsType;
	typedef BasicUnbufferedStreamBuf<T, TraitsType> BaseType;

	typename BaseType::int_type readFromDevice()
	{
		if (_it != _lob.end())
			return BaseType::charToInt(*_it++);
		else
			return -1;
	}

	typename BaseType::int_type writeToDevice(T c)
	{
		_lob.appendRaw(&c, 1);
		return 1;
	}

private:
	LOB<T>& _lob;
	typename LOB<T>::Iterator _it;
};


template <typename T>
class LOBIOS: public virtual std::ios
	/// The base class for LOBInputStream and
	/// LOBOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	LOBIOS(LOB<T>& lob, openmode mode): _buf(lob)
		/// Creates the LOBIOS with the given LOB.
	{
		poco_ios_init(&_buf);
	}

	~LOBIOS()
		/// Destroys the LOBIOS.
	{
	}

	LOBStreamBuf<T>* rdbuf()
		/// Returns a pointer to the internal LOBStreamBuf.
	{
		return &_buf;
	}

protected:
	LOBStreamBuf<T> _buf;
};


template <typename T>
class LOBOutputStream: public LOBIOS<T>, public std::basic_ostream<T, std::char_traits<T> >
	/// An output stream for writing to a LOB.
{
public:
	LOBOutputStream(LOB<T>& lob):
		LOBIOS<T>(lob, std::ios::out),
		std::ostream(LOBIOS<T>::rdbuf())
		/// Creates the LOBOutputStream with the given LOB.
	{
	}

	~LOBOutputStream()
		/// Destroys the LOBOutputStream.
	{
	}
};


template <typename T>
class LOBInputStream: public LOBIOS<T>, public std::basic_istream<T, std::char_traits<T> >
	/// An input stream for reading from a LOB.
{
public:
	LOBInputStream(LOB<T>& lob):
		LOBIOS<T>(lob, std::ios::in),
		std::istream(LOBIOS<T>::rdbuf())
		/// Creates the LOBInputStream with the given LOB.
	{
	}

	~LOBInputStream()
		/// Destroys the LOBInputStream.
	{
	}
};


typedef LOBOutputStream<unsigned char> BLOBOutputStream;
typedef LOBOutputStream<char> CLOBOutputStream;

typedef LOBInputStream<unsigned char> BLOBInputStream;
typedef LOBInputStream<char> CLOBInputStream;

} } // namespace Poco::Data


#endif // Data_LOBStream_INCLUDED
