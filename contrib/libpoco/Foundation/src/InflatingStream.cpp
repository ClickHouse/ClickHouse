//
// InflatingStream.cpp
//
// $Id: //poco/1.4/Foundation/src/InflatingStream.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  ZLibStream
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/InflatingStream.h"
#include "Poco/Exception.h"


namespace Poco {


InflatingStreamBuf::InflatingStreamBuf(std::istream& istr, StreamType type): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&istr),
	_pOstr(0),
	_eof(false),
	_check(type != STREAM_ZIP)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[INFLATE_BUFFER_SIZE];

	int rc = inflateInit2(&_zstr, 15 + (type == STREAM_GZIP ? 16 : 0));
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


InflatingStreamBuf::InflatingStreamBuf(std::istream& istr, int windowBits): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&istr),
	_pOstr(0),
	_eof(false),
	_check(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[INFLATE_BUFFER_SIZE];

	int rc = inflateInit2(&_zstr, windowBits);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


InflatingStreamBuf::InflatingStreamBuf(std::ostream& ostr, StreamType type): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&ostr),
	_eof(false),
	_check(type != STREAM_ZIP)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[INFLATE_BUFFER_SIZE];

	int rc = inflateInit2(&_zstr, 15 + (type == STREAM_GZIP ? 16 : 0));
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc));
	}
}


InflatingStreamBuf::InflatingStreamBuf(std::ostream& ostr, int windowBits): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&ostr),
	_eof(false),
	_check(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[INFLATE_BUFFER_SIZE];

	int rc = inflateInit2(&_zstr, windowBits);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc));
	}
}


InflatingStreamBuf::~InflatingStreamBuf()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
	delete [] _buffer;
	inflateEnd(&_zstr);
}


int InflatingStreamBuf::close()
{
	sync();
	_pIstr = 0;
	_pOstr = 0;
	return 0;
}


void InflatingStreamBuf::reset()
{
	int rc = inflateReset(&_zstr);
	if (rc == Z_OK)
		_eof = false;
	else
		throw IOException(zError(rc));
}


int InflatingStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	if (_eof || !_pIstr) return 0;

	if (_zstr.avail_in == 0)
	{
		int n = 0;
		if (_pIstr->good())
		{
			_pIstr->read(_buffer, INFLATE_BUFFER_SIZE);
			n = static_cast<int>(_pIstr->gcount());
		}
		_zstr.next_in   = (unsigned char*) _buffer;
		_zstr.avail_in  = n;
	}
	_zstr.next_out  = (unsigned char*) buffer;
	_zstr.avail_out = static_cast<unsigned>(length);
	for (;;)
	{
		int rc = inflate(&_zstr, Z_NO_FLUSH);
		if (rc == Z_DATA_ERROR && !_check)
		{
			if (_zstr.avail_in == 0)
			{
				if (_pIstr->good())
					rc = Z_OK;
				else
					rc = Z_STREAM_END;
			}
		}
		if (rc == Z_STREAM_END)
		{
			_eof = true;
			return static_cast<int>(length) - _zstr.avail_out;
		}
		if (rc != Z_OK) throw IOException(zError(rc));
		if (_zstr.avail_out == 0)
			return static_cast<int>(length);
		if (_zstr.avail_in == 0)
		{
			int n = 0;
			if (_pIstr->good())
			{
				_pIstr->read(_buffer, INFLATE_BUFFER_SIZE);
				n = static_cast<int>(_pIstr->gcount());
			}
			if (n > 0)
			{
				_zstr.next_in  = (unsigned char*) _buffer;
				_zstr.avail_in = n;
			} 
			else return static_cast<int>(length) - _zstr.avail_out;
		}
	}
}


int InflatingStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	if (length == 0 || !_pOstr) return 0;
	
	_zstr.next_in   = (unsigned char*) buffer;
	_zstr.avail_in  = static_cast<unsigned>(length);
	_zstr.next_out  = (unsigned char*) _buffer;
	_zstr.avail_out = INFLATE_BUFFER_SIZE;
	for (;;)
	{
		int rc = inflate(&_zstr, Z_NO_FLUSH);
		if (rc == Z_STREAM_END)
		{
			_pOstr->write(_buffer, INFLATE_BUFFER_SIZE - _zstr.avail_out);
			if (!_pOstr->good()) throw IOException(zError(rc));
			break;
		}
		if (rc != Z_OK) throw IOException(zError(rc)); 
		if (_zstr.avail_out == 0)
		{
			_pOstr->write(_buffer, INFLATE_BUFFER_SIZE);
			if (!_pOstr->good()) throw IOException(zError(rc));
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = INFLATE_BUFFER_SIZE;
		}
		if (_zstr.avail_in == 0)
		{
			_pOstr->write(_buffer, INFLATE_BUFFER_SIZE - _zstr.avail_out);
			if (!_pOstr->good()) throw IOException(zError(rc)); 
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = INFLATE_BUFFER_SIZE;
			break;
		}
	}
	return static_cast<int>(length);
}


int InflatingStreamBuf::sync()
{
	int n = BufferedStreamBuf::sync();
	if (!n && _pOstr) _pOstr->flush();
	return n;
}


InflatingIOS::InflatingIOS(std::ostream& ostr, InflatingStreamBuf::StreamType type):
	_buf(ostr, type)
{
	poco_ios_init(&_buf);
}


InflatingIOS::InflatingIOS(std::ostream& ostr, int windowBits):
	_buf(ostr, windowBits)
{
	poco_ios_init(&_buf);
}


InflatingIOS::InflatingIOS(std::istream& istr, InflatingStreamBuf::StreamType type):
	_buf(istr, type)
{
	poco_ios_init(&_buf);
}


InflatingIOS::InflatingIOS(std::istream& istr, int windowBits):
	_buf(istr, windowBits)
{
	poco_ios_init(&_buf);
}


InflatingIOS::~InflatingIOS()
{
}


InflatingStreamBuf* InflatingIOS::rdbuf()
{
	return &_buf;
}


InflatingOutputStream::InflatingOutputStream(std::ostream& ostr, InflatingStreamBuf::StreamType type):
	InflatingIOS(ostr, type),
	std::ostream(&_buf)
{
}


InflatingOutputStream::InflatingOutputStream(std::ostream& ostr, int windowBits):
	InflatingIOS(ostr, windowBits),
	std::ostream(&_buf)
{
}


InflatingOutputStream::~InflatingOutputStream()
{
}


int InflatingOutputStream::close()
{
	return _buf.close();
}


InflatingInputStream::InflatingInputStream(std::istream& istr, InflatingStreamBuf::StreamType type):
	InflatingIOS(istr, type),
	std::istream(&_buf)
{
}


InflatingInputStream::InflatingInputStream(std::istream& istr, int windowBits):
	InflatingIOS(istr, windowBits),
	std::istream(&_buf)
{
}


InflatingInputStream::~InflatingInputStream()
{
}


void InflatingInputStream::reset()
{
	_buf.reset();
	clear();
}


} // namespace Poco
