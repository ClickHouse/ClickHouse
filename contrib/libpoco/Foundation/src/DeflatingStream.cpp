//
// DeflatingStream.cpp
//
// $Id: //poco/1.4/Foundation/src/DeflatingStream.cpp#1 $
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


#include "Poco/DeflatingStream.h"
#include "Poco/Exception.h"


namespace Poco {


DeflatingStreamBuf::DeflatingStreamBuf(std::istream& istr, StreamType type, int level): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&istr),
	_pOstr(0),
	_eof(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[DEFLATE_BUFFER_SIZE];

	int rc = deflateInit2(&_zstr, level, Z_DEFLATED, 15 + (type == STREAM_GZIP ? 16 : 0), 8, Z_DEFAULT_STRATEGY);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


DeflatingStreamBuf::DeflatingStreamBuf(std::istream& istr, int windowBits, int level): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&istr),
	_pOstr(0),
	_eof(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[DEFLATE_BUFFER_SIZE];

	int rc = deflateInit2(&_zstr, level, Z_DEFLATED, windowBits, 8, Z_DEFAULT_STRATEGY);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


DeflatingStreamBuf::DeflatingStreamBuf(std::ostream& ostr, StreamType type, int level): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&ostr),
	_eof(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[DEFLATE_BUFFER_SIZE];

	int rc = deflateInit2(&_zstr, level, Z_DEFLATED, 15 + (type == STREAM_GZIP ? 16 : 0), 8, Z_DEFAULT_STRATEGY);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


DeflatingStreamBuf::DeflatingStreamBuf(std::ostream& ostr, int windowBits, int level): 
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&ostr),
	_eof(false)
{
	_zstr.zalloc    = Z_NULL;
	_zstr.zfree     = Z_NULL;
	_zstr.opaque    = Z_NULL;
	_zstr.next_in   = 0;
	_zstr.avail_in  = 0;
	_zstr.next_out  = 0;
	_zstr.avail_out = 0;

	_buffer = new char[DEFLATE_BUFFER_SIZE];

	int rc = deflateInit2(&_zstr, level, Z_DEFLATED, windowBits, 8, Z_DEFAULT_STRATEGY);
	if (rc != Z_OK) 
	{
		delete [] _buffer;
		throw IOException(zError(rc)); 
	}
}


DeflatingStreamBuf::~DeflatingStreamBuf()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
	delete [] _buffer;
	deflateEnd(&_zstr);
}


int DeflatingStreamBuf::close()
{
	BufferedStreamBuf::sync();
	_pIstr = 0;
	if (_pOstr)
	{
		if (_zstr.next_out)
		{
			int rc = deflate(&_zstr, Z_FINISH);
			if (rc != Z_OK && rc != Z_STREAM_END) throw IOException(zError(rc)); 
			_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE - _zstr.avail_out);
			if (!_pOstr->good()) throw IOException(zError(rc)); 
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = DEFLATE_BUFFER_SIZE;
			while (rc != Z_STREAM_END)
			{
				rc = deflate(&_zstr, Z_FINISH);
				if (rc != Z_OK && rc != Z_STREAM_END) throw IOException(zError(rc)); 
				_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE - _zstr.avail_out);
				if (!_pOstr->good()) throw IOException(zError(rc)); 
				_zstr.next_out  = (unsigned char*) _buffer;
				_zstr.avail_out = DEFLATE_BUFFER_SIZE;
			}
		}
		_pOstr->flush();
		_pOstr = 0;
	}
	return 0;
}


int DeflatingStreamBuf::sync()
{
	if (BufferedStreamBuf::sync())
		return -1;

	if (_pOstr && _zstr.next_out)
	{
		int rc = deflate(&_zstr, Z_SYNC_FLUSH);
		if (rc != Z_OK) throw IOException(zError(rc)); 
		_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE - _zstr.avail_out);
		if (!_pOstr->good()) throw IOException(zError(rc)); 
		while (_zstr.avail_out == 0) 
		{
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = DEFLATE_BUFFER_SIZE;
			rc = deflate(&_zstr, Z_SYNC_FLUSH);
			if (rc != Z_OK) throw IOException(zError(rc)); 
			_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE - _zstr.avail_out);
			if (!_pOstr->good()) throw IOException(zError(rc)); 
		};
		_zstr.next_out  = (unsigned char*) _buffer;
		_zstr.avail_out = DEFLATE_BUFFER_SIZE;
	}
	return 0;
}


int DeflatingStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	if (!_pIstr) return 0;
	if (_zstr.avail_in == 0 && !_eof)
	{
		int n = 0;
		if (_pIstr->good())
		{
			_pIstr->read(_buffer, DEFLATE_BUFFER_SIZE);
			n = static_cast<int>(_pIstr->gcount());
		}
		if (n > 0)
		{
			_zstr.next_in  = (unsigned char*) _buffer;
			_zstr.avail_in = n;
		}
		else
		{
			_zstr.next_in  = 0;
			_zstr.avail_in = 0;
			_eof = true;
		}
	}
	_zstr.next_out  = (unsigned char*) buffer;
	_zstr.avail_out = static_cast<unsigned>(length);
	for (;;)
	{
		int rc = deflate(&_zstr, _eof ? Z_FINISH : Z_NO_FLUSH);
		if (_eof && rc == Z_STREAM_END) 
		{
			_pIstr = 0;
			return static_cast<int>(length) - _zstr.avail_out;
		}
		if (rc != Z_OK) throw IOException(zError(rc)); 
		if (_zstr.avail_out == 0)
		{
			return static_cast<int>(length);
		}
		if (_zstr.avail_in == 0)
		{
			int n = 0;
			if (_pIstr->good())
			{
				_pIstr->read(_buffer, DEFLATE_BUFFER_SIZE);
				n = static_cast<int>(_pIstr->gcount());
			}
			if (n > 0)
			{
				_zstr.next_in  = (unsigned char*) _buffer;
				_zstr.avail_in = n;
			}
			else
			{
				_zstr.next_in  = 0;
				_zstr.avail_in = 0;
				_eof = true;
			}
		}
	}
}


int DeflatingStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	if (length == 0 || !_pOstr) return 0;

	_zstr.next_in   = (unsigned char*) buffer;
	_zstr.avail_in  = static_cast<unsigned>(length);
	_zstr.next_out  = (unsigned char*) _buffer;
	_zstr.avail_out = DEFLATE_BUFFER_SIZE;
	for (;;)
	{
		int rc = deflate(&_zstr, Z_NO_FLUSH);
		if (rc != Z_OK) throw IOException(zError(rc)); 
		if (_zstr.avail_out == 0)
		{
			_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE);
			if (!_pOstr->good()) throw IOException(zError(rc)); 
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = DEFLATE_BUFFER_SIZE;
		}
		if (_zstr.avail_in == 0)
		{
			_pOstr->write(_buffer, DEFLATE_BUFFER_SIZE - _zstr.avail_out);
			if (!_pOstr->good()) throw IOException(zError(rc)); 
			_zstr.next_out  = (unsigned char*) _buffer;
			_zstr.avail_out = DEFLATE_BUFFER_SIZE;
			break;
		}
	}
	return static_cast<int>(length);
}


DeflatingIOS::DeflatingIOS(std::ostream& ostr, DeflatingStreamBuf::StreamType type, int level):
	_buf(ostr, type, level)
{
	poco_ios_init(&_buf);
}


DeflatingIOS::DeflatingIOS(std::ostream& ostr, int windowBits, int level):
	_buf(ostr, windowBits, level)
{
	poco_ios_init(&_buf);
}


DeflatingIOS::DeflatingIOS(std::istream& istr, DeflatingStreamBuf::StreamType type, int level):
	_buf(istr, type, level)
{
	poco_ios_init(&_buf);
}


DeflatingIOS::DeflatingIOS(std::istream& istr, int windowBits, int level):
	_buf(istr, windowBits, level)
{
	poco_ios_init(&_buf);
}


DeflatingIOS::~DeflatingIOS()
{
}


DeflatingStreamBuf* DeflatingIOS::rdbuf()
{
	return &_buf;
}


DeflatingOutputStream::DeflatingOutputStream(std::ostream& ostr, DeflatingStreamBuf::StreamType type, int level):
	DeflatingIOS(ostr, type, level),
	std::ostream(&_buf)
{
}


DeflatingOutputStream::DeflatingOutputStream(std::ostream& ostr, int windowBits, int level):
	DeflatingIOS(ostr, windowBits, level),
	std::ostream(&_buf)
{
}


DeflatingOutputStream::~DeflatingOutputStream()
{
}


int DeflatingOutputStream::close()
{
	return _buf.close();
}


int DeflatingOutputStream::sync()
{
	return _buf.pubsync();
}


DeflatingInputStream::DeflatingInputStream(std::istream& istr, DeflatingStreamBuf::StreamType type, int level):
	DeflatingIOS(istr, type, level),
	std::istream(&_buf)
{
}


DeflatingInputStream::DeflatingInputStream(std::istream& istr, int windowBits, int level):
	DeflatingIOS(istr, windowBits, level),
	std::istream(&_buf)
{
}


DeflatingInputStream::~DeflatingInputStream()
{
}


} // namespace Poco
