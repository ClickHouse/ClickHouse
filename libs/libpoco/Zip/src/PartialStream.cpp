//
// PartialStream.cpp
//
// $Id: //poco/1.4/Zip/src/PartialStream.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  PartialStream
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/PartialStream.h"
#include "Poco/Exception.h"
#include <cstring>


namespace Poco {
namespace Zip {


PartialStreamBuf::PartialStreamBuf(std::istream& in, std::ios::pos_type start, std::ios::pos_type end, const std::string& pre, const std::string& post, bool initStream):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_initialized(!initStream),
	_start(start),
	_numBytes(end-start),
	_bytesWritten(0),
	_pIstr(&in),
	_pOstr(0),
	_prefix(pre),
	_postfix(post),
	_ignoreStart(0),
	_buffer(0),
	_bufferOffset(0)
{
}


PartialStreamBuf::PartialStreamBuf(std::ostream& out, std::size_t start, std::size_t end, bool initStream):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_initialized(!initStream),
	_start(0),
	_numBytes(0),
	_bytesWritten(0),
	_pIstr(0),
	_pOstr(&out),
	_ignoreStart(start),
	_buffer(end),
	_bufferOffset(0)
{
}


PartialStreamBuf::~PartialStreamBuf()
{
}


int PartialStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	if (_pIstr == 0 ||length == 0) return -1;
	if (!_initialized)
	{
		_initialized = true;
		_pIstr->clear();
		_pIstr->seekg(_start, std::ios_base::beg);
		if (_pIstr->fail())
			throw Poco::IOException("Failed to reposition in stream");
	}
	if (!_prefix.empty())
	{
		std::streamsize tmp = (_prefix.size() > length)? length: static_cast<std::streamsize>(_prefix.size());
		std::memcpy(buffer, _prefix.c_str(), tmp);
		_prefix = _prefix.substr(tmp);
		return tmp;
	}

	if (_numBytes == 0)
	{
		if (!_postfix.empty())
		{
			std::streamsize tmp = (_postfix.size() > length)? length: static_cast<std::streamsize>(_postfix.size());
			std::memcpy(buffer, _postfix.c_str(), tmp);
			_postfix = _postfix.substr(tmp);
			return tmp;
		}
		else
			return -1;
	}

	if (!_pIstr->good())
		return -1;

	if (_numBytes < length)
		length = static_cast<std::streamsize>(_numBytes);

	_pIstr->read(buffer, length);
	std::streamsize bytesRead = _pIstr->gcount();
	_numBytes -= bytesRead;
	return bytesRead;

}


int PartialStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	if (_pOstr == 0 || length == 0) return -1;
	if (!_initialized)
	{
		_initialized = true;
		_pOstr->clear();
		if (_pOstr->fail())
			throw Poco::IOException("Failed to clear stream status");
	}

	if (_ignoreStart > 0)
	{
		if (_ignoreStart > length)
		{
			_ignoreStart -= length;
			// fake return values
			return length;
		}
		else
		{
			std::streamsize cnt = static_cast<std::streamsize>(length - _ignoreStart - _buffer.size());
			if (cnt > 0)
			{
				_pOstr->write(buffer+_ignoreStart, cnt);
				_bytesWritten += cnt;
			}

			// copy the rest into buffer
			cnt += static_cast<std::streamsize>(_ignoreStart);
			_ignoreStart = 0;
			poco_assert (cnt < length);
			_bufferOffset = length - cnt;
			std::memcpy(_buffer.begin(), buffer + cnt, _bufferOffset);

			return length;
		}
	}
	if (_buffer.size() > 0)
	{
		// always treat each write as the potential last one
		// thus first fill the buffer with the last n bytes of the msg

		// how much of the already cached data do we need to write?
		Poco::Int32 cache = _bufferOffset + length - _buffer.size();
		if (cache > 0)
		{
			if (cache > _bufferOffset)
				cache = _bufferOffset;
			_pOstr->write(_buffer.begin(), cache);
			_bytesWritten += cache;
			_bufferOffset -= cache;
			if (_bufferOffset > 0)
				std::memmove(_buffer.begin(), _buffer.begin()+cache, _bufferOffset);
		}

		// now fill up _buffer with the last bytes from buffer
		Poco::Int32 pos = static_cast<Poco::Int32>(length - static_cast<Poco::Int32>(_buffer.size()) + _bufferOffset);
		if (pos <= 0)
		{
			// all of the message goes to _buffer
			std::memcpy(_buffer.begin() + _bufferOffset, buffer, length);
		}
		else
		{
			poco_assert (_bufferOffset == 0);
			std::memcpy(_buffer.begin(), buffer+pos, _buffer.size());
			_bufferOffset = static_cast<Poco::UInt32>(_buffer.size());
			// the rest is written
			_pOstr->write(buffer, static_cast<std::streamsize>(length - _buffer.size()));
			_bytesWritten += (length - _buffer.size());
		}
	}
	else
	{
		_pOstr->write(buffer, length);
		_bytesWritten += length;
	}

	if (_pOstr->good())
		return length;

	throw Poco::IOException("Failed to write to output stream");
}


void PartialStreamBuf::close()
{
	// DONT write data from _buffer!
}


PartialIOS::PartialIOS(std::istream& istr, std::ios::pos_type start, std::ios::pos_type end, const std::string& pre, const std::string& post, bool initStream): _buf(istr, start, end, pre, post, initStream)
{
	poco_ios_init(&_buf);
}


PartialIOS::PartialIOS(std::ostream& ostr, std::size_t start, std::size_t end, bool initStream): _buf(ostr, start, end, initStream)
{
	poco_ios_init(&_buf);
}


PartialIOS::~PartialIOS()
{
}


PartialStreamBuf* PartialIOS::rdbuf()
{
	return &_buf;
}


PartialInputStream::PartialInputStream(std::istream& istr, std::ios::pos_type start, std::ios::pos_type end, bool initStream, const std::string& pre, const std::string& post): 
	PartialIOS(istr, start, end, pre, post, initStream), 
	std::istream(&_buf)
{
}


PartialInputStream::~PartialInputStream()
{
}


PartialOutputStream::PartialOutputStream(std::ostream& ostr, std::size_t start, std::size_t end, bool initStream):
	PartialIOS(ostr, start, end, initStream), 
	std::ostream(&_buf)
{
}


PartialOutputStream::~PartialOutputStream()
{
	try
	{
		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


} } // namespace Poco::Zip
