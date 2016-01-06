//
// AutoDetectStream.cpp
//
// $Id: //poco/1.4/Zip/src/AutoDetectStream.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  AutoDetectStream
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/AutoDetectStream.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipArchiveInfo.h"
#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Zip/ZipFileInfo.h"
#include "Poco/Exception.h"
#include <cstring>


namespace Poco {
namespace Zip {


AutoDetectStreamBuf::AutoDetectStreamBuf(std::istream& in, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&in),
	_pOstr(0),
	_eofDetected(false),
	_matchCnt(0),
	_prefix(pre),
	_postfix(post),
	_reposition(reposition),
	_start(start)
{
}


AutoDetectStreamBuf::AutoDetectStreamBuf(std::ostream& out):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&out),
	_eofDetected(false),
	_matchCnt(0),
	_prefix(),
	_postfix(),
	_reposition(false),
	_start(0u)
{
}


AutoDetectStreamBuf::~AutoDetectStreamBuf()
{
}


int AutoDetectStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	poco_assert_dbg(length >= 8);
	if (_pIstr == 0 ||length == 0) return -1;

	if (_reposition)
	{
		_pIstr->seekg(_start, std::ios_base::beg);
		_reposition = false;
	}

	if (!_prefix.empty())
	{
		std::streamsize tmp = (_prefix.size() > length)? length: static_cast<std::streamsize>(_prefix.size());
		std::memcpy(buffer, _prefix.c_str(), tmp);
		_prefix = _prefix.substr(tmp);
		return tmp;
	}

	if (_eofDetected)
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

	char byte3('\x00');
	std::streamsize tempPos = 0;
	static std::istream::int_type eof = std::istream::traits_type::eof();
	while (_pIstr->good() && !_pIstr->eof() && (tempPos+4) < length)
	{ 
		std::istream::int_type c = _pIstr->get();
		if (c != eof)
		{
			// all zip headers start with the same 2byte prefix
			if (_matchCnt<2)
			{
				if (c == ZipLocalFileHeader::HEADER[_matchCnt])
					++_matchCnt;
				else
				{
					// matchcnt was either 0 or 1 the headers have all unique chars -> safe to set to 0
					if (_matchCnt == 1)
					{
						buffer[tempPos++] = ZipLocalFileHeader::HEADER[0];
					}
					_matchCnt = 0;

					buffer[tempPos++] = static_cast<char>(c);
				}
			}
			else
			{
				//the upper 2 bytes differ: the lower one must be in range 1,3,5,7, the upper must be one larger: 2,4,6,8
				if (_matchCnt == 2)
				{
					if (ZipLocalFileHeader::HEADER[2] == c || 
						ZipArchiveInfo::HEADER[2] == c || 
						ZipFileInfo::HEADER[2] == c || 
						ZipDataInfo::HEADER[2] == c)
					{
						byte3 = static_cast<char>(c);;
						_matchCnt++; 
					}
					else
					{
						buffer[tempPos++] = ZipLocalFileHeader::HEADER[0];
						buffer[tempPos++] = ZipLocalFileHeader::HEADER[1];
						buffer[tempPos++] = static_cast<char>(c);
						_matchCnt = 0;
					}
				}
				else if (_matchCnt == 3)
				{
					if (c-1 == byte3)
					{
						// a match, pushback
						_pIstr->putback(c);
						_pIstr->putback(byte3);
						_pIstr->putback(ZipLocalFileHeader::HEADER[1]);
						_pIstr->putback(ZipLocalFileHeader::HEADER[0]);
						_eofDetected = true;
						return tempPos;
					}
					else
					{
						buffer[tempPos++] = ZipLocalFileHeader::HEADER[0];
						buffer[tempPos++] = ZipLocalFileHeader::HEADER[1];
						buffer[tempPos++] = byte3;
						buffer[tempPos++] = c;
						_matchCnt = 0; //the headers have all unique chars -> safe to set to 0
					}
				}
			}
		}
	}

	return tempPos;

}


int AutoDetectStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	if (_pOstr == 0 || length == 0) return -1;
	_pOstr->write(buffer, length);
	if (_pOstr->good())
		return length;
	throw Poco::IOException("Failed to write to output stream");
}


AutoDetectIOS::AutoDetectIOS(std::istream& istr, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start):
	_buf(istr, pre, post, reposition, start)
{
	poco_ios_init(&_buf);
}


AutoDetectIOS::AutoDetectIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


AutoDetectIOS::~AutoDetectIOS()
{
}


AutoDetectStreamBuf* AutoDetectIOS::rdbuf()
{
	return &_buf;
}


AutoDetectInputStream::AutoDetectInputStream(std::istream& istr, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start):
	AutoDetectIOS(istr, pre, post, reposition, start),
	std::istream(&_buf)
{
}


AutoDetectInputStream::~AutoDetectInputStream()
{
}


AutoDetectOutputStream::AutoDetectOutputStream(std::ostream& ostr):
	AutoDetectIOS(ostr), 
	std::ostream(&_buf)
{
}


AutoDetectOutputStream::~AutoDetectOutputStream()
{
}


} } // namespace Poco::Zip
