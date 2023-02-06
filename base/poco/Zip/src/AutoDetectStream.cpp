//
// AutoDetectStream.cpp
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


AutoDetectStreamBuf::AutoDetectStreamBuf(std::istream& in, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start, bool needsZip64):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&in),
	_eofDetected(false),
	_matchCnt(0),
	_prefix(pre),
	_postfix(post),
	_reposition(reposition),
	_start(start),
	_needsZip64(needsZip64),
	_length(0)
{
}


AutoDetectStreamBuf::~AutoDetectStreamBuf()
{
}


int AutoDetectStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	poco_assert_dbg(length >= 8);
	if (_pIstr == 0 || length == 0) return -1;

	if (_reposition)
	{
		_pIstr->seekg(_start, std::ios_base::beg);
		_reposition = false;
		if (!_pIstr->good()) return -1;
	}

	if (!_prefix.empty())
	{
		std::streamsize n = (_prefix.size() > length) ? length : static_cast<std::streamsize>(_prefix.size());
		std::memcpy(buffer, _prefix.data(), n);
		_prefix.erase(0, n);
		return static_cast<int>(n);
	}

	if (_eofDetected)
	{
		if (!_postfix.empty())
		{
			std::streamsize n = (_postfix.size() > length) ? length : static_cast<std::streamsize>(_postfix.size());
			std::memcpy(buffer, _postfix.data(), n);
			_postfix.erase(0, n);
			return static_cast<int>(n);
		}
		else return -1;
	}

	if (!_pIstr->good()) return -1;

	std::streamsize offset = 0;
	static std::istream::int_type eof = std::istream::traits_type::eof();
	while (_pIstr->good() && !_pIstr->eof() && (offset + 4) < length)
	{
		std::istream::int_type c = _pIstr->get();
		if (c != eof)
		{
			if (_matchCnt < 3)
			{
				if (c == ZipDataInfo::HEADER[_matchCnt])
				{
					++_matchCnt;
				}
				else
				{
					for (int i = 0; i < _matchCnt; i++)
					{
						buffer[offset++] = ZipDataInfo::HEADER[i];
					}
					if (c == ZipDataInfo::HEADER[0])
					{
						_matchCnt = 1;
					}
					else
					{
						_matchCnt = 0;
						buffer[offset++] = static_cast<char>(c);
					}
				}
			}
			else if (_matchCnt == 3)
			{
				if (ZipDataInfo::HEADER[3] == c)
				{
					std::streamsize dataInfoSize = 0;
					if (_needsZip64)
					{
						ZipDataInfo64 dataInfo(*_pIstr, true);
						if (!_pIstr->good()) throw Poco::IOException("Failed to read data descriptor");

						dataInfoSize = dataInfo.getFullHeaderSize();
						if (dataInfo.getCompressedSize() == _length + offset)
						{
							_pIstr->seekg(-static_cast<int>(dataInfoSize), std::ios::cur);
							if (!_pIstr->good()) throw Poco::IOException("Failed to seek on input stream");

							_eofDetected = true;
							_length += offset;
						
							if (offset == 0 && !_postfix.empty())
							{
								offset = (_postfix.size() > length) ? length : static_cast<std::streamsize>(_postfix.size());
								std::memcpy(buffer, _postfix.data(), offset);
								_postfix.erase(0, offset);
							}
						
							return static_cast<int>(offset);
						}
					}
					else
					{
						ZipDataInfo dataInfo(*_pIstr, true);
						if (!_pIstr->good()) throw Poco::IOException("Failed to read data descriptor");

						dataInfoSize = dataInfo.getFullHeaderSize();
						if (dataInfo.getCompressedSize() == _length + offset)
						{
							_pIstr->seekg(-static_cast<int>(dataInfoSize), std::ios::cur);
							if (!_pIstr->good()) throw Poco::IOException("Failed to seek on input stream");

							_eofDetected = true;
							_length += offset;
						
							if (offset == 0 && !_postfix.empty())
							{
								offset = (_postfix.size() > length) ? length : static_cast<std::streamsize>(_postfix.size());
								std::memcpy(buffer, _postfix.data(), offset);
								_postfix.erase(0, offset);
							}
						
							return static_cast<int>(offset);
						}
					}

					_pIstr->seekg(-static_cast<int>(dataInfoSize - 4), std::ios::cur);
					if (!_pIstr->good()) throw Poco::IOException("Failed to seek on input stream");

					buffer[offset++] = ZipDataInfo::HEADER[0];
					buffer[offset++] = ZipDataInfo::HEADER[1];
					buffer[offset++] = ZipDataInfo::HEADER[2];
					buffer[offset++] = ZipDataInfo::HEADER[3];
					_matchCnt = 0;
				}
				else
				{
					buffer[offset++] = ZipDataInfo::HEADER[0];
					buffer[offset++] = ZipDataInfo::HEADER[1];
					buffer[offset++] = ZipDataInfo::HEADER[2];
					buffer[offset++] = c;
					_matchCnt = 0;
				}
			}
		}
	}

	_length += offset;
	return static_cast<int>(offset);

}


int AutoDetectStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	return -1; // not supported
}


AutoDetectIOS::AutoDetectIOS(std::istream& istr, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start, bool needsZip64):
	_buf(istr, pre, post, reposition, start, needsZip64)
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


AutoDetectInputStream::AutoDetectInputStream(std::istream& istr, const std::string& pre, const std::string& post, bool reposition, Poco::UInt32 start, bool needsZip64):
	AutoDetectIOS(istr, pre, post, reposition, start, needsZip64),
	std::istream(&_buf)
{
}


AutoDetectInputStream::~AutoDetectInputStream()
{
}


} } // namespace Poco::Zip
