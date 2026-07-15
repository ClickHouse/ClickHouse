//
// MultipartReader.cpp
//
// Library: Net
// Package: Messages
// Module:  MultipartReader
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MultipartReader.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Net/NetException.h"
#include "Poco/Ascii.h"


using Poco::BufferedStreamBuf;


namespace Poco {
namespace Net {


//
// MultipartStreamBuf
//


MultipartStreamBuf::MultipartStreamBuf(std::istream& istr, const std::string& boundary):
	BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_istr(istr),
	_boundary(boundary),
	_lastPart(false)
{
}


MultipartStreamBuf::~MultipartStreamBuf()
{
}


int MultipartStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	poco_assert (!_boundary.empty() && _boundary.length() < length - 6);

	static const int eof = std::char_traits<char>::eof();
	std::streambuf& buf = *_istr.rdbuf();

	int n  = 0;
	int ch = buf.sbumpc();
	if (ch == eof) return -1;
	*buffer++ = (char) ch; ++n;
	if (ch == '\n' || (ch == '\r' && buf.sgetc() == '\n'))
	{
		if (ch == '\r')
		{
			ch = buf.sbumpc(); // '\n'
			*buffer++ = (char) ch; ++n;
		}
		ch = buf.sgetc();
		if (ch == '\r' || ch == '\n') return n;
		*buffer++ = (char) buf.sbumpc(); ++n;
		if (ch == '-' && buf.sgetc() == '-')
		{
			ch = buf.sbumpc(); // '-'
			*buffer++ = (char) ch; ++n;
			std::string::const_iterator it  = _boundary.begin();
			std::string::const_iterator end = _boundary.end();
			ch = buf.sbumpc();
			*buffer++ = (char) ch; ++n;
			while (it != end && ch == *it)
			{
				++it;
				ch = buf.sbumpc();
				*buffer++ = (char) ch; ++n;
			}
			if (it == end)
			{
				if (ch == '\n' || (ch == '\r' && buf.sgetc() == '\n'))
				{
					if (ch == '\r')
					{
						buf.sbumpc(); // '\n'
					}
					return 0;
				}
				else if (ch == '-' && buf.sgetc() == '-')
				{
					buf.sbumpc(); // '-'
					_lastPart = true;
					return 0;
				}
			}
		}
	}
	ch = buf.sgetc();
	while (ch != eof && ch != '\r' && ch != '\n' && n < length)
	{
		*buffer++ = (char) buf.sbumpc(); ++n;
		ch = buf.sgetc();
	}
	return n;
}


bool MultipartStreamBuf::lastPart() const
{
	return _lastPart;
}


//
// MultipartIOS
//


MultipartIOS::MultipartIOS(std::istream& istr, const std::string& boundary):
	_buf(istr, boundary)
{
	poco_ios_init(&_buf);
}


MultipartIOS::~MultipartIOS()
{
	try
	{
		_buf.sync();
	}
	catch (...)
	{
	}
}


MultipartStreamBuf* MultipartIOS::rdbuf()
{
	return &_buf;
}


bool MultipartIOS::lastPart() const
{
	return _buf.lastPart();
}


//
// MultipartInputStream
//


MultipartInputStream::MultipartInputStream(std::istream& istr, const std::string& boundary):
	MultipartIOS(istr, boundary),
	std::istream(&_buf)
{
}


MultipartInputStream::~MultipartInputStream()
{
}


//
// MultipartReader
//


MultipartReader::MultipartReader(std::istream& istr):
	_istr(istr)
{
}


MultipartReader::MultipartReader(std::istream& istr, const std::string& boundary):
	_istr(istr),
	_boundary(boundary)
{
}


MultipartReader::~MultipartReader()
{

}


void MultipartReader::nextPart(MessageHeader& messageHeader)
{
	if (!_pMPI)
	{
		if (_boundary.empty())
			guessBoundary();
		else
			findFirstBoundary();
	}
	else if (_pMPI->lastPart())
	{
		throw MultipartException("No more parts available");
	}
	parseHeader(messageHeader);
	_pMPI = std::make_unique<MultipartInputStream>(_istr, _boundary);
}


bool MultipartReader::hasNextPart()
{
	return (!_pMPI || !_pMPI->lastPart()) && _istr.good();
}


std::istream& MultipartReader::stream() const
{
	poco_check_ptr (_pMPI);

	return *_pMPI;
}


const std::string& MultipartReader::boundary() const
{
	return _boundary;
}


void MultipartReader::findFirstBoundary()
{
	std::string expect("--");
	expect.append(_boundary);
	std::string line;
	line.reserve(expect.length());
	bool ok = true;
	do
	{
		ok = readLine(line, expect.length());
	}
	while (ok && line != expect);

	if (!ok) throw MultipartException("No boundary line found");
}


void MultipartReader::guessBoundary()
{
	static const int eof = std::char_traits<char>::eof();
	int ch = _istr.get();
	while (Poco::Ascii::isSpace(ch))
		ch = _istr.get();
	if (ch == '-' && _istr.peek() == '-')
	{
		_istr.get();
		ch = _istr.peek();
		while (ch != eof && ch != '\r' && ch != '\n' && _boundary.size() < 128) // Note: should be no longer than 70 chars acc. to RFC 2046
		{
			_boundary += (char) _istr.get();
			ch = _istr.peek();
		}
		if (ch != '\r' && ch != '\n')
			throw MultipartException("Invalid boundary line found");
		if (ch == '\r' || ch == '\n')
			_istr.get();
		if (_istr.peek() == '\n')
			_istr.get();
	}
	else throw MultipartException("No boundary line found");
}


void MultipartReader::parseHeader(MessageHeader& messageHeader)
{
	messageHeader.clear();
	messageHeader.read(_istr);
	int ch = _istr.get();
	if (ch == '\r' && _istr.peek() == '\n') _istr.get();
}


bool MultipartReader::readLine(std::string& line, std::string::size_type n)
{
	static const int eof = std::char_traits<char>::eof();
	static const int maxLength = 1024;

	line.clear();
	int ch = _istr.peek();
	int length = 0;
	while (ch != eof && ch != '\r' && ch != '\n' && length < maxLength)
	{
		ch = (char) _istr.get();
		if (line.length() < n) line += ch;
		ch = _istr.peek();
		length++;
	}
	if (ch != eof) _istr.get();
	if (ch == '\r' && _istr.peek() == '\n') _istr.get();
	return ch != eof && length < maxLength;
}


} } // namespace Poco::Net
