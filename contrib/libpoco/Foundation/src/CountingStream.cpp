//
// CountingStream.cpp
//
// $Id: //poco/1.4/Foundation/src/CountingStream.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  CountingStream
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CountingStream.h"


namespace Poco {


CountingStreamBuf::CountingStreamBuf(): 
	_pIstr(0), 
	_pOstr(0), 
	_chars(0), 
	_lines(0), 
	_pos(0)
{
}


CountingStreamBuf::CountingStreamBuf(std::istream& istr): 
	_pIstr(&istr), 
	_pOstr(0), 
	_chars(0), 
	_lines(0), 
	_pos(0)
{
}


CountingStreamBuf::CountingStreamBuf(std::ostream& ostr): 
	_pIstr(0), 
	_pOstr(&ostr), 
	_chars(0), 
	_lines(0), 
	_pos(0)
{
}


CountingStreamBuf::~CountingStreamBuf()
{
}


int CountingStreamBuf::readFromDevice()
{
	if (_pIstr)
	{
		int c = _pIstr->get();
		if (c != -1)
		{
			++_chars;
			if (_pos++ == 0) ++_lines;
			if (c == '\n') _pos = 0;
		}
		return c;
	}
	return -1;
}


int CountingStreamBuf::writeToDevice(char c)
{
	++_chars;
	if (_pos++ == 0) ++_lines;
	if (c == '\n') _pos = 0;
	if (_pOstr) _pOstr->put(c);
	return charToInt(c);
}


void CountingStreamBuf::reset()
{
	_chars = 0;
	_lines = 0;
	_pos   = 0;
}


void CountingStreamBuf::setCurrentLineNumber(int line)
{
	_lines = line;
}


void CountingStreamBuf::addChars(int chars)
{
	_chars += chars;
}

		
void CountingStreamBuf::addLines(int lines)
{
	_lines += lines;
}

		
void CountingStreamBuf::addPos(int pos)
{
	_pos += pos;
}


CountingIOS::CountingIOS()
{
	poco_ios_init(&_buf);
}


CountingIOS::CountingIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


CountingIOS::CountingIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


CountingIOS::~CountingIOS()
{
}


void CountingIOS::reset()
{
	_buf.reset();
}


void CountingIOS::setCurrentLineNumber(int line)
{
	_buf.setCurrentLineNumber(line);
}


void CountingIOS::addChars(int chars)
{
	_buf.addChars(chars);
}

		
void CountingIOS::addLines(int lines)
{
	_buf.addLines(lines);
}

		
void CountingIOS::addPos(int pos)
{
	_buf.addPos(pos);
}


CountingStreamBuf* CountingIOS::rdbuf()
{
	return &_buf;
}


CountingInputStream::CountingInputStream(std::istream& istr): CountingIOS(istr), std::istream(&_buf)
{
}


CountingInputStream::~CountingInputStream()
{
}


CountingOutputStream::CountingOutputStream(): std::ostream(&_buf)
{
}


CountingOutputStream::CountingOutputStream(std::ostream& ostr): CountingIOS(ostr), std::ostream(&_buf)
{
}


CountingOutputStream::~CountingOutputStream()
{
}


} // namespace Poco
