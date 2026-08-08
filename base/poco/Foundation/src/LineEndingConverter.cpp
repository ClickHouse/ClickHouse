//
// LineEndingConverter.cpp
//
// Library: Foundation
// Package: Streams
// Module:  LineEndingConverter
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LineEndingConverter.h"


namespace Poco {


const std::string LineEnding::NEWLINE_DEFAULT(POCO_DEFAULT_NEWLINE_CHARS);
const std::string LineEnding::NEWLINE_CR("\r");
const std::string LineEnding::NEWLINE_CRLF("\r\n");
const std::string LineEnding::NEWLINE_LF("\n");


LineEndingConverterStreamBuf::LineEndingConverterStreamBuf(std::istream& istr): 
	_pIstr(&istr), 
	_pOstr(0), 
	_newLine(LineEnding::NEWLINE_DEFAULT),
	_lastChar(0)
{
	_it = _newLine.end();
}


LineEndingConverterStreamBuf::LineEndingConverterStreamBuf(std::ostream& ostr): 
	_pIstr(0), 
	_pOstr(&ostr), 
	_newLine(LineEnding::NEWLINE_DEFAULT),
	_lastChar(0)
{
	_it = _newLine.end();
}


LineEndingConverterStreamBuf::~LineEndingConverterStreamBuf()
{
}


void LineEndingConverterStreamBuf::setNewLine(const std::string& newLineCharacters)
{
	_newLine = newLineCharacters;
	_it      = _newLine.end();
}


const std::string& LineEndingConverterStreamBuf::getNewLine() const
{
	return _newLine;
}


int LineEndingConverterStreamBuf::readFromDevice()
{
	poco_assert_dbg (_pIstr);

	while (_it == _newLine.end())
	{
		int c = _pIstr->get();
		if (c == '\r')
		{
			if (_pIstr->peek() == '\n') _pIstr->get();
			_it = _newLine.begin();
		}
		else if (c == '\n')
		{
			_it = _newLine.begin();
		}
		else return c;
	}
	return *_it++;
}


int LineEndingConverterStreamBuf::writeToDevice(char c)
{
	poco_assert_dbg (_pOstr);

	if (c == '\r' || (c == '\n' && _lastChar != '\r'))
		_pOstr->write(_newLine.data(), (std::streamsize) _newLine.length());
	if (c != '\n' && c != '\r')
		_pOstr->put(c);
	_lastChar = c;
	return charToInt(c);
}


LineEndingConverterIOS::LineEndingConverterIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


LineEndingConverterIOS::LineEndingConverterIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


LineEndingConverterIOS::~LineEndingConverterIOS()
{
}


void LineEndingConverterIOS::setNewLine(const std::string& newLineCharacters)
{
	_buf.setNewLine(newLineCharacters);
}


const std::string& LineEndingConverterIOS::getNewLine() const
{
	return _buf.getNewLine();
}


LineEndingConverterStreamBuf* LineEndingConverterIOS::rdbuf()
{
	return &_buf;
}


InputLineEndingConverter::InputLineEndingConverter(std::istream& istr): 
	LineEndingConverterIOS(istr), 
	std::istream(&_buf)
{
}


InputLineEndingConverter::InputLineEndingConverter(std::istream& istr, const std::string& newLineCharacters): 
	LineEndingConverterIOS(istr), 
	std::istream(&_buf)
{
	setNewLine(newLineCharacters);
}


InputLineEndingConverter::~InputLineEndingConverter()
{
}


OutputLineEndingConverter::OutputLineEndingConverter(std::ostream& ostr): 
	LineEndingConverterIOS(ostr), 
	std::ostream(&_buf)
{
}


OutputLineEndingConverter::OutputLineEndingConverter(std::ostream& ostr, const std::string& newLineCharacters): 
	LineEndingConverterIOS(ostr), 
	std::ostream(&_buf)
{
	setNewLine(newLineCharacters);
}


OutputLineEndingConverter::~OutputLineEndingConverter()
{
}


} // namespace Poco
