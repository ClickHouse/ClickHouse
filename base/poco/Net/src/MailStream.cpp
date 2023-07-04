//
// MailStream.cpp
//
// Library: Net
// Package: Mail
// Module:  MailStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MailStream.h"


namespace Poco {
namespace Net {


MailStreamBuf::MailStreamBuf(std::istream& istr):
	_pIstr(&istr), 
	_pOstr(0),
	_state(ST_CR_LF)
{
}


MailStreamBuf::MailStreamBuf(std::ostream& ostr):
	_pIstr(0), 
	_pOstr(&ostr),
	_state(ST_CR_LF)
{
}


MailStreamBuf::~MailStreamBuf()
{
}


void MailStreamBuf::close()
{
	if (_pOstr && _state != ST_CR_LF_DOT_CR_LF)
	{
		if (!_buffer.empty())
			_pOstr->write(_buffer.data(), (std::streamsize) _buffer.length());
		if (_state != ST_CR_LF)
			_pOstr->write("\r\n", 2);
		_pOstr->write(".\r\n", 3);
		_state = ST_CR_LF_DOT_CR_LF;
	}
}

		
int MailStreamBuf::readFromDevice()
{
	int c = std::char_traits<char>::eof();
	if (!_buffer.empty())
	{
		c = _buffer[0];
		_buffer.erase(0, 1);
	}
	else
	{
		c = readOne();
		while (c != std::char_traits<char>::eof() && _state != ST_DATA && _state != ST_CR_LF_DOT_CR_LF)
			c = readOne();
		if (!_buffer.empty())
		{
			c = _buffer[0];
			_buffer.erase(0, 1);
		}
	}
	return c;
}


int MailStreamBuf::readOne()
{
	int c = std::char_traits<char>::eof();
	if (_state != ST_CR_LF_DOT_CR_LF)
	{
		c = _pIstr->get();
		switch (c)
		{
		case '\r':
			if (_state == ST_CR_LF_DOT)
				_state = ST_CR_LF_DOT_CR;
			else
				_state = ST_CR;
			break;
		case '\n':
			if (_state == ST_CR)
				_state = ST_CR_LF;
			else if (_state == ST_CR_LF_DOT_CR)
				_state = ST_CR_LF_DOT_CR_LF;
			else
				_state = ST_DATA;
			break;
		case '.':
			if (_state == ST_CR_LF)
				_state = ST_CR_LF_DOT;
			else if (_state == ST_CR_LF_DOT)
				_state = ST_CR_LF_DOT_DOT;
			else
				_state = ST_DATA;
			break;
		default:
			_state = ST_DATA;
		}
		if (_state == ST_CR_LF_DOT_DOT)
			_state = ST_DATA;
		else if (_state == ST_CR_LF_DOT_CR_LF)
			_buffer.resize(_buffer.size() - 2);
		else if (c != std::char_traits<char>::eof())
			_buffer += (char) c;
	}
	return c;
}


int MailStreamBuf::writeToDevice(char c)
{
	switch (c)
	{
	case '\r':
		_state = ST_CR;
		break;
	case '\n':
		if (_state == ST_CR)
			_state = ST_CR_LF;
		else
			_state = ST_DATA;
		break;
	case '.':
		if (_state == ST_CR_LF)
			_state = ST_CR_LF_DOT;
		else
			_state = ST_DATA;
		break;
	default:
		_state = ST_DATA;
	}
	if (_state == ST_DATA)
	{
		if (!_buffer.empty())
		{
			_pOstr->write(_buffer.data(), (std::streamsize) _buffer.length());
			_buffer.clear();
		}
		_pOstr->put(c);
	}
	else if (_state == ST_CR_LF_DOT)
	{
		// buffer contains one or more CR-LF pairs
		_pOstr->write(_buffer.data(), (std::streamsize) _buffer.length());
		_pOstr->write("..", 2);
		_state = ST_DATA;
		_buffer.clear();
	}
	else _buffer += c;
	return charToInt(c);
}


MailIOS::MailIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


MailIOS::MailIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


MailIOS::~MailIOS()
{
}


void MailIOS::close()
{
	_buf.close();
}


MailStreamBuf* MailIOS::rdbuf()
{
	return &_buf;
}


MailInputStream::MailInputStream(std::istream& istr): 
	MailIOS(istr), 
	std::istream(&_buf)
{
}


MailInputStream::~MailInputStream()
{
}


MailOutputStream::MailOutputStream(std::ostream& ostr): 
	MailIOS(ostr), 
	std::ostream(&_buf)
{
}


MailOutputStream::~MailOutputStream()
{
}


} } // namespace Poco::Net
