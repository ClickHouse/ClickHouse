//
// TeeStream.cpp
//
// $Id: //poco/1.4/Foundation/src/TeeStream.cpp#1 $
//
// Library: Foundation
// Package: Streams
// Module:  TeeStream
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/TeeStream.h"


namespace Poco {


TeeStreamBuf::TeeStreamBuf(): 
	_pIstr(0)
{
}


TeeStreamBuf::TeeStreamBuf(std::istream& istr): 
	_pIstr(&istr)
{
}


TeeStreamBuf::TeeStreamBuf(std::ostream& ostr): 
	_pIstr(0)
{
	_streams.push_back(&ostr);
}


TeeStreamBuf::~TeeStreamBuf()
{
}


void TeeStreamBuf::addStream(std::ostream& ostr)
{
	_streams.push_back(&ostr);
}


int TeeStreamBuf::readFromDevice()
{
	if (_pIstr)
	{
		int c = _pIstr->get();
		if (c != -1) writeToDevice((char) c);
		return c;
	}
	return -1;
}


int TeeStreamBuf::writeToDevice(char c)
{
	for (StreamVec::iterator it = _streams.begin(); it != _streams.end(); ++it)
	{
		(*it)->put(c);
	}
	return charToInt(c);
}


TeeIOS::TeeIOS()
{
	poco_ios_init(&_buf);
}


TeeIOS::TeeIOS(std::istream& istr): _buf(istr)
{
	poco_ios_init(&_buf);
}


TeeIOS::TeeIOS(std::ostream& ostr): _buf(ostr)
{
	poco_ios_init(&_buf);
}


TeeIOS::~TeeIOS()
{
}


void TeeIOS::addStream(std::ostream& ostr)
{
	_buf.addStream(ostr);
}


TeeStreamBuf* TeeIOS::rdbuf()
{
	return &_buf;
}


TeeInputStream::TeeInputStream(std::istream& istr): TeeIOS(istr), std::istream(&_buf)
{
}


TeeInputStream::~TeeInputStream()
{
}


TeeOutputStream::TeeOutputStream(): std::ostream(&_buf)
{
}


TeeOutputStream::TeeOutputStream(std::ostream& ostr): TeeIOS(ostr), std::ostream(&_buf)
{
}


TeeOutputStream::~TeeOutputStream()
{
}


} // namespace Poco
