//
// HTTPIOStream.h
//
// $Id: //poco/Main/template/class.h#4 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPIOStream
//
// Definition of the HTTPIOStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPIOStream_INCLUDED
#define Net_HTTPIOStream_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/UnbufferedStreamBuf.h"


namespace Poco {
namespace Net {


class HTTPClientSession;


class Net_API HTTPResponseStreamBuf: public Poco::UnbufferedStreamBuf
{
public:
	HTTPResponseStreamBuf(std::istream& istr);
	
	~HTTPResponseStreamBuf();
		
private:
	int readFromDevice();
	
	std::istream& _istr;
};


inline int HTTPResponseStreamBuf::readFromDevice()
{
	return _istr.get();
}


class Net_API HTTPResponseIOS: public virtual std::ios
{
public:
	HTTPResponseIOS(std::istream& istr);
	
	~HTTPResponseIOS();
	
	HTTPResponseStreamBuf* rdbuf();

protected:
	HTTPResponseStreamBuf _buf;
};


inline HTTPResponseStreamBuf* HTTPResponseIOS::rdbuf()
{
	return &_buf;
}


class Net_API HTTPResponseStream: public HTTPResponseIOS, public std::istream
{
public:
	HTTPResponseStream(std::istream& istr, HTTPClientSession* pSession);
		
	~HTTPResponseStream();
	
private:
	HTTPClientSession* _pSession;
};


} } // namespace Poco::Net


#endif // Net_HTTPIOStream_INCLUDED
