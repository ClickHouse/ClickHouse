//
// MultipartWriter.cpp
//
// $Id: //poco/1.4/Net/src/MultipartWriter.cpp#1 $
//
// Library: Net
// Package: Messages
// Module:  MultipartWriter
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/MultipartWriter.h"
#include "Poco/Net/MessageHeader.h"
#include "Poco/Random.h"
#include "Poco/NumberFormatter.h"


using Poco::Random;
using Poco::NumberFormatter;


namespace Poco {
namespace Net {


MultipartWriter::MultipartWriter(std::ostream& ostr):
	_ostr(ostr),
	_boundary(createBoundary()),
	_firstPart(true)
{
}


MultipartWriter::MultipartWriter(std::ostream& ostr, const std::string& boundary):
	_ostr(ostr),
	_boundary(boundary),
	_firstPart(true)
{
	if (_boundary.empty())
		_boundary = createBoundary();
}


MultipartWriter::~MultipartWriter()
{
}

	
void MultipartWriter::nextPart(const MessageHeader& header)
{
	if (_firstPart)
		_firstPart = false;
	else
		_ostr << "\r\n";
	_ostr << "--" << _boundary << "\r\n";
	header.write(_ostr);
	_ostr << "\r\n";
}

	
void MultipartWriter::close()
{
	_ostr << "\r\n--" << _boundary << "--\r\n";
}


const std::string& MultipartWriter::boundary() const
{
	return _boundary;
}


std::string MultipartWriter::createBoundary()
{
	std::string boundary("MIME_boundary_");
	Random rnd;
	NumberFormatter::appendHex(boundary, rnd.next(), 8);
	NumberFormatter::appendHex(boundary, rnd.next(), 8);
	return boundary;
}


} } // namespace Poco::Net
