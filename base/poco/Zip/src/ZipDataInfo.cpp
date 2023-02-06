//
// ZipDataInfo.cpp
//
// Library: Zip
// Package: Zip
// Module:  ZipDataInfo
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Exception.h"
#include <istream>
#include <cstring>


namespace Poco {
namespace Zip {


const char ZipDataInfo::HEADER[ZipCommon::HEADER_SIZE] = {'\x50', '\x4b', '\x07', '\x08'};


ZipDataInfo::ZipDataInfo():
	_rawInfo(),
	_valid(true)
{
	std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	std::memset(_rawInfo+ZipCommon::HEADER_SIZE, 0, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_valid = true;
}


ZipDataInfo::ZipDataInfo(std::istream& in, bool assumeHeaderRead):
	_rawInfo(),
	_valid(false)
{
	if (assumeHeaderRead)
	{
		std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	}
	else
	{
		in.read(_rawInfo, ZipCommon::HEADER_SIZE);
		if (in.gcount() != ZipCommon::HEADER_SIZE)
			throw Poco::IOException("Failed to read data info header");
		if (std::memcmp(_rawInfo, HEADER, ZipCommon::HEADER_SIZE) != 0)
			throw Poco::DataFormatException("Bad data info header");
	}
	// now copy the rest of the header
	in.read(_rawInfo+ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_valid = (!in.eof() && in.good());
}


ZipDataInfo::~ZipDataInfo()
{
}


const char ZipDataInfo64::HEADER[ZipCommon::HEADER_SIZE] = {'\x50', '\x4b', '\x07', '\x08'};


ZipDataInfo64::ZipDataInfo64():
	_rawInfo(),
	_valid(true)
{
	std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	std::memset(_rawInfo+ZipCommon::HEADER_SIZE, 0, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_valid = true;
}


ZipDataInfo64::ZipDataInfo64(std::istream& in, bool assumeHeaderRead):
	_rawInfo(),
	_valid(false)
{
	if (assumeHeaderRead)
	{
		std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	}
	else
	{
		in.read(_rawInfo, ZipCommon::HEADER_SIZE);
		if (in.gcount() != ZipCommon::HEADER_SIZE)
			throw Poco::IOException("Failed to read data info header");
		if (std::memcmp(_rawInfo, HEADER, ZipCommon::HEADER_SIZE) != 0)
			throw Poco::DataFormatException("Bad data info header");
	}

	// now copy the rest of the header
	in.read(_rawInfo+ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_valid = (!in.eof() && in.good());
}


ZipDataInfo64::~ZipDataInfo64()
{
}


} } // namespace Poco::Zip
