//
// ZipDataInfo.cpp
//
// $Id: //poco/1.4/Zip/src/ZipDataInfo.cpp#1 $
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
		std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	else
		in.read(_rawInfo, ZipCommon::HEADER_SIZE);
	poco_assert (std::memcmp(_rawInfo, HEADER, ZipCommon::HEADER_SIZE) == 0);
	// now copy the rest of the header
	in.read(_rawInfo+ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_valid = (!in.eof() && in.good());
}


ZipDataInfo::~ZipDataInfo()
{
}


} } // namespace Poco::Zip
