//
// ZipArchiveInfo.cpp
//
// $Id: //poco/1.4/Zip/src/ZipArchiveInfo.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipArchiveInfo
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipArchiveInfo.h"
#include "Poco/Zip/ZipException.h"
#include "Poco/Buffer.h"
#include <istream>
#include <cstring>


namespace Poco {
namespace Zip {


const char ZipArchiveInfo::HEADER[ZipCommon::HEADER_SIZE] = {'\x50', '\x4b', '\x05', '\x06'};


ZipArchiveInfo::ZipArchiveInfo(std::istream& in, bool assumeHeaderRead):
	_rawInfo(),
	_startPos(in.tellg()),
	_comment()
{
	if (assumeHeaderRead)
		_startPos -= ZipCommon::HEADER_SIZE;
	parse(in, assumeHeaderRead);
}

ZipArchiveInfo::ZipArchiveInfo():
	_rawInfo(),
	_startPos(0),
	_comment()
{
	std::memset(_rawInfo, 0, FULLHEADER_SIZE);
	std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
}


ZipArchiveInfo::~ZipArchiveInfo()
{
}


void ZipArchiveInfo::parse(std::istream& inp, bool assumeHeaderRead)
{
	if (!assumeHeaderRead)
	{
		inp.read(_rawInfo, ZipCommon::HEADER_SIZE);
	}
	else
	{
		std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	}
	poco_assert (std::memcmp(_rawInfo, HEADER, ZipCommon::HEADER_SIZE) == 0);
	// read the rest of the header
	inp.read(_rawInfo + ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	Poco::UInt16 len = getZipCommentSize();
	if (len > 0)
	{
		Poco::Buffer<char> buf(len);
		inp.read(buf.begin(), len);
		_comment = std::string(buf.begin(), len);
	}
}


std::string ZipArchiveInfo::createHeader() const
{
	std::string result(_rawInfo, FULLHEADER_SIZE);
	result.append(_comment);
	return result;
}


void ZipArchiveInfo::setZipComment(const std::string& comment)
{
	// Confirm string is of valid size
	if (comment.size() > 65535)
		throw ZipException("Maximum number of entries for a ZIP file reached: 65535");

	// Change the value of the ZIP Comment Size to reflect new comment size
	ZipUtil::set16BitValue(static_cast<Poco::UInt16>(comment.size()), _rawInfo, ZIPCOMMENT_LENGTH_POS);

	// Now change our internal comment
	_comment = comment;
}


} } // namespace Poco::Zip
