//
// ZipFileInfo.cpp
//
// $Id: //poco/1.4/Zip/src/ZipFileInfo.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipFileInfo
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipFileInfo.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Buffer.h"
#include <istream>
#include <cstring>


namespace Poco {
namespace Zip {


const char ZipFileInfo::HEADER[ZipCommon::HEADER_SIZE] = {'\x50', '\x4b', '\x01', '\x02'};


ZipFileInfo::ZipFileInfo(const ZipLocalFileHeader& header):
	_rawInfo(),
	_crc32(0),
	_compressedSize(0),
	_uncompressedSize(0),
	_fileName(),
	_lastModifiedAt(),
	_extraField()
{
	std::memset(_rawInfo, 0, FULLHEADER_SIZE);
	std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	setCompressedSize(header.getCompressedSize());
	setUncompressedSize(header.getUncompressedSize());
	setCRC(header.getCRC());
	setCompressionMethod(header.getCompressionMethod());
	setCompressionLevel(header.getCompressionLevel());
	setRequiredVersion(header.getMajorVersionNumber(), header.getMinorVersionNumber());
	setHostSystem(header.getHostSystem());
	setLastModifiedAt(header.lastModifiedAt());
	setEncryption(false);
	setFileName(header.getFileName());

	if (getHostSystem() == ZipCommon::HS_UNIX)
		setUnixAttributes();

	_rawInfo[GENERAL_PURPOSE_POS+1] |= 0x08; // Set "language encoding flag" to indicate that filenames and paths are in UTF-8.	   
}


ZipFileInfo::ZipFileInfo(std::istream& in, bool assumeHeaderRead):
	_rawInfo(),
	_crc32(0),
	_compressedSize(0),
	_uncompressedSize(0),
	_fileName(),
	_lastModifiedAt(),
	_extraField()
{
	// sanity check
	poco_assert_dbg (RELATIVEOFFSETLOCALHEADER_POS + RELATIVEOFFSETLOCALHEADER_SIZE == FULLHEADER_SIZE);
	parse(in, assumeHeaderRead);
}


ZipFileInfo::~ZipFileInfo()
{
}


void ZipFileInfo::parse(std::istream& inp, bool assumeHeaderRead)
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
	_crc32 = getCRCFromHeader();
	_compressedSize = getCompressedSizeFromHeader();
	_uncompressedSize = getUncompressedSizeFromHeader();
	parseDateTime();
	Poco::UInt16 len = getFileNameLength();
	Poco::Buffer<char> buf(len);
	inp.read(buf.begin(), len);
	_fileName = std::string(buf.begin(), len);
	if (hasExtraField())
	{
		len = getExtraFieldLength();
		Poco::Buffer<char> xtra(len);
		inp.read(xtra.begin(), len);
		_extraField = std::string(xtra.begin(), len);
	}
	len = getFileCommentLength();
	if (len > 0)
	{
		Poco::Buffer<char> buf2(len);
		inp.read(buf2.begin(), len);
		_fileComment = std::string(buf2.begin(), len);
	}
}


std::string ZipFileInfo::createHeader() const
{
	std::string result(_rawInfo, FULLHEADER_SIZE);
	result.append(_fileName);
	result.append(_extraField);
	result.append(_fileComment);
	return result;
}


void ZipFileInfo::setUnixAttributes()
{
	bool isDir = isDirectory();
	int mode;
	if (isDir)
		mode = DEFAULT_UNIX_DIR_MODE;
	else
		mode = DEFAULT_UNIX_FILE_MODE;
	Poco::UInt32 attrs = (mode << 16) | (isDir ? 0x10 : 0);
	setExternalFileAttributes(attrs);
}


} } // namespace Poco::Zip
