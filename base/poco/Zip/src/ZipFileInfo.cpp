//
// ZipFileInfo.cpp
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
	_localHeaderOffset(0),
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

	if (header.searchCRCAndSizesAfterData())
		_rawInfo[GENERAL_PURPOSE_POS] |= 0x08;
}


ZipFileInfo::ZipFileInfo(std::istream& in, bool assumeHeaderRead):
	_rawInfo(),
	_crc32(0),
	_compressedSize(0),
	_uncompressedSize(0),
	_localHeaderOffset(0),
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
		if (inp.gcount() != ZipCommon::HEADER_SIZE)
			throw Poco::IOException("Failed to read file info header");
		if (std::memcmp(_rawInfo, HEADER, ZipCommon::HEADER_SIZE) != 0)
			throw Poco::DataFormatException("Bad file info header");
	}
	else
	{
		std::memcpy(_rawInfo, HEADER, ZipCommon::HEADER_SIZE);
	}

	// read the rest of the header
	inp.read(_rawInfo + ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
	_crc32 = getCRCFromHeader();
	_compressedSize = getCompressedSizeFromHeader();
	_uncompressedSize = getUncompressedSizeFromHeader();
	_localHeaderOffset = getOffsetFromHeader();
	parseDateTime();
	Poco::UInt16 len = getFileNameLength();
	if (len > 0)
	{
		Poco::Buffer<char> buf(len);
		inp.read(buf.begin(), len);
		_fileName = std::string(buf.begin(), len);
	}
	if (hasExtraField())
	{
		len = getExtraFieldLength();
		if (len > 0)
		{
			Poco::Buffer<char> xtra(len);
			inp.read(xtra.begin(), len);
			_extraField = std::string(xtra.begin(), len);
			char* ptr = xtra.begin();
			while (ptr <= xtra.begin() + len - 4)
			{
				Poco::UInt16 id = ZipUtil::get16BitValue(ptr, 0);
				ptr += 2;
				Poco::UInt16 size = ZipUtil::get16BitValue(ptr, 0);
				ptr += 2;
				if (id == ZipCommon::ZIP64_EXTRA_ID)
				{
					if (size >= 8 && getUncompressedSizeFromHeader() == ZipCommon::ZIP64_MAGIC)
					{
						setUncompressedSize(ZipUtil::get64BitValue(ptr, 0));
						size -= 8;
						ptr += 8;
					}
					if (size >= 8 && getCompressedSizeFromHeader() == ZipCommon::ZIP64_MAGIC)
					{
						setCompressedSize(ZipUtil::get64BitValue(ptr, 0));
						size -= 8;
						ptr += 8;
					}
					if (size >= 8 && getOffsetFromHeader() == ZipCommon::ZIP64_MAGIC)
					{
						setOffset(ZipUtil::get64BitValue(ptr, 0));
						size -= 8;
						ptr += 8;
					}
				}
				else
				{
					ptr += size;
				}
			}
		}
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
