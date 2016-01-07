//
// ZipUtil.cpp
//
// $Id: //poco/1.4/Zip/src/ZipUtil.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipUtil
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipUtil.h"
#include "Poco/Zip/ZipException.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipFileInfo.h"
#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Zip/ZipArchiveInfo.h"
#include <cstring>


namespace Poco {
namespace Zip {


Poco::DateTime ZipUtil::parseDateTime(const char* pVal, const Poco::UInt32 timePos, const Poco::UInt32 datePos)
{
	Poco::UInt16 time = ZipUtil::get16BitValue(pVal, timePos);
	Poco::UInt16 date = ZipUtil::get16BitValue(pVal, datePos);
	//TIME: second 0-4, minute 5-10, hour 11-15, second resolution is 2!
	int sec = 2*(time & 0x001fu);         // 0000 0000 0001 1111
	int min = ((time & 0x07e0u) >> 5);  // 0000 0111 1110 0000
	int hour= ((time & 0xf800u) >> 11); // 1111 1000 0000 0000

	//DATE: day 0-4, month 5-8, year (starting with 1980): 9-16
	int day = (date & 0x001fu);        // 0000 0000 0001 1111
	int mon = ((date & 0x01e0u) >> 5); // 0000 0001 1110 0000
	int year= 1980+((date & 0xfe00u) >> 9); // 1111 1110 0000 0000
	return Poco::DateTime(year, mon, day, hour, min, sec);
}


void ZipUtil::setDateTime(const Poco::DateTime& dt, char* pVal, const Poco::UInt32 timePos, const Poco::UInt32 datePos)
{
	//TIME: second 0-4, minute 5-10, hour 11-15
	Poco::UInt16 time = static_cast<Poco::UInt16>((dt.second()/2) + (dt.minute()<<5) + (dt.hour()<<11));
	//DATE: day 0-4, month 5-8, year (starting with 1980): 9-16
	int year = dt.year() - 1980;
	if (year<0)
		year = 0;
	Poco::UInt16 date = static_cast<Poco::UInt16>(dt.day() + (dt.month()<<5) + (year<<9));
	ZipUtil::set16BitValue(time, pVal, timePos);
	ZipUtil::set16BitValue(date, pVal, datePos);

}


std::string ZipUtil::fakeZLibInitString(ZipCommon::CompressionLevel cl)
{
	std::string init(2, ' ');

	// compression info:
	// deflate is used, bit 0-3: 0x08
	// dictionary size is always 32k: calc ld2(32k)-8 = ld2(2^15) - 8 = 15 - 8 = 7 --> bit 4-7: 0x70
	init[0] = '\x78';

	// now fake flags
	// bits 0-4 check bits: set them so that init[0]*256+init[1] % 31 == 0
	// bit 5: preset dictionary? always no for us, set to 0
	// bits 6-7: compression level: 00 very fast, 01 fast, 10 normal, 11 best
	if (cl == ZipCommon::CL_SUPERFAST)
		init[1] = '\x00';
	else if (cl == ZipCommon::CL_FAST)
		init[1] = '\x40';
	else if (cl == ZipCommon::CL_NORMAL)
		init[1] = '\x80';
	else
		init[1] = '\xc0';
	// now set the last 5 bits
	Poco::UInt16 tmpVal = ((Poco::UInt16)init[0])*256+((unsigned char)init[1]);
	char checkBits = (31 - (char)(tmpVal%31));
	init[1] |= checkBits; // set the lower 5 bits
	tmpVal = ((Poco::UInt16)init[0])*256+((unsigned char)init[1]);
	poco_assert_dbg ((tmpVal % 31) == 0);
	return init;
}


void ZipUtil::sync(std::istream& in)
{
	enum
	{
		PREFIX = 2, 
		BUFFER_SIZE = 1024
	};
	char temp[BUFFER_SIZE];
	in.read(temp, PREFIX);
	std::size_t tempPos = PREFIX;

	while (in.good() && !in.eof())
	{ 
		// all zip headers start withe same 2byte prefix
		if(std::memcmp(ZipLocalFileHeader::HEADER, &temp[tempPos - PREFIX], PREFIX) == 0)
		{
			// we have a possible header!
			// read the next 2 bytes
			in.read(temp+tempPos, PREFIX);
			tempPos += PREFIX;
			if (std::memcmp(ZipLocalFileHeader::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0 || 
				std::memcmp(ZipArchiveInfo::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0 || 
				std::memcmp(ZipFileInfo::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0 ||
				std::memcmp(ZipDataInfo::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0)
			{
				if (std::memcmp(ZipLocalFileHeader::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0)
				{
					in.putback(ZipLocalFileHeader::HEADER[3]);
					in.putback(ZipLocalFileHeader::HEADER[2]);
					in.putback(ZipLocalFileHeader::HEADER[1]);
					in.putback(ZipLocalFileHeader::HEADER[0]);
				}
				else if (std::memcmp(ZipArchiveInfo::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0)
				{
					in.putback(ZipArchiveInfo::HEADER[3]);
					in.putback(ZipArchiveInfo::HEADER[2]);
					in.putback(ZipArchiveInfo::HEADER[1]);
					in.putback(ZipArchiveInfo::HEADER[0]);
				}
				else if (std::memcmp(ZipFileInfo::HEADER+PREFIX, &temp[tempPos - PREFIX], PREFIX) == 0)
				{
					in.putback(ZipFileInfo::HEADER[3]);
					in.putback(ZipFileInfo::HEADER[2]);
					in.putback(ZipFileInfo::HEADER[1]);
					in.putback(ZipFileInfo::HEADER[0]);
				}
				else
				{
					in.putback(ZipDataInfo::HEADER[3]);
					in.putback(ZipDataInfo::HEADER[2]);
					in.putback(ZipDataInfo::HEADER[1]);
					in.putback(ZipDataInfo::HEADER[0]);
				}
				return;
			}
			else
			{
				// we have read 2 bytes, should only be one: putback the last char
				in.putback(temp[tempPos - 1]);
				--tempPos;
			}
		}
		else
		{
			// read one byte
			in.read(temp + tempPos, 1);
			++tempPos;
		}

		if (tempPos > (BUFFER_SIZE - ZipCommon::HEADER_SIZE))
		{
			std::memcpy(temp, &temp[tempPos - ZipCommon::HEADER_SIZE], ZipCommon::HEADER_SIZE);
			tempPos = ZipCommon::HEADER_SIZE;
		}
	}
}


void ZipUtil::verifyZipEntryFileName(const std::string& fn)
{
	if (fn.find("\\") != std::string::npos)
		throw ZipException("Illegal entry name " + fn + " containing \\");
	if (fn == "/")
		throw ZipException("Illegal entry name /");
	if (fn.empty())
		throw ZipException("Illegal empty entry name");
	if (!ZipCommon::isValidPath(fn))
		throw ZipException("Illegal entry name " + fn + " containing parent directory reference");
}


std::string ZipUtil::validZipEntryFileName(const Poco::Path& entry)
{
	std::string fn = entry.toString(Poco::Path::PATH_UNIX);
	verifyZipEntryFileName(fn);
	return fn;
}


} } // namespace Poco::Zip
