//
// ZipUtil.h
//
// Library: Zip
// Package: Zip
// Module:  ZipUtil
//
// Definition of the ZipUtil class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipUtil_INCLUDED
#define Zip_ZipUtil_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/DateTime.h"
#include "Poco/Path.h"
#include <istream>


namespace Poco {
namespace Zip {


class Zip_API ZipUtil
	/// A utility class used for parsing header information inside of zip files
{
public:
	static Poco::UInt16 get16BitValue(const char* pVal, const Poco::UInt32 pos);

	static Poco::UInt32 get32BitValue(const char* pVal, const Poco::UInt32 pos);

	static Poco::UInt64 get64BitValue(const char* pVal, const Poco::UInt32 pos);
	
	static void set16BitValue(const Poco::UInt16 val, char* pVal, const Poco::UInt32 pos);

	static void set32BitValue(const Poco::UInt32 val, char* pVal, const Poco::UInt32 pos);

	static void set64BitValue(const Poco::UInt64 val, char* pVal, const Poco::UInt32 pos);

	static Poco::DateTime parseDateTime(const char* pVal, const Poco::UInt32 timePos, const Poco::UInt32 datePos);

	static void setDateTime(const Poco::DateTime& dt, char* pVal, const Poco::UInt32 timePos, const Poco::UInt32 datePos);

	static std::string fakeZLibInitString(ZipCommon::CompressionLevel cl);

	static void sync(std::istream& in);
		/// Searches the next valid header in the input stream, stops right before it

	static void syncDataDescriptor(std::istream& in, bool force64);
		/// Searches the next data descriptor

	static void verifyZipEntryFileName(const std::string& zipPath);
		/// Verifies that the name of the ZipEntry is a valid path

	static std::string validZipEntryFileName(const Poco::Path& entry);

private:
	ZipUtil();
	~ZipUtil();
	ZipUtil(const ZipUtil&);
	ZipUtil& operator=(const ZipUtil&);
};


inline Poco::UInt16 ZipUtil::get16BitValue(const char* pVal, const Poco::UInt32 pos)
{
	return static_cast<Poco::UInt16>((unsigned char)pVal[pos])+ (static_cast<Poco::UInt16>((unsigned char)pVal[pos+1]) << 8);
}


inline Poco::UInt32 ZipUtil::get32BitValue(const char* pVal, const Poco::UInt32 pos)
{
	return static_cast<Poco::UInt32>((unsigned char)pVal[pos])+ (static_cast<Poco::UInt32>((unsigned char)pVal[pos+1]) << 8)+
		(static_cast<Poco::UInt32>((unsigned char)pVal[pos+2]) << 16) + (static_cast<Poco::UInt32>((unsigned char)pVal[pos+3]) << 24);
}


inline Poco::UInt64 ZipUtil::get64BitValue(const char* pVal, const Poco::UInt32 pos)
{
	Poco::UInt64 val = ZipUtil::get32BitValue(pVal, pos+4);
	val = (val << 32) | ZipUtil::get32BitValue(pVal, pos);
	return val;
}


inline void ZipUtil::set16BitValue(const Poco::UInt16 val, char* pVal, const Poco::UInt32 pos)
{
	pVal[pos] = static_cast<char>(val);
	pVal[pos+1] = static_cast<char>(val>>8);
}


inline void ZipUtil::set32BitValue(const Poco::UInt32 val, char* pVal, const Poco::UInt32 pos)
{
	pVal[pos] = static_cast<char>(val);
	pVal[pos+1] = static_cast<char>(val>>8);
	pVal[pos+2] = static_cast<char>(val>>16);
	pVal[pos+3] = static_cast<char>(val>>24);
}


inline void ZipUtil::set64BitValue(const Poco::UInt64 val, char* pVal, const Poco::UInt32 pos)
{
	pVal[pos] = static_cast<char>(val);
	pVal[pos+1] = static_cast<char>(val>>8);
	pVal[pos+2] = static_cast<char>(val>>16);
	pVal[pos+3] = static_cast<char>(val>>24);
	pVal[pos+4] = static_cast<char>(val>>32);
	pVal[pos+5] = static_cast<char>(val>>40);
	pVal[pos+6] = static_cast<char>(val>>48);
	pVal[pos+7] = static_cast<char>(val>>56);
}


} } // namespace Poco::Zip


#endif // Zip_ZipUtil_INCLUDED
