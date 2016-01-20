//
// ZipDataInfo.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/ZipDataInfo.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipDataInfo
//
// Definition of the ZipDataInfo class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipDataInfo_INCLUDED
#define Zip_ZipDataInfo_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/Zip/ZipUtil.h"


namespace Poco {
namespace Zip {


class Zip_API ZipDataInfo
	/// A ZipDataInfo stores a Zip data descriptor
{
public:
	static const char HEADER[ZipCommon::HEADER_SIZE];

	ZipDataInfo();
	/// Creates a header with all fields (except the header field) set to 0

	ZipDataInfo(std::istream& in, bool assumeHeaderRead);
		/// Creates the ZipDataInfo.

	~ZipDataInfo();
		/// Destroys the ZipDataInfo.

	bool isValid() const;

	Poco::UInt32 getCRC32() const;

	void setCRC32(Poco::UInt32 crc);

	Poco::UInt32 getCompressedSize() const;

	void setCompressedSize(Poco::UInt32 size);

	Poco::UInt32 getUncompressedSize() const;

	void setUncompressedSize(Poco::UInt32 size);

	static Poco::UInt32 getFullHeaderSize();

	const char* getRawHeader() const;

private:
	enum
	{
		HEADER_POS = 0,
		CRC32_POS  = HEADER_POS + ZipCommon::HEADER_SIZE,
		CRC32_SIZE = 4,
		COMPRESSED_POS = CRC32_POS + CRC32_SIZE,
		COMPRESSED_SIZE = 4,
		UNCOMPRESSED_POS = COMPRESSED_POS + COMPRESSED_SIZE,
		UNCOMPRESSED_SIZE = 4,
		FULLHEADER_SIZE = UNCOMPRESSED_POS + UNCOMPRESSED_SIZE
	};

	char _rawInfo[FULLHEADER_SIZE];
	bool _valid;
};


inline const char* ZipDataInfo::getRawHeader() const
{
	return _rawInfo;
}


inline bool ZipDataInfo::isValid() const
{
	return _valid;
}


inline Poco::UInt32 ZipDataInfo::getCRC32() const
{
	return ZipUtil::get32BitValue(_rawInfo, CRC32_POS);
}


inline void ZipDataInfo::setCRC32(Poco::UInt32 crc)
{
	return ZipUtil::set32BitValue(crc, _rawInfo, CRC32_POS);
}


inline Poco::UInt32 ZipDataInfo::getCompressedSize() const
{
	return ZipUtil::get32BitValue(_rawInfo, COMPRESSED_POS);
}


inline void ZipDataInfo::setCompressedSize(Poco::UInt32 size)
{
	return ZipUtil::set32BitValue(size, _rawInfo, COMPRESSED_POS);
}


inline Poco::UInt32 ZipDataInfo::getUncompressedSize() const
{
	return ZipUtil::get32BitValue(_rawInfo, UNCOMPRESSED_POS);
}


inline void ZipDataInfo::setUncompressedSize(Poco::UInt32 size)
{
	return ZipUtil::set32BitValue(size, _rawInfo, UNCOMPRESSED_POS);
}


inline Poco::UInt32 ZipDataInfo::getFullHeaderSize()
{
	return FULLHEADER_SIZE;
}


} } // namespace Poco::Zip


#endif // Zip_ZipDataInfo_INCLUDED
