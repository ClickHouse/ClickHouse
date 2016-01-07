//
// ZipArchiveInfo.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/ZipArchiveInfo.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipArchiveInfo
//
// Definition of the ZipArchiveInfo class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipArchiveInfo_INCLUDED
#define Zip_ZipArchiveInfo_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/Zip/ZipUtil.h"


namespace Poco {
namespace Zip {


class Zip_API ZipArchiveInfo
	/// A ZipArchiveInfo stores central directory info
{
public:
	static const char HEADER[ZipCommon::HEADER_SIZE];

	ZipArchiveInfo();
		/// Default constructor, everything set to zero or empty

	ZipArchiveInfo(std::istream& in, bool assumeHeaderRead);
		/// Creates the ZipArchiveInfo by parsing the input stream.
		/// If assumeHeaderRead is true we assume that the first 4 bytes were already read outside.

	~ZipArchiveInfo();
		/// Destroys the ZipArchiveInfo.

	Poco::UInt16 getDiskNumber() const;
		/// Get the number of the disk where this header can be found

	Poco::UInt16 getFirstDiskForDirectoryHeader() const;
		/// Returns the number of the disk that contains the start of the directory header

	Poco::UInt16 getNumberOfEntries() const;
		/// Returns the number of entries on this disk

	Poco::UInt16 getTotalNumberOfEntries() const;
		/// Returns the total number of entries on all disks

	Poco::UInt32 getCentralDirectorySize() const;
		/// Returns the size of the central directory in bytes

	std::streamoff getHeaderOffset() const;
		/// Returns the offset of the header in relation to the begin of this disk

	const std::string& getZipComment() const;
		/// Returns the (optional) Zip Comment

	void setZipComment(const std::string& comment);
		/// Sets the optional Zip comment.

	void setNumberOfEntries(Poco::UInt16 val);
		/// Returns the number of entries on this disk

	void setTotalNumberOfEntries(Poco::UInt16 val);
		/// Returns the total number of entries on all disks

	void setCentralDirectorySize(Poco::UInt32 val);
		/// Returns the size of the central directory in bytes

	void setHeaderOffset(Poco::UInt32 val);

	std::string createHeader() const;
		/// Creates a header

private:
	void parse(std::istream& inp, bool assumeHeaderRead);

	Poco::UInt16 getZipCommentSize() const;

private:
	enum
	{
		HEADER_POS = 0,
		NUMBEROFTHISDISK_POS = HEADER_POS + ZipCommon::HEADER_SIZE,
		NUMBEROFTHISDISK_SIZE = 2,
		NUMBEROFCENTRALDIRDISK_POS = NUMBEROFTHISDISK_POS + NUMBEROFTHISDISK_SIZE,
		NUMBEROFCENTRALDIRDISK_SIZE = 2,
		NUMENTRIESTHISDISK_POS = NUMBEROFCENTRALDIRDISK_POS + NUMBEROFCENTRALDIRDISK_SIZE,
		NUMENTRIESTHISDISK_SIZE = 2,
		TOTALNUMENTRIES_POS = NUMENTRIESTHISDISK_POS + NUMENTRIESTHISDISK_SIZE,
		TOTALNUMENTRIES_SIZE = 2,
		CENTRALDIRSIZE_POS = TOTALNUMENTRIES_POS + TOTALNUMENTRIES_SIZE,
		CENTRALDIRSIZE_SIZE = 4,
		CENTRALDIRSTARTOFFSET_POS = CENTRALDIRSIZE_POS + CENTRALDIRSIZE_SIZE,
		CENTRALDIRSTARTOFFSET_SIZE = 4,
		ZIPCOMMENT_LENGTH_POS = CENTRALDIRSTARTOFFSET_POS + CENTRALDIRSTARTOFFSET_SIZE,
		ZIPCOMMENT_LENGTH_SIZE = 2,
		FULLHEADER_SIZE = 22
	};

	char           _rawInfo[FULLHEADER_SIZE];
	std::streamoff _startPos;
	std::string    _comment;
};


inline Poco::UInt16 ZipArchiveInfo::getDiskNumber() const
{
	return ZipUtil::get16BitValue(_rawInfo, NUMBEROFTHISDISK_POS);
}


inline Poco::UInt16 ZipArchiveInfo::getFirstDiskForDirectoryHeader() const
{
	return ZipUtil::get16BitValue(_rawInfo, NUMBEROFCENTRALDIRDISK_POS);
}


inline Poco::UInt16 ZipArchiveInfo::getNumberOfEntries() const
{
	return ZipUtil::get16BitValue(_rawInfo, NUMENTRIESTHISDISK_POS);
}


inline Poco::UInt16 ZipArchiveInfo::getTotalNumberOfEntries() const
{
	return ZipUtil::get16BitValue(_rawInfo, TOTALNUMENTRIES_POS);
}


inline Poco::UInt32 ZipArchiveInfo::getCentralDirectorySize() const
{
	return ZipUtil::get32BitValue(_rawInfo, CENTRALDIRSIZE_POS);
}


inline std::streamoff ZipArchiveInfo::getHeaderOffset() const
{
	return _startPos;
}


inline Poco::UInt16 ZipArchiveInfo::getZipCommentSize() const
{
	return ZipUtil::get16BitValue(_rawInfo, ZIPCOMMENT_LENGTH_POS);
}


inline const std::string& ZipArchiveInfo::getZipComment() const
{
	return _comment;
}


inline void ZipArchiveInfo::setNumberOfEntries(Poco::UInt16 val)
{
	ZipUtil::set16BitValue(val, _rawInfo, NUMENTRIESTHISDISK_POS);
}


inline void ZipArchiveInfo::setTotalNumberOfEntries(Poco::UInt16 val)
{
	ZipUtil::set16BitValue(val, _rawInfo, TOTALNUMENTRIES_POS);
}


inline void ZipArchiveInfo::setCentralDirectorySize(Poco::UInt32 val)
{
	ZipUtil::set32BitValue(val, _rawInfo, CENTRALDIRSIZE_POS);
}


inline void ZipArchiveInfo::setHeaderOffset(Poco::UInt32 val)
{
	ZipUtil::set32BitValue(val, _rawInfo, CENTRALDIRSTARTOFFSET_POS);
}


} } // namespace Poco::Zip


#endif // Zip_ZipArchiveInfo_INCLUDED
