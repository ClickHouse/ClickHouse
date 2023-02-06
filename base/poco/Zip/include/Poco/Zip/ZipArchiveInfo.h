//
// ZipArchiveInfo.h
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
		/// Sets the number of entries on this disk

	void setTotalNumberOfEntries(Poco::UInt16 val);
		/// Sets the total number of entries on all disks

	void setCentralDirectorySize(Poco::UInt32 val);
		/// Sets the size of the central directory in bytes

	void setCentralDirectoryOffset(Poco::UInt32 val);
		/// Sets the offset of the central directory from beginning of first disk

	void setHeaderOffset(std::streamoff val);
		/// Sets the offset of the header in relation to the begin of this disk

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


class Zip_API ZipArchiveInfo64
	/// A ZipArchiveInfo64 stores central directory info
{
public:
	static const char HEADER[ZipCommon::HEADER_SIZE];
	static const char LOCATOR_HEADER[ZipCommon::HEADER_SIZE];

	ZipArchiveInfo64();
		/// Default constructor, everything set to zero or empty

	ZipArchiveInfo64(std::istream& in, bool assumeHeaderRead);
		/// Creates the ZipArchiveInfo64 by parsing the input stream.
		/// If assumeHeaderRead is true we assume that the first 4 bytes were already read outside.

	~ZipArchiveInfo64();
		/// Destroys the ZipArchiveInfo64.

	void getVersionMadeBy(int& major, int& minor);	
		/// The ZIP version used to create the file

	void getRequiredVersion(int& major, int& minor);
		/// The minimum version required to extract the data

	Poco::UInt32 getDiskNumber() const;
		/// Get the number of the disk where this header can be found

	Poco::UInt32 getFirstDiskForDirectoryHeader() const;
		/// Returns the number of the disk that contains the start of the directory header

	Poco::UInt64 getNumberOfEntries() const;
		/// Returns the number of entries on this disk

	Poco::UInt64 getTotalNumberOfEntries() const;
		/// Returns the total number of entries on all disks

	Poco::UInt64 getCentralDirectorySize() const;
		/// Returns the size of the central directory in bytes

	std::streamoff getCentralDirectoryOffset() const;
		/// Returns the offset of the central directory from beginning of first disk
	
	std::streamoff getHeaderOffset() const;
		/// Returns the offset of the header in relation to the begin of this disk

	void setNumberOfEntries(Poco::UInt64 val);
		/// Sets the number of entries on this disk

	void setTotalNumberOfEntries(Poco::UInt64 val);
		/// Sets the total number of entries on all disks

	void setCentralDirectorySize(Poco::UInt64 val);
		/// Set the size of the central directory in bytes

	void setCentralDirectoryOffset(Poco::UInt64 val);
		/// Returns the offset of the central directory from beginning of first disk

	void setHeaderOffset(std::streamoff val);
		/// Sets the offset of the header in relation to the begin of this disk
	
	void setTotalNumberOfDisks(Poco::UInt32 val);
		/// Sets the offset of the central directory from beginning of first disk

	std::string createHeader() const;
		/// Creates a header

private:
	void parse(std::istream& inp, bool assumeHeaderRead);
	void setRequiredVersion(int major, int minor);

private:
	enum
	{
		HEADER_POS = 0,
		RECORDSIZE_POS = HEADER_POS + ZipCommon::HEADER_SIZE,
		RECORDSIZE_SIZE = 8,
		VERSIONMADEBY_POS = RECORDSIZE_POS + RECORDSIZE_SIZE,
		VERSIONMADEBY_SIZE = 2,
		VERSION_NEEDED_POS = VERSIONMADEBY_POS + VERSIONMADEBY_SIZE,
		VERSION_NEEDED_SIZE = 2,
		NUMBEROFTHISDISK_POS = VERSION_NEEDED_POS + VERSION_NEEDED_SIZE,
		NUMBEROFTHISDISK_SIZE = 4,
		NUMBEROFCENTRALDIRDISK_POS = NUMBEROFTHISDISK_POS + NUMBEROFTHISDISK_SIZE,
		NUMBEROFCENTRALDIRDISK_SIZE = 4,
		NUMENTRIESTHISDISK_POS = NUMBEROFCENTRALDIRDISK_POS + NUMBEROFCENTRALDIRDISK_SIZE,
		NUMENTRIESTHISDISK_SIZE = 8,
		TOTALNUMENTRIES_POS = NUMENTRIESTHISDISK_POS + NUMENTRIESTHISDISK_SIZE,
		TOTALNUMENTRIES_SIZE = 8,
		CENTRALDIRSIZE_POS = TOTALNUMENTRIES_POS + TOTALNUMENTRIES_SIZE,
		CENTRALDIRSIZE_SIZE = 8,
		CENTRALDIRSTARTOFFSET_POS = CENTRALDIRSIZE_POS + CENTRALDIRSIZE_SIZE,
		CENTRALDIRSTARTOFFSET_SIZE = 8,
		FULL_HEADER_SIZE = 56,

		LOCATOR_HEADER_POS = 0,
		NUMBEROFENDOFCENTRALDIRDISK_POS = LOCATOR_HEADER_POS + ZipCommon::HEADER_SIZE,
		NUMBEROFENDOFCENTRALDIRDISK_SIZE = 4,
		ENDOFCENTRALDIROFFSET_POS = NUMBEROFENDOFCENTRALDIRDISK_POS + NUMBEROFENDOFCENTRALDIRDISK_SIZE,
		ENDOFCENTRALDIROFFSET_SIZE = 8,
		TOTALNUMBEROFENDDISKS_POS = ENDOFCENTRALDIROFFSET_POS + ENDOFCENTRALDIROFFSET_SIZE,
		TOTALNUMBEROFENDDISKS_SIZE = 4,
			
		FULL_LOCATOR_SIZE = 20
	};

	char           _rawInfo[FULL_HEADER_SIZE];
	std::string     _extraField;
	char            _locInfo[FULL_LOCATOR_SIZE];
	std::streamoff _startPos;
};


//
// inlines
//


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


inline void ZipArchiveInfo::setCentralDirectoryOffset(Poco::UInt32 val)
{
	ZipUtil::set32BitValue(val, _rawInfo, CENTRALDIRSTARTOFFSET_POS);
}

inline void ZipArchiveInfo::setHeaderOffset(std::streamoff val)
{
	_startPos = val;
}


inline Poco::UInt32 ZipArchiveInfo64::getDiskNumber() const
{
	return ZipUtil::get32BitValue(_rawInfo, NUMBEROFTHISDISK_POS);
}


inline Poco::UInt32 ZipArchiveInfo64::getFirstDiskForDirectoryHeader() const
{
	return ZipUtil::get32BitValue(_rawInfo, NUMBEROFCENTRALDIRDISK_POS);
}


inline Poco::UInt64 ZipArchiveInfo64::getNumberOfEntries() const
{
	return ZipUtil::get64BitValue(_rawInfo, NUMENTRIESTHISDISK_POS);
}


inline Poco::UInt64 ZipArchiveInfo64::getTotalNumberOfEntries() const
{
	return ZipUtil::get64BitValue(_rawInfo, TOTALNUMENTRIES_POS);
}


inline Poco::UInt64 ZipArchiveInfo64::getCentralDirectorySize() const
{
	return ZipUtil::get64BitValue(_rawInfo, CENTRALDIRSIZE_POS);
}


inline std::streamoff ZipArchiveInfo64::getCentralDirectoryOffset() const
{
	return _startPos;
}


inline std::streamoff ZipArchiveInfo64::getHeaderOffset() const
{
	return _startPos;
}


inline void ZipArchiveInfo64::setRequiredVersion(int major, int minor)
{
	poco_assert (minor < 10);
	poco_assert (major < 24);
	Poco::UInt8 val = static_cast<unsigned char>(major)*10+static_cast<unsigned char>(minor);
	_rawInfo[VERSIONMADEBY_POS] = static_cast<char>(val);
	_rawInfo[VERSION_NEEDED_POS] = static_cast<char>(val);
}


inline void ZipArchiveInfo64::setNumberOfEntries(Poco::UInt64 val)
{
	ZipUtil::set64BitValue(val, _rawInfo, NUMENTRIESTHISDISK_POS);
}


inline void ZipArchiveInfo64::setTotalNumberOfEntries(Poco::UInt64 val)
{
	ZipUtil::set64BitValue(val, _rawInfo, TOTALNUMENTRIES_POS);
}


inline void ZipArchiveInfo64::setCentralDirectorySize(Poco::UInt64 val)
{
	ZipUtil::set64BitValue(val, _rawInfo, CENTRALDIRSIZE_POS);
}


inline void ZipArchiveInfo64::setCentralDirectoryOffset(Poco::UInt64 val)
{
	ZipUtil::set64BitValue(val, _rawInfo, CENTRALDIRSTARTOFFSET_POS);
}


inline void ZipArchiveInfo64::setHeaderOffset(std::streamoff val)
{
	_startPos = val;
	ZipUtil::set64BitValue(val, _locInfo, ENDOFCENTRALDIROFFSET_POS);
}


inline void ZipArchiveInfo64::setTotalNumberOfDisks(Poco::UInt32 val)
{
	ZipUtil::set32BitValue(val, _locInfo, TOTALNUMBEROFENDDISKS_POS);
}


} } // namespace Poco::Zip


#endif // Zip_ZipArchiveInfo_INCLUDED
