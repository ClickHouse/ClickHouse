//
// ZipFileInfo.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/ZipFileInfo.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipFileInfo
//
// Definition of the ZipFileInfo class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipFileInfo_INCLUDED
#define Zip_ZipFileInfo_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/Zip/ZipUtil.h"


namespace Poco {
namespace Zip {


class ZipLocalFileHeader;


class Zip_API ZipFileInfo
	/// Stores a Zip directory entry of a file
{
public:
	static const char HEADER[ZipCommon::HEADER_SIZE];

	ZipFileInfo(const ZipLocalFileHeader& header);
		/// Creates a ZipFileInfo from a ZipLocalFileHeader

	ZipFileInfo(std::istream& in, bool assumeHeaderRead);
		/// Creates the ZipFileInfo by parsing the input stream.
		/// If assumeHeaderRead is true we assume that the first 4 bytes were already read outside.

	~ZipFileInfo();
		/// Destroys the ZipFileInfo.

	Poco::UInt32 getRelativeOffsetOfLocalHeader() const;
		/// Where on the disk starts the localheader. Combined with the disk number gives the exact location of the header

	ZipCommon::CompressionMethod getCompressionMethod() const;

	bool isEncrypted() const;

	const Poco::DateTime& lastModifiedAt() const;

	Poco::UInt32 getCRC() const;

	Poco::UInt32 getHeaderSize() const;
		/// Returns the total size of the header including filename + other additional fields

	Poco::UInt32 getCompressedSize() const;

	Poco::UInt32 getUncompressedSize() const;

	const std::string& getFileName() const;

	bool isFile() const;

	bool isDirectory() const;

	bool hasExtraField() const;

	const std::string& getExtraField() const;

	const std::string& getFileComment() const;

	void getVersionMadeBy(int& major, int& minor);
		/// The ZIP version used to create the file

	void getRequiredVersion(int& major, int& minor);
		/// The minimum version required to extract the data

	ZipCommon::HostSystem getHostSystem() const;

	Poco::UInt16 getDiskNumberStart() const;
		/// The number of the disk on which this file begins (multidisk archives)

	ZipCommon::FileType getFileType() const;
		/// Binary or ASCII file?

	std::string createHeader() const;

	void setOffset(Poco::UInt32 val);

private:
	void setCRC(Poco::UInt32 val);

	void setCompressedSize(Poco::UInt32 val);

	void setUncompressedSize(Poco::UInt32 val);

	void setCompressionMethod(ZipCommon::CompressionMethod cm);

	void setCompressionLevel(ZipCommon::CompressionLevel cl);

	void setRequiredVersion(int major, int minor);

	void setHostSystem(ZipCommon::HostSystem hs);

	void setLastModifiedAt(const Poco::DateTime& dt);

	void setEncryption(bool val);

	void setFileNameLength(Poco::UInt16 size);

	void setFileName(const std::string& str);
	
	void setExternalFileAttributes(Poco::UInt32 attrs);

	void parse(std::istream& in, bool assumeHeaderRead);

	void parseDateTime();

	Poco::UInt32 getCRCFromHeader() const;

	Poco::UInt32 getCompressedSizeFromHeader() const;

	Poco::UInt32 getUncompressedSizeFromHeader() const;

	Poco::UInt16 getFileNameLength() const;

	Poco::UInt16 getExtraFieldLength() const;

	Poco::UInt16 getFileCommentLength() const;

	Poco::UInt32 getExternalFileAttributes() const;
	
	void setUnixAttributes();

private:
	enum
	{
		HEADER_POS = 0,
		VERSIONMADEBY_POS = HEADER_POS + ZipCommon::HEADER_SIZE,
		VERSIONMADEBY_SIZE = 2,
		VERSION_NEEDED_POS = VERSIONMADEBY_POS + VERSIONMADEBY_SIZE,
		VERSION_NEEDED_SIZE = 2,
		GENERAL_PURPOSE_POS = VERSION_NEEDED_POS + VERSION_NEEDED_SIZE,
		GENERAL_PURPOSE_SIZE = 2,
		COMPR_METHOD_POS = GENERAL_PURPOSE_POS + GENERAL_PURPOSE_SIZE,
		COMPR_METHOD_SIZE = 2,
		LASTMODFILETIME_POS = COMPR_METHOD_POS + COMPR_METHOD_SIZE,
		LASTMODFILETIME_SIZE = 2,
		LASTMODFILEDATE_POS = LASTMODFILETIME_POS + LASTMODFILETIME_SIZE,
		LASTMODFILEDATE_SIZE = 2,
		CRC32_POS = LASTMODFILEDATE_POS + LASTMODFILEDATE_SIZE,
		CRC32_SIZE = 4,
		COMPRESSED_SIZE_POS = CRC32_POS + CRC32_SIZE,
		COMPRESSED_SIZE_SIZE = 4,
		UNCOMPRESSED_SIZE_POS = COMPRESSED_SIZE_POS + COMPRESSED_SIZE_SIZE,
		UNCOMPRESSED_SIZE_SIZE = 4,
		FILENAME_LENGTH_POS = UNCOMPRESSED_SIZE_POS + UNCOMPRESSED_SIZE_SIZE,
		FILENAME_LENGTH_SIZE = 2,
		EXTRAFIELD_LENGTH_POS = FILENAME_LENGTH_POS + FILENAME_LENGTH_SIZE,
		EXTRAFIELD_LENGTH_SIZE = 2,
		FILECOMMENT_LENGTH_POS = EXTRAFIELD_LENGTH_POS + EXTRAFIELD_LENGTH_SIZE,
		FILECOMMENT_LENGTH_SIZE = 2,
		DISKNUMBERSTART_POS = FILECOMMENT_LENGTH_POS + FILECOMMENT_LENGTH_SIZE,
		DISKNUMBERSTART_SIZE = 2,
		INTERNALFILE_ATTR_POS = DISKNUMBERSTART_POS + DISKNUMBERSTART_SIZE,
		INTERNALFILE_ATTR_SIZE = 2,
		EXTERNALFILE_ATTR_POS = INTERNALFILE_ATTR_POS + INTERNALFILE_ATTR_SIZE,
		EXTERNALFILE_ATTR_SIZE = 4,
		RELATIVEOFFSETLOCALHEADER_POS = EXTERNALFILE_ATTR_POS + EXTERNALFILE_ATTR_SIZE,
		RELATIVEOFFSETLOCALHEADER_SIZE = 4,
		FULLHEADER_SIZE = 46
	};
	
	enum 
	{
		DEFAULT_UNIX_FILE_MODE = 0640,
		DEFAULT_UNIX_DIR_MODE  = 0755
	};

	char           _rawInfo[FULLHEADER_SIZE];
	Poco::UInt32   _crc32;
	Poco::UInt32   _compressedSize;
	Poco::UInt32   _uncompressedSize;
	std::string    _fileName;
	Poco::DateTime _lastModifiedAt;
	std::string    _extraField;
	std::string    _fileComment;
};


inline Poco::UInt32 ZipFileInfo::getRelativeOffsetOfLocalHeader() const
{
	return ZipUtil::get32BitValue(_rawInfo, RELATIVEOFFSETLOCALHEADER_POS);
}


inline Poco::UInt32 ZipFileInfo::getCRCFromHeader() const
{
	return ZipUtil::get32BitValue(_rawInfo, CRC32_POS);
}


inline Poco::UInt32 ZipFileInfo::getCompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawInfo, COMPRESSED_SIZE_POS);
}


inline Poco::UInt32 ZipFileInfo::getUncompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawInfo, UNCOMPRESSED_SIZE_POS);
}


inline void ZipFileInfo::parseDateTime()
{
	_lastModifiedAt = ZipUtil::parseDateTime(_rawInfo, LASTMODFILETIME_POS, LASTMODFILEDATE_POS);
}


inline ZipCommon::CompressionMethod ZipFileInfo::getCompressionMethod() const
{
	return static_cast<ZipCommon::CompressionMethod>(ZipUtil::get16BitValue(_rawInfo, COMPR_METHOD_POS));
}


inline bool ZipFileInfo::isEncrypted() const
{
	// bit 0 indicates encryption
	return ((ZipUtil::get16BitValue(_rawInfo, GENERAL_PURPOSE_POS) & 0x0001) != 0);
}


inline const Poco::DateTime& ZipFileInfo::lastModifiedAt() const
{
	return _lastModifiedAt;
}


inline Poco::UInt32 ZipFileInfo::getCRC() const
{
	return _crc32;
}


inline Poco::UInt32 ZipFileInfo::getCompressedSize() const
{
	return _compressedSize;
}


inline Poco::UInt32 ZipFileInfo::getUncompressedSize() const
{
	return _uncompressedSize;
}


inline const std::string& ZipFileInfo::getFileName() const
{
	return _fileName;
}


inline bool ZipFileInfo::isFile() const
{
	return !isDirectory();
}


inline bool ZipFileInfo::isDirectory() const
{
	poco_assert_dbg(!_fileName.empty());
	return getUncompressedSize() == 0 && getCompressionMethod() == ZipCommon::CM_STORE && _fileName[_fileName.length()-1] == '/';
}


inline Poco::UInt16 ZipFileInfo::getFileNameLength() const
{
	return ZipUtil::get16BitValue(_rawInfo, FILENAME_LENGTH_POS);
}


inline Poco::UInt16 ZipFileInfo::getExtraFieldLength() const
{
	return ZipUtil::get16BitValue(_rawInfo, EXTRAFIELD_LENGTH_POS);
}


inline bool ZipFileInfo::hasExtraField() const
{
	return getExtraFieldLength() > 0;
}


inline const std::string& ZipFileInfo::getExtraField() const
{
	return _extraField;
}


inline const std::string& ZipFileInfo::getFileComment() const
{
	return _fileComment;
}


inline Poco::UInt16 ZipFileInfo::getFileCommentLength() const
{
	return ZipUtil::get16BitValue(_rawInfo, FILECOMMENT_LENGTH_POS);
}


inline void ZipFileInfo::getVersionMadeBy(int& major, int& minor)
{
	major = (_rawInfo[VERSIONMADEBY_POS]/10);
	minor = (_rawInfo[VERSIONMADEBY_POS]%10);
}


inline void ZipFileInfo::getRequiredVersion(int& major, int& minor)
{
	major = (_rawInfo[VERSION_NEEDED_POS]/10);
	minor = (_rawInfo[VERSION_NEEDED_POS]%10);
}


inline ZipCommon::HostSystem ZipFileInfo::getHostSystem() const
{
	return static_cast<ZipCommon::HostSystem>(_rawInfo[VERSION_NEEDED_POS + 1]);
}


inline Poco::UInt16 ZipFileInfo::getDiskNumberStart() const
{
	return ZipUtil::get16BitValue(_rawInfo, DISKNUMBERSTART_POS);
}


inline ZipCommon::FileType ZipFileInfo::getFileType() const
{
	return static_cast<ZipCommon::FileType>(_rawInfo[INTERNALFILE_ATTR_POS] & 0x01);
}


inline Poco::UInt32 ZipFileInfo::getExternalFileAttributes() const
{
	return ZipUtil::get32BitValue(_rawInfo, EXTERNALFILE_ATTR_POS);
}


inline Poco::UInt32 ZipFileInfo::getHeaderSize() const
{
	return FULLHEADER_SIZE + getFileNameLength() + getExtraFieldLength() + getFileCommentLength();
}


inline void ZipFileInfo::setCRC(Poco::UInt32 val)
{
	_crc32 = val;
	ZipUtil::set32BitValue(val, _rawInfo, CRC32_POS);
}


inline void ZipFileInfo::setOffset(Poco::UInt32 val)
{
	ZipUtil::set32BitValue(val, _rawInfo, RELATIVEOFFSETLOCALHEADER_POS);
}


inline void ZipFileInfo::setCompressedSize(Poco::UInt32 val)
{
	_compressedSize = val;
	ZipUtil::set32BitValue(val, _rawInfo, COMPRESSED_SIZE_POS);
}


inline void ZipFileInfo::setUncompressedSize(Poco::UInt32 val)
{
	_uncompressedSize = val;
	ZipUtil::set32BitValue(val, _rawInfo, UNCOMPRESSED_SIZE_POS);
}


inline void ZipFileInfo::setCompressionMethod(ZipCommon::CompressionMethod cm)
{
	ZipUtil::set16BitValue(static_cast<Poco::UInt16>(cm), _rawInfo, COMPR_METHOD_POS);
}


inline void ZipFileInfo::setCompressionLevel(ZipCommon::CompressionLevel cl)
{
	// bit 1 and 2 indicate the level
	Poco::UInt16 val = static_cast<Poco::UInt16>(cl);
	val <<= 1; 
	Poco::UInt16 mask = 0xfff9;
	_rawInfo[GENERAL_PURPOSE_POS] = ((_rawInfo[GENERAL_PURPOSE_POS] & mask) | val);
}


inline void ZipFileInfo::setFileNameLength(Poco::UInt16 size)
{
	ZipUtil::set16BitValue(size, _rawInfo, FILENAME_LENGTH_POS);
}


inline void ZipFileInfo::setHostSystem(ZipCommon::HostSystem hs)
{
	_rawInfo[VERSIONMADEBY_POS + 1] = static_cast<char>(hs);
	_rawInfo[VERSION_NEEDED_POS + 1] = static_cast<char>(hs);
}


inline void ZipFileInfo::setRequiredVersion(int major, int minor)
{
	poco_assert (minor < 10);
	poco_assert (major < 24);
	Poco::UInt8 val = static_cast<unsigned char>(major)*10+static_cast<unsigned char>(minor);
	_rawInfo[VERSIONMADEBY_POS] = static_cast<char>(val);
	_rawInfo[VERSION_NEEDED_POS] = static_cast<char>(val);
}


inline void ZipFileInfo::setLastModifiedAt(const Poco::DateTime& dt)
{
	_lastModifiedAt = dt;
	ZipUtil::setDateTime(dt, _rawInfo, LASTMODFILETIME_POS, LASTMODFILEDATE_POS);
}


inline void ZipFileInfo::setEncryption(bool val)
{
	if (val)
		_rawInfo[GENERAL_PURPOSE_POS] |= 0x01;
	else
		_rawInfo[GENERAL_PURPOSE_POS] &= 0xfe;
}


inline void ZipFileInfo::setFileName(const std::string& str)
{
	_fileName = str;
	setFileNameLength(static_cast<Poco::UInt16>(str.size()));
}


inline void ZipFileInfo::setExternalFileAttributes(Poco::UInt32 attrs)
{
	ZipUtil::set32BitValue(attrs, _rawInfo, EXTERNALFILE_ATTR_POS);
}


} } // namespace Poco::Zip


#endif // Zip_ZipFileInfo_INCLUDED
