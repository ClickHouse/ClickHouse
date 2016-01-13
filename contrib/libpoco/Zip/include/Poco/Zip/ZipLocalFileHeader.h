//
// ZipLocalFileHeader.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/ZipLocalFileHeader.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipLocalFileHeader
//
// Definition of the ZipLocalFileHeader class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipLocalFileHeader_INCLUDED
#define Zip_ZipLocalFileHeader_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ZipUtil.h"
#include "Poco/Zip/ZipCommon.h"
#include "Poco/DateTime.h"
#include "Poco/Path.h"
#include <istream>


namespace Poco {
namespace Zip {


class ParseCallback;


class Zip_API ZipLocalFileHeader
	/// Stores a Zip local file header
{
public:
	static const char HEADER[ZipCommon::HEADER_SIZE];

	ZipLocalFileHeader(const Poco::Path& fileName, const Poco::DateTime& lastModifiedAt, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl);
		/// Creates a zip file header from an absoluteFile. fileName is the name of the file in the zip, outputIsSeekable determines if we write
		/// CRC and file sizes to the LocalFileHeader or after data compression into a ZipDataInfo

	ZipLocalFileHeader(std::istream& inp, bool assumeHeaderRead, ParseCallback& callback);
		/// Creates the ZipLocalFileHeader by parsing the input stream.
		/// If assumeHeaderRead is true we assume that the first 4 bytes were already read outside.
		/// If skipOverDataBlock is true we position the stream after the data block (either at the next FileHeader or the Directory Entry)

	virtual ~ZipLocalFileHeader();
		/// Destroys the ZipLocalFileHeader.

	ZipCommon::HostSystem getHostSystem() const;

	int getMajorVersionNumber() const;

	int getMinorVersionNumber() const;

	void getRequiredVersion(int& major, int& minor);
		/// The minimum version required to extract the data

	Poco::UInt32 getHeaderSize() const;
		/// Returns the total size of the header including filename + extra field size

	void setStartPos(std::streamoff start);
		/// Sets the start position to start and the end position to start+compressedSize

	std::streamoff getStartPos() const;
		/// Returns the position of the first byte of the header in the file stream

	std::streamoff getEndPos() const;
		/// Points past the last byte of the file entry (ie. either the first byte of the next header, or the directory)

	std::streamoff getDataStartPos() const;
		/// Returns the streamoffset for the very first byte of data. Will be equal to DataEndPos if no data present

	std::streamoff getDataEndPos() const;

	ZipCommon::CompressionMethod getCompressionMethod() const;

	ZipCommon::CompressionLevel getCompressionLevel() const;
	/// Returns the compression level used. Only valid when the compression method is CM_DEFLATE

	bool isEncrypted() const;

	const Poco::DateTime& lastModifiedAt() const;

	Poco::UInt32 getCRC() const;

	Poco::UInt32 getCompressedSize() const;

	Poco::UInt32 getUncompressedSize() const;

	void setCRC(Poco::UInt32 val);

	void setCompressedSize(Poco::UInt32 val);

	void setUncompressedSize(Poco::UInt32 val);

	const std::string& getFileName() const;

	bool isFile() const;

	bool isDirectory() const;

	bool hasExtraField() const;

	const std::string& getExtraField() const;

	bool hasData() const;

	bool searchCRCAndSizesAfterData() const;

	void setSearchCRCAndSizesAfterData(bool val);

	void setFileName(const std::string& fileName, bool isDirectory);

	std::string createHeader() const;
		/// Creates a header

private:
	void parse(std::istream& inp, bool assumeHeaderRead);

	void parseDateTime();

	void init(const Poco::Path& fileName, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl);

	Poco::UInt16 getFileNameLength() const;
	
	Poco::UInt16 getExtraFieldLength() const;

	Poco::UInt32 getCRCFromHeader() const;

	Poco::UInt32 getCompressedSizeFromHeader() const;

	Poco::UInt32 getUncompressedSizeFromHeader() const;

	void setRequiredVersion(int major, int minor);

	void setHostSystem(ZipCommon::HostSystem hs);

	void setLastModifiedAt(const Poco::DateTime& dt);

	void setEncryption(bool val);

	void setFileNameLength(Poco::UInt16 size);

	void setExtraFieldSize(Poco::UInt16 size);

	void setCompressionMethod(ZipCommon::CompressionMethod cm);

	void setCompressionLevel(ZipCommon::CompressionLevel cl);

private:
	enum
	{
		HEADER_POS = 0,
		VERSION_SIZE = 2,
		VERSION_POS = HEADER_POS+ZipCommon::HEADER_SIZE,
		GENERAL_PURPOSE_SIZE = 2,
		GENERAL_PURPOSE_POS = VERSION_POS + VERSION_SIZE,
		COMPR_METHOD_SIZE = 2,
		COMPR_METHOD_POS = GENERAL_PURPOSE_POS + GENERAL_PURPOSE_SIZE,
		LASTMODEFILETIME_SIZE = 2,
		LASTMODEFILETIME_POS = COMPR_METHOD_POS + COMPR_METHOD_SIZE,
		LASTMODEFILEDATE_SIZE = 2,
		LASTMODEFILEDATE_POS = LASTMODEFILETIME_POS + LASTMODEFILETIME_SIZE,
		CRC32_SIZE = 4,
		CRC32_POS = LASTMODEFILEDATE_POS + LASTMODEFILEDATE_SIZE,
		COMPRESSEDSIZE_SIZE = 4,
		COMPRESSEDSIZE_POS = CRC32_POS + CRC32_SIZE,
		UNCOMPRESSEDSIZE_SIZE = 4,
		UNCOMPRESSEDSIZE_POS = COMPRESSEDSIZE_POS + COMPRESSEDSIZE_SIZE,
		FILELENGTH_SIZE = 2,
		FILELENGTH_POS = UNCOMPRESSEDSIZE_POS + UNCOMPRESSEDSIZE_SIZE,
		EXTRAFIELD_LENGTH = 2,
		EXTRAFIELD_POS = FILELENGTH_POS + FILELENGTH_SIZE,
		FULLHEADER_SIZE = 30
	};

	char           _rawHeader[FULLHEADER_SIZE];
	std::streamoff _startPos;
	std::streamoff _endPos;
	std::string    _fileName;
	Poco::DateTime _lastModifiedAt;
	std::string    _extraField;
	Poco::UInt32   _crc32;
	Poco::UInt32   _compressedSize;
	Poco::UInt32   _uncompressedSize;
};


inline void ZipLocalFileHeader::setFileNameLength(Poco::UInt16 size)
{
	ZipUtil::set16BitValue(size, _rawHeader, FILELENGTH_POS);
}


inline void ZipLocalFileHeader::setExtraFieldSize(Poco::UInt16 size)
{
	ZipUtil::set16BitValue(size, _rawHeader, EXTRAFIELD_POS);
}


inline ZipCommon::HostSystem ZipLocalFileHeader::getHostSystem() const
{
	return static_cast<ZipCommon::HostSystem>(_rawHeader[VERSION_POS + 1]);
}


inline void ZipLocalFileHeader::setHostSystem(ZipCommon::HostSystem hs)
{
	_rawHeader[VERSION_POS + 1] = static_cast<char>(hs);
}


inline int ZipLocalFileHeader::getMajorVersionNumber() const
{
	return (_rawHeader[VERSION_POS]/10);
}


inline int ZipLocalFileHeader::getMinorVersionNumber() const
{
	return (_rawHeader[VERSION_POS]%10);
}


inline void ZipLocalFileHeader::getRequiredVersion(int& major, int& minor)
{
	major = getMajorVersionNumber();
	minor = getMinorVersionNumber();
}


inline void ZipLocalFileHeader::setRequiredVersion(int major, int minor)
{
	poco_assert (minor < 10);
	poco_assert (major < 24);
	_rawHeader[VERSION_POS] = static_cast<char>(static_cast<unsigned char>(major)*10+static_cast<unsigned char>(minor));
}

inline Poco::UInt16 ZipLocalFileHeader::getFileNameLength() const
{
	return ZipUtil::get16BitValue(_rawHeader, FILELENGTH_POS);
}


inline Poco::UInt16 ZipLocalFileHeader::getExtraFieldLength() const
{
	return ZipUtil::get16BitValue(_rawHeader, EXTRAFIELD_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getHeaderSize() const
{
	return FULLHEADER_SIZE+getExtraFieldLength()+getFileNameLength();
}


inline std::streamoff ZipLocalFileHeader::getStartPos() const
{
	return _startPos;
}


inline void ZipLocalFileHeader::setStartPos(std::streamoff start)
{
	_startPos = start;
	_endPos = start + getHeaderSize()+getCompressedSize();
}


inline std::streamoff ZipLocalFileHeader::getEndPos() const
{
	return _endPos;
}


inline void ZipLocalFileHeader::parseDateTime()
{
	_lastModifiedAt = ZipUtil::parseDateTime(_rawHeader, LASTMODEFILETIME_POS, LASTMODEFILEDATE_POS);
}


inline void ZipLocalFileHeader::setLastModifiedAt(const Poco::DateTime& dt)
{
	_lastModifiedAt = dt;
	ZipUtil::setDateTime(dt, _rawHeader, LASTMODEFILETIME_POS, LASTMODEFILEDATE_POS);
}


inline ZipCommon::CompressionMethod ZipLocalFileHeader::getCompressionMethod() const
{
	return static_cast<ZipCommon::CompressionMethod>(ZipUtil::get16BitValue(_rawHeader, COMPR_METHOD_POS));
}


inline ZipCommon::CompressionLevel ZipLocalFileHeader::getCompressionLevel() const
{
	// bit 1 and 2 indicate the level
	return static_cast<ZipCommon::CompressionLevel>((ZipUtil::get16BitValue(_rawHeader, GENERAL_PURPOSE_POS)>>1) & 0x0003);
}


inline void ZipLocalFileHeader::setCompressionMethod(ZipCommon::CompressionMethod cm)
{
	ZipUtil::set16BitValue(static_cast<Poco::UInt16>(cm), _rawHeader, COMPR_METHOD_POS);
}


inline void ZipLocalFileHeader::setCompressionLevel(ZipCommon::CompressionLevel cl)
{
	// bit 1 and 2 indicate the level
	Poco::UInt16 val = static_cast<Poco::UInt16>(cl);
	val <<= 1; 
	Poco::UInt16 mask = 0xfff9;
	_rawHeader[GENERAL_PURPOSE_POS] = ((_rawHeader[GENERAL_PURPOSE_POS] & mask) | val);
}


inline bool ZipLocalFileHeader::isEncrypted() const
{
	// bit 0 indicates encryption
	return ((ZipUtil::get16BitValue(_rawHeader, GENERAL_PURPOSE_POS) & 0x0001) != 0);
}


inline void ZipLocalFileHeader::setEncryption(bool val)
{
	if (val)
		_rawHeader[GENERAL_PURPOSE_POS] |= 0x01;
	else
		_rawHeader[GENERAL_PURPOSE_POS] &= 0xfe;
}


inline void ZipLocalFileHeader::setSearchCRCAndSizesAfterData(bool val)
{
	//set bit 3 of general purpose reg
	if (val)
		_rawHeader[GENERAL_PURPOSE_POS] |= 0x08;
	else
		_rawHeader[GENERAL_PURPOSE_POS] &= 0xf7;
}


inline const Poco::DateTime& ZipLocalFileHeader::lastModifiedAt() const
{
	return _lastModifiedAt;
}


inline Poco::UInt32 ZipLocalFileHeader::getCRC() const
{
	return _crc32;
}


inline Poco::UInt32 ZipLocalFileHeader::getCompressedSize() const
{
	return _compressedSize;
}


inline Poco::UInt32 ZipLocalFileHeader::getUncompressedSize() const
{
	return _uncompressedSize;
}


inline void ZipLocalFileHeader::setCRC(Poco::UInt32 val)
{
	_crc32 = val;
	ZipUtil::set32BitValue(val, _rawHeader, CRC32_POS);
}


inline void ZipLocalFileHeader::setCompressedSize(Poco::UInt32 val)
{
	_compressedSize = val;
	ZipUtil::set32BitValue(val, _rawHeader, COMPRESSEDSIZE_POS);
}


inline void ZipLocalFileHeader::setUncompressedSize(Poco::UInt32 val)
{
	_uncompressedSize = val;
	ZipUtil::set32BitValue(val, _rawHeader, UNCOMPRESSEDSIZE_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getCRCFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, CRC32_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getCompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, COMPRESSEDSIZE_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getUncompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, UNCOMPRESSEDSIZE_POS);
}


inline const std::string& ZipLocalFileHeader::getFileName() const
{
	return _fileName;
}


inline bool ZipLocalFileHeader::isFile() const
{
	return !isDirectory();
}


inline bool ZipLocalFileHeader::isDirectory() const
{
	poco_assert_dbg(!_fileName.empty());
	return getUncompressedSize() == 0 && getCompressionMethod() == ZipCommon::CM_STORE && _fileName[_fileName.length()-1] == '/';
}


inline bool ZipLocalFileHeader::hasExtraField() const
{
	return getExtraFieldLength() > 0;
}


inline const std::string& ZipLocalFileHeader::getExtraField() const
{
	return _extraField;
}


inline bool ZipLocalFileHeader::hasData() const
{
	return (getCompressedSize() > 0);
}


inline std::streamoff ZipLocalFileHeader::getDataStartPos() const
{
	return getStartPos() + getHeaderSize();
}


inline std::streamoff ZipLocalFileHeader::getDataEndPos() const
{
	return getDataStartPos()+getCompressedSize();
}


} } // namespace Poco::Zip


#endif // Zip_ZipLocalFileHeader_INCLUDED
