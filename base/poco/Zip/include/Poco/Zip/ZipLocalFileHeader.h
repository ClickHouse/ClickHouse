//
// ZipLocalFileHeader.h
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

	ZipLocalFileHeader(const Poco::Path& fileName, const Poco::DateTime& lastModifiedAt, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl, bool forceZip64 = false);
		/// Creates a zip file header from an absoluteFile. fileName is the name of the file in the zip, outputIsSeekable determines if we write
		/// CRC and file sizes to the LocalFileHeader or after data compression into a ZipDataInfo
		/// If forceZip64 is set true then the file header is allocated with zip64 extension.

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

	bool hasSupportedCompressionMethod() const;

	const Poco::DateTime& lastModifiedAt() const;

	Poco::UInt32 getCRC() const;

	Poco::UInt64 getCompressedSize() const;

	Poco::UInt64 getUncompressedSize() const;

	void setCRC(Poco::UInt32 val);

	void setCompressedSize(Poco::UInt64 val);

	void setUncompressedSize(Poco::UInt64 val);

	const std::string& getFileName() const;

	bool isFile() const;

	bool isDirectory() const;

	bool hasExtraField() const;

	const std::string& getExtraField() const;

	bool hasData() const;

	bool searchCRCAndSizesAfterData() const;

	void setSearchCRCAndSizesAfterData(bool val);

	void setFileName(const std::string& fileName, bool isDirectory);

	bool needsZip64() const;

	void setZip64Data();

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
		COMPRESSED_SIZE_SIZE = 4,
		COMPRESSED_SIZE_POS = CRC32_POS + CRC32_SIZE,
		UNCOMPRESSED_SIZE_SIZE = 4,
		UNCOMPRESSED_SIZE_POS = COMPRESSED_SIZE_POS + COMPRESSED_SIZE_SIZE,
		FILE_LENGTH_SIZE = 2,
		FILE_LENGTH_POS = UNCOMPRESSED_SIZE_POS + UNCOMPRESSED_SIZE_SIZE,
		EXTRA_FIELD_LENGTH = 2,
		EXTRA_FIELD_POS = FILE_LENGTH_POS + FILE_LENGTH_SIZE,
		FULLHEADER_SIZE = 30,

		EXTRA_DATA_TAG_SIZE = 2,
		EXTRA_DATA_TAG_POS = 0,
		EXTRA_DATA_SIZE_SIZE = 2,
		EXTRA_DATA_SIZE_POS = EXTRA_DATA_TAG_POS + EXTRA_DATA_TAG_SIZE,
		EXTRA_DATA_POS = EXTRA_DATA_SIZE_POS + EXTRA_DATA_SIZE_SIZE,
		EXTRA_DATA_UNCOMPRESSED_SIZE_SIZE = 8,
		EXTRA_DATA_COMPRESSED_SIZE_SIZE = 8,
		FULLEXTRA_DATA_SIZE = 20
	};

	bool		   _forceZip64;
	char           _rawHeader[FULLHEADER_SIZE];
	std::streamoff _startPos;
	std::streamoff _endPos;
	std::string    _fileName;
	Poco::DateTime _lastModifiedAt;
	std::string    _extraField;
	Poco::UInt32   _crc32;
	Poco::UInt64   _compressedSize;
	Poco::UInt64   _uncompressedSize;

	friend class ZipStreamBuf;
};


inline void ZipLocalFileHeader::setFileNameLength(Poco::UInt16 size)
{
	ZipUtil::set16BitValue(size, _rawHeader, FILE_LENGTH_POS);
}


inline void ZipLocalFileHeader::setExtraFieldSize(Poco::UInt16 size)
{
	ZipUtil::set16BitValue(size, _rawHeader, EXTRA_FIELD_POS);
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


inline bool ZipLocalFileHeader::needsZip64() const
{
	return _forceZip64 || _startPos >= ZipCommon::ZIP64_MAGIC || _compressedSize >= ZipCommon::ZIP64_MAGIC || _uncompressedSize >= ZipCommon::ZIP64_MAGIC;
}


inline void ZipLocalFileHeader::setZip64Data()
{
	setRequiredVersion(4, 5);
	char data[FULLEXTRA_DATA_SIZE];
	ZipUtil::set16BitValue(ZipCommon::ZIP64_EXTRA_ID, data, EXTRA_DATA_TAG_POS);
	Poco::UInt16 pos = EXTRA_DATA_POS;
	ZipUtil::set64BitValue(_uncompressedSize, data, pos); pos += 8;
	ZipUtil::set32BitValue(ZipCommon::ZIP64_MAGIC, _rawHeader, UNCOMPRESSED_SIZE_POS);
	ZipUtil::set64BitValue(_compressedSize, data, pos); pos += 8;
	ZipUtil::set32BitValue(ZipCommon::ZIP64_MAGIC, _rawHeader, COMPRESSED_SIZE_POS);
	ZipUtil::set16BitValue(pos - EXTRA_DATA_POS, data, EXTRA_DATA_SIZE_POS);
	_extraField = std::string(data, pos);
	ZipUtil::set16BitValue(pos, _rawHeader, EXTRA_FIELD_POS);
}


inline void ZipLocalFileHeader::setRequiredVersion(int major, int minor)
{
	poco_assert (minor < 10);
	poco_assert (major < 24);
	_rawHeader[VERSION_POS] = static_cast<char>(static_cast<unsigned char>(major)*10+static_cast<unsigned char>(minor));
}


inline Poco::UInt16 ZipLocalFileHeader::getFileNameLength() const
{
	return ZipUtil::get16BitValue(_rawHeader, FILE_LENGTH_POS);
}


inline Poco::UInt16 ZipLocalFileHeader::getExtraFieldLength() const
{
	return ZipUtil::get16BitValue(_rawHeader, EXTRA_FIELD_POS);
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
	_endPos = start + getHeaderSize()+static_cast<std::streamoff>(getCompressedSize());
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


inline bool ZipLocalFileHeader::hasSupportedCompressionMethod() const
{
	ZipCommon::CompressionMethod method = getCompressionMethod();
	return method == ZipCommon::CM_DEFLATE || method == ZipCommon::CM_STORE;
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


inline Poco::UInt64 ZipLocalFileHeader::getCompressedSize() const
{
	return _compressedSize;
}


inline Poco::UInt64 ZipLocalFileHeader::getUncompressedSize() const
{
	return _uncompressedSize;
}


inline void ZipLocalFileHeader::setCRC(Poco::UInt32 val)
{
	_crc32 = val;
	ZipUtil::set32BitValue(val, _rawHeader, CRC32_POS);
}


inline void ZipLocalFileHeader::setCompressedSize(Poco::UInt64 val)
{
	_compressedSize = val;
	ZipUtil::set32BitValue(val >= ZipCommon::ZIP64_MAGIC ? ZipCommon::ZIP64_MAGIC : static_cast<Poco::UInt32>(val), _rawHeader, COMPRESSED_SIZE_POS);
}


inline void ZipLocalFileHeader::setUncompressedSize(Poco::UInt64 val)
{
	_uncompressedSize = val;
	ZipUtil::set32BitValue(val >= ZipCommon::ZIP64_MAGIC ? ZipCommon::ZIP64_MAGIC : static_cast<Poco::UInt32>(val), _rawHeader, UNCOMPRESSED_SIZE_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getCRCFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, CRC32_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getCompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, COMPRESSED_SIZE_POS);
}


inline Poco::UInt32 ZipLocalFileHeader::getUncompressedSizeFromHeader() const
{
	return ZipUtil::get32BitValue(_rawHeader, UNCOMPRESSED_SIZE_POS);
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
	return getUncompressedSize() == 0 && _fileName[_fileName.length()-1] == '/';
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
	return getDataStartPos()+static_cast<std::streamoff>(getCompressedSize());
}


} } // namespace Poco::Zip


#endif // Zip_ZipLocalFileHeader_INCLUDED
