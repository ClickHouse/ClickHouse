//
// ZipLocalFileHeader.cpp
//
// Library: Zip
// Package: Zip
// Module:  ZipLocalFileHeader
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Zip/ParseCallback.h"
#include "Poco/Buffer.h"
#include "Poco/Exception.h"
#include "Poco/File.h"
#include <cstring>


namespace Poco {
namespace Zip {


const char ZipLocalFileHeader::HEADER[ZipCommon::HEADER_SIZE] = {'\x50', '\x4b', '\x03', '\x04'};


ZipLocalFileHeader::ZipLocalFileHeader(const Poco::Path& fileName,
    const Poco::DateTime& lastModifiedAt,
    ZipCommon::CompressionMethod cm,
    ZipCommon::CompressionLevel cl,
    bool forceZip64):
    _forceZip64(forceZip64),
    _rawHeader(),
    _startPos(-1),
    _endPos(-1),
    _fileName(),
    _lastModifiedAt(),
    _extraField(),
    _crc32(0),
    _compressedSize(0),
    _uncompressedSize(0)
{
    std::memcpy(_rawHeader, HEADER, ZipCommon::HEADER_SIZE);
    std::memset(_rawHeader+ZipCommon::HEADER_SIZE, 0, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
    setHostSystem(ZipCommon::HS_FAT);
    setEncryption(false);
    setExtraFieldSize(0);
    setLastModifiedAt(lastModifiedAt);
    init(fileName, cm, cl);
}


ZipLocalFileHeader::ZipLocalFileHeader(std::istream& inp, bool assumeHeaderRead, ParseCallback& callback):
    _forceZip64(false),
    _rawHeader(),
    _startPos(inp.tellg()),
    _endPos(-1),
    _fileName(),
    _lastModifiedAt(),
    _extraField(),
    _crc32(0),
    _compressedSize(0),
    _uncompressedSize(0)
{
    poco_assert_dbg( (EXTRA_FIELD_POS+EXTRA_FIELD_LENGTH) == FULLHEADER_SIZE);

    if (assumeHeaderRead)
        _startPos -= ZipCommon::HEADER_SIZE;

    parse(inp, assumeHeaderRead);

    bool ok = callback.handleZipEntry(inp, *this);

    if (ok)
    {
        if (searchCRCAndSizesAfterData())
        {
            char header[ZipCommon::HEADER_SIZE]={'\x00', '\x00', '\x00', '\x00'};
            inp.read(header, ZipCommon::HEADER_SIZE);
            if (_forceZip64)
            {
                ZipDataInfo64 nfo(inp, true);
                setCRC(nfo.getCRC32());
                setCompressedSize(nfo.getCompressedSize());
                setUncompressedSize(nfo.getUncompressedSize());
            }
            else
            {
                ZipDataInfo nfo(inp, true);
                setCRC(nfo.getCRC32());
                setCompressedSize(nfo.getCompressedSize());
                setUncompressedSize(nfo.getUncompressedSize());
            }
        }
    }
    else
    {
        poco_assert_dbg(!searchCRCAndSizesAfterData());
        ZipUtil::sync(inp);
    }
    _endPos = _startPos + getHeaderSize() + _compressedSize; // exclude the data block!
}


ZipLocalFileHeader::~ZipLocalFileHeader()
{
}


void ZipLocalFileHeader::parse(std::istream& inp, bool assumeHeaderRead)
{
    if (!assumeHeaderRead)
    {
        inp.read(_rawHeader, ZipCommon::HEADER_SIZE);
		if (inp.gcount() != ZipCommon::HEADER_SIZE)
			throw Poco::IOException("Failed to read local file header");
		if (std::memcmp(_rawHeader, HEADER, ZipCommon::HEADER_SIZE) != 0)
			throw Poco::DataFormatException("Bad local file header");
    }
    else
    {
        std::memcpy(_rawHeader, HEADER, ZipCommon::HEADER_SIZE);
    }

    // read the rest of the header
    inp.read(_rawHeader + ZipCommon::HEADER_SIZE, FULLHEADER_SIZE - ZipCommon::HEADER_SIZE);
    poco_assert (_rawHeader[VERSION_POS + 1]>= ZipCommon::HS_FAT && _rawHeader[VERSION_POS + 1] < ZipCommon::HS_UNUSED);
    poco_assert (getMajorVersionNumber() <= 4); // Allow for Zip64 version 4.5
    poco_assert (ZipUtil::get16BitValue(_rawHeader, COMPR_METHOD_POS) < ZipCommon::CM_UNUSED);
    parseDateTime();
    Poco::UInt16 len = getFileNameLength();
    if (len > 0)
    {
    	Poco::Buffer<char> buf(len);
    	inp.read(buf.begin(), len);
    	_fileName = std::string(buf.begin(), len);
	}

    if (!searchCRCAndSizesAfterData())
    {
        _crc32 = getCRCFromHeader();
        _compressedSize = getCompressedSizeFromHeader();
        _uncompressedSize = getUncompressedSizeFromHeader();
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
					_forceZip64 = true;
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
				}
				else
				{
					ptr += size;
				}
			}
        }
    }
}


bool ZipLocalFileHeader::searchCRCAndSizesAfterData() const
{
	if (getCompressionMethod() == ZipCommon::CM_STORE || getCompressionMethod() == ZipCommon::CM_DEFLATE)
	{
		// check bit 3
		return ((ZipUtil::get16BitValue(_rawHeader, GENERAL_PURPOSE_POS) & 0x0008) != 0);
	}
	return false;
}


void ZipLocalFileHeader::setFileName(const std::string& fileName, bool isDirectory)
{
    poco_assert (!fileName.empty());
    Poco::Path aPath(fileName);

    if (isDirectory)
    {
        aPath.makeDirectory();
        setCRC(0);
        setCompressedSize(0);
        setUncompressedSize(0);
        setCompressionMethod(ZipCommon::CM_STORE);
        setCompressionLevel(ZipCommon::CL_NORMAL);
    }
    else
    {
        aPath.makeFile();
    }
    _fileName = aPath.toString(Poco::Path::PATH_UNIX);
    if (_fileName[0] == '/')
        _fileName = _fileName.substr(1);
    if (isDirectory)
    {
        poco_assert_dbg (_fileName[_fileName.size()-1] == '/');
    }
    setFileNameLength(static_cast<Poco::UInt16>(_fileName.size()));
}


void ZipLocalFileHeader::init(const Poco::Path& fName, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl)
{
    poco_assert (_fileName.empty());
    setSearchCRCAndSizesAfterData(false);
    Poco::Path fileName(fName);
    fileName.setDevice(""); // clear device!
    setFileName(fileName.toString(Poco::Path::PATH_UNIX), fileName.isDirectory());
    setRequiredVersion(2, 0);
    if (fileName.isFile())
    {
        setCompressionMethod(cm);
        setCompressionLevel(cl);
    }
    else
        setCompressionMethod(ZipCommon::CM_STORE);
    if (_forceZip64)
        setZip64Data();

    _rawHeader[GENERAL_PURPOSE_POS+1] |= 0x08; // Set "language encoding flag" to indicate that filenames and paths are in UTF-8.
}


std::string ZipLocalFileHeader::createHeader() const
{
    std::string result(_rawHeader, FULLHEADER_SIZE);
    result.append(_fileName);
    result.append(_extraField);
    return result;
}


} } // namespace Poco::Zip
