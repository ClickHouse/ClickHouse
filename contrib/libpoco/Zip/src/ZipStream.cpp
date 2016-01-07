//
// ZipStream.cpp
//
// $Id: //poco/1.4/Zip/src/ZipStream.cpp#4 $
//
// Library: Zip
// Package: Zip
// Module:  ZipStream
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipStream.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/AutoDetectStream.h"
#include "Poco/Zip/PartialStream.h"
#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Zip/ZipException.h"
#include "Poco/Exception.h"
#include "Poco/InflatingStream.h"
#include "Poco/DeflatingStream.h"
#if defined(POCO_UNBUNDLED)
#include <zlib.h>
#else
#include "Poco/zlib.h"
#endif


namespace Poco {
namespace Zip {


ZipStreamBuf::ZipStreamBuf(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::in),
	_pIstr(&istr),
	_pOstr(0),
	_ptrBuf(),
	_ptrOBuf(),
	_ptrHelper(),
	_ptrOHelper(),
	_crc32(Poco::Checksum::TYPE_CRC32),
	_expectedCrc32(0),
	_checkCRC(true),
	_bytesWritten(0),
	_pHeader(0)
{
	if (fileEntry.isDirectory())
		return;
	_expectedCrc32 = fileEntry.getCRC();
	std::streamoff start = fileEntry.getDataStartPos();
	std::streamoff end = fileEntry.getDataEndPos();
	_checkCRC = !fileEntry.searchCRCAndSizesAfterData();
	if (fileEntry.getCompressionMethod() == ZipCommon::CM_DEFLATE)
	{
		// Fake init bytes at beginning of stream
		std::string init = ZipUtil::fakeZLibInitString(fileEntry.getCompressionLevel());

		// Fake adler at end of stream: just some dummy value, not checked anway
		std::string crc(4, ' ');
		if (fileEntry.searchCRCAndSizesAfterData())
		{
			_ptrHelper = new AutoDetectInputStream(istr, init, crc, reposition, start);
		}
		else
		{
			_ptrHelper = new PartialInputStream(istr, start, end, reposition, init, crc);
		}
		_ptrBuf = new Poco::InflatingInputStream(*_ptrHelper, Poco::InflatingStreamBuf::STREAM_ZIP);
	}
	else if (fileEntry.getCompressionMethod() == ZipCommon::CM_STORE)
	{
		if (fileEntry.searchCRCAndSizesAfterData())
		{
			_ptrBuf = new AutoDetectInputStream(istr, "", "", reposition, start);
		}
		else
		{
			_ptrBuf = new PartialInputStream(istr, start, end, reposition);
		}
	}
	else throw Poco::NotImplementedException("Unsupported compression method");
}


ZipStreamBuf::ZipStreamBuf(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool reposition):
	Poco::BufferedStreamBuf(STREAM_BUFFER_SIZE, std::ios::out),
	_pIstr(0),
	_pOstr(&ostr),
	_ptrBuf(),
	_ptrOBuf(),
	_ptrHelper(),
	_ptrOHelper(),
	_crc32(Poco::Checksum::TYPE_CRC32),
	_expectedCrc32(0),
	_checkCRC(false),
	_bytesWritten(0),
	_pHeader(&fileEntry)
{
	if (fileEntry.isEncrypted())
		throw Poco::NotImplementedException("Encryption not supported");

	if (fileEntry.isDirectory())
	{
		// only header, no payload, zero crc
		fileEntry.setSearchCRCAndSizesAfterData(false);
		fileEntry.setCompressedSize(0);
		fileEntry.setUncompressedSize(0);
		fileEntry.setCRC(0);
		std::string header = fileEntry.createHeader();
		ostr.write(header.c_str(), static_cast<std::streamsize>(header.size()));
	}
	else
	{
		fileEntry.setSearchCRCAndSizesAfterData(!reposition);
		if (fileEntry.getCompressionMethod() == ZipCommon::CM_DEFLATE)
		{
			int level = Z_DEFAULT_COMPRESSION;
			if (fileEntry.getCompressionLevel() == ZipCommon::CL_FAST || fileEntry.getCompressionLevel() == ZipCommon::CL_SUPERFAST)
				level = Z_BEST_SPEED;
			else if (fileEntry.getCompressionLevel() == ZipCommon::CL_MAXIMUM)
				level = Z_BEST_COMPRESSION;
			// ignore the zlib init string which is of size 2 and also ignore the 4 byte adler32 value at the end of the stream!
			_ptrOHelper = new PartialOutputStream(*_pOstr, 2, 4, false); 
			_ptrOBuf = new Poco::DeflatingOutputStream(*_ptrOHelper, DeflatingStreamBuf::STREAM_ZLIB, level);
		}
		else if (fileEntry.getCompressionMethod() == ZipCommon::CM_STORE)
		{
			_ptrOHelper = new PartialOutputStream(*_pOstr, 0, 0, false);
			_ptrOBuf = new PartialOutputStream(*_ptrOHelper, 0, 0, false);
		}
		else throw Poco::NotImplementedException("Unsupported compression method");

		// now write the header to the ostr!
		std::string header = fileEntry.createHeader();
		ostr.write(header.c_str(), static_cast<std::streamsize>(header.size()));
	}
}


ZipStreamBuf::~ZipStreamBuf()
{
	// make sure destruction of streams happens in correct order
	_ptrOBuf = 0;
	_ptrOHelper = 0;
	_ptrBuf = 0;
	_ptrHelper = 0;
}


int ZipStreamBuf::readFromDevice(char* buffer, std::streamsize length)
{
	if (!_ptrBuf) return 0; // directory entry
	_ptrBuf->read(buffer, length);
	int cnt = _ptrBuf->gcount();
	if (cnt > 0)
	{
		_crc32.update(buffer, cnt);
	}
	else
	{
		if (_crc32.checksum() != _expectedCrc32)
		{
			if (_checkCRC)
				throw ZipException("CRC failure");
			else
			{
				// the CRC value is written directly after the data block
				// parse it directly from the input stream
				ZipDataInfo nfo(*_pIstr, false);
				// now push back the header to the stream, so that the ZipLocalFileHeader can read it
				Poco::Int32 size = static_cast<Poco::Int32>(nfo.getFullHeaderSize());
				_expectedCrc32 = nfo.getCRC32();
				const char* rawHeader = nfo.getRawHeader();
				for (Poco::Int32 i = size-1; i >= 0; --i)
					_pIstr->putback(rawHeader[i]);
				if (!crcValid())
					throw ZipException("CRC failure");
			}
		}
	}
	return cnt;
}


int ZipStreamBuf::writeToDevice(const char* buffer, std::streamsize length)
{
	if (!_ptrOBuf) return 0; // directory entry
	if (length == 0)
		return 0;
	_bytesWritten += length;
	_ptrOBuf->write(buffer, length);
	_crc32.update(buffer, length);
	return length;
}


void ZipStreamBuf::close()
{
	if (_ptrOBuf && _pHeader)
	{
		_ptrOBuf->flush();
		DeflatingOutputStream* pDO = dynamic_cast<DeflatingOutputStream*>(_ptrOBuf.get());
		if (pDO)
			pDO->close();
		if (_ptrOHelper)
		{
			_ptrOHelper->flush();
			_ptrOHelper->close();
		}
		_ptrOBuf = 0;
		poco_assert (*_pOstr);
		// write an extra datablock if required
		// or fix the crc entries
		if (_pHeader->searchCRCAndSizesAfterData())
		{
			ZipDataInfo info;
			info.setCRC32(_crc32.checksum());
			info.setUncompressedSize(_bytesWritten);
			info.setCompressedSize(static_cast<Poco::UInt32>(_ptrOHelper->bytesWritten()));
			_pOstr->write(info.getRawHeader(), static_cast<std::streamsize>(info.getFullHeaderSize()));
		}
		else
		{
			poco_check_ptr (_pHeader);
			_pHeader->setCRC(_crc32.checksum());
			_pHeader->setUncompressedSize(_bytesWritten);
			_pHeader->setCompressedSize(static_cast<Poco::UInt32>(_ptrOHelper->bytesWritten()));
			_pOstr->seekp(_pHeader->getStartPos(), std::ios_base::beg);
			poco_assert (*_pOstr);
			std::string header = _pHeader->createHeader();
			_pOstr->write(header.c_str(), static_cast<std::streamsize>(header.size()));
			_pOstr->seekp(0, std::ios_base::end);
			poco_assert (*_pOstr);
		}
		_pHeader = 0;
	}
}


bool ZipStreamBuf::crcValid() const
{
	if (!_ptrBuf) return true; // directory entry
	return _crc32.checksum() == _expectedCrc32;
}


ZipIOS::ZipIOS(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition): _buf(istr, fileEntry, reposition)
{
	poco_ios_init(&_buf);
}


ZipIOS::ZipIOS(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool reposition): _buf(ostr, fileEntry, reposition)
{
	poco_ios_init(&_buf);
}


ZipIOS::~ZipIOS()
{
}


ZipStreamBuf* ZipIOS::rdbuf()
{
	return &_buf;
}


ZipInputStream::ZipInputStream(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition): ZipIOS(istr, fileEntry, reposition), std::istream(&_buf)
{
}


ZipInputStream::~ZipInputStream()
{
}


bool ZipInputStream::crcValid() const
{
	return _buf.crcValid();
}


ZipOutputStream::ZipOutputStream(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool seekableOutput): ZipIOS(ostr, fileEntry, seekableOutput), std::ostream(&_buf)
{
}


ZipOutputStream::~ZipOutputStream()
{
}


void ZipOutputStream::close()
{
	flush();
	_buf.close();
}


} } // namespace Poco::Zip
