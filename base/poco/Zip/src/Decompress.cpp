//
// Decompress.cpp
//
// Library: Zip
// Package: Zip
// Module:  Decompress
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/Decompress.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/ZipStream.h"
#include "Poco/Zip/ZipException.h"
#include "Poco/File.h"
#include "Poco/Exception.h"
#include "Poco/StreamCopier.h"
#include "Poco/Delegate.h"
#include "Poco/FileStream.h"
#include "Poco/Format.h"


namespace Poco {
namespace Zip {


Decompress::Decompress(std::istream& in, const Poco::Path& outputDir, bool flattenDirs, bool keepIncompleteFiles):
	_in(in),
	_outDir(outputDir),
	_flattenDirs(flattenDirs),
	_keepIncompleteFiles(keepIncompleteFiles),
	_mapping()
{
	_outDir.makeAbsolute();
	_outDir.makeDirectory();
	if (!_in.good()) throw Poco::IOException("Bad input stream");
	Poco::File tmp(_outDir);
	if (!tmp.exists())
	{
		tmp.createDirectories();
	}
	if (!tmp.isDirectory())
		throw Poco::IOException("Failed to create/open directory: " + _outDir.toString());
	EOk += Poco::Delegate<Decompress, std::pair<const ZipLocalFileHeader, const Poco::Path> >(this, &Decompress::onOk);

}


Decompress::~Decompress()
{
	try
	{
		EOk -= Poco::Delegate<Decompress, std::pair<const ZipLocalFileHeader, const Poco::Path> >(this, &Decompress::onOk);
	}
	catch (...)
	{
		poco_unexpected();
	}
}


ZipArchive Decompress::decompressAllFiles()
{
	poco_assert (_mapping.empty());
	ZipArchive arch(_in, *this);
	return arch;
}


bool Decompress::handleZipEntry(std::istream& zipStream, const ZipLocalFileHeader& hdr)
{
	if (hdr.isDirectory())
	{
		// directory have 0 size, nth to read
		if (!_flattenDirs)
		{
			std::string dirName = hdr.getFileName();
			if (!ZipCommon::isValidPath(dirName))
				throw ZipException("Illegal entry name", dirName);
			Poco::Path dir(_outDir, dirName);
			dir.makeDirectory();
			Poco::File aFile(dir);
			aFile.createDirectories();
		}
		if (hdr.getCompressionMethod() == ZipCommon::CM_DEFLATE)
		{
			// If directory is stored with deflate method, two extra bytes
			// (the result of deflating a zero-length sequence) must be read.
			char buffer[2];
			zipStream.read(buffer, 2);
		}
		return true;
	}
	try
	{
		std::string fileName = hdr.getFileName();
		if (_flattenDirs)
		{
			// remove path info
			Poco::Path p(fileName);
			p.makeFile();
			fileName = p.getFileName();
		}

		if (!ZipCommon::isValidPath(fileName))
		{
			throw ZipException("Illegal entry name", fileName);
		}

		if (!hdr.hasSupportedCompressionMethod())
		{
			throw ZipException(Poco::format("Unsupported compression method (%d)", static_cast<int>(hdr.getCompressionMethod())), fileName);
		}

		Poco::Path file(fileName);
		file.makeFile();
		Poco::Path dest(_outDir, file);
		dest.makeFile();
		if (dest.depth() > 0)
		{
			Poco::File aFile(dest.parent());
			aFile.createDirectories();
		}
		Poco::FileOutputStream out(dest.toString());
		ZipInputStream inp(zipStream, hdr, false);
		Poco::StreamCopier::copyStream(inp, out);
		out.close();
		Poco::File aFile(dest.toString());
		if (!aFile.exists() || !aFile.isFile())
		{
			std::pair<const ZipLocalFileHeader, const std::string> tmp = std::make_pair(hdr, "Failed to create output stream " + dest.toString());
			EError.notify(this, tmp);
			return false;
		}

		if (!inp.crcValid())
		{
			if (!_keepIncompleteFiles)
				aFile.remove();
			std::pair<const ZipLocalFileHeader, const std::string> tmp = std::make_pair(hdr, "CRC mismatch. Corrupt file: " + dest.toString());
			EError.notify(this, tmp);
			return false;
		}

		// cannot check against hdr.getUnCompressedSize if CRC and size are not set in hdr but in a ZipDataInfo
		// crc is typically enough to detect errors
		if (aFile.getSize() != hdr.getUncompressedSize() && !hdr.searchCRCAndSizesAfterData())
		{
			if (!_keepIncompleteFiles)
				aFile.remove();
			std::pair<const ZipLocalFileHeader, const std::string> tmp = std::make_pair(hdr, "Filesizes do not match. Corrupt file: " + dest.toString());
			EError.notify(this, tmp);
			return false;
		}

		std::pair<const ZipLocalFileHeader, const Poco::Path> tmp = std::make_pair(hdr, file);
		EOk.notify(this, tmp);
	}
	catch (Poco::Exception& e)
	{
		std::pair<const ZipLocalFileHeader, const std::string> tmp = std::make_pair(hdr, std::string("Exception: " + e.displayText()));
		EError.notify(this, tmp);
		return false;
	}
	catch (...)
	{
		std::pair<const ZipLocalFileHeader, const std::string> tmp = std::make_pair(hdr, std::string("Unknown Exception"));
		EError.notify(this, tmp);
		return false;
	}

	return true;
}


void Decompress::onOk(const void*, std::pair<const ZipLocalFileHeader, const Poco::Path>& val)
{
	_mapping.insert(std::make_pair(val.first.getFileName(), val.second));
}


} } // namespace Poco::Zip
