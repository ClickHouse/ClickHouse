//
// Compress.cpp
//
// Library: Zip
// Package: Zip
// Module:	Compress
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#include "Poco/Zip/Compress.h"
#include "Poco/Zip/ZipLocalFileHeader.h"
#include "Poco/Zip/ZipStream.h"
#include "Poco/Zip/ZipArchiveInfo.h"
#include "Poco/Zip/ZipDataInfo.h"
#include "Poco/Zip/ZipException.h"
#include "Poco/StreamCopier.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/String.h"


namespace Poco {
namespace Zip {


Compress::Compress(std::ostream& out, bool seekableOut, bool forceZip64):
	_out(out),
	_seekableOut(seekableOut),
	_forceZip64(forceZip64),
	_files(),
	_infos(),
	_dirs(),
	_offset(0)
{
	_storeExtensions.insert("gif");
	_storeExtensions.insert("png");
	_storeExtensions.insert("jpg");
	_storeExtensions.insert("jpeg");
}


Compress::~Compress()
{
}


void Compress::addEntry(std::istream& in, const Poco::DateTime& lastModifiedAt, const Poco::Path& fileName, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl)
{
	if (cm == ZipCommon::CM_AUTO)
	{
		std::string ext = Poco::toLower(fileName.getExtension());
		if (_storeExtensions.find(ext) != _storeExtensions.end())
		{
			cm = ZipCommon::CM_STORE;
			cl = ZipCommon::CL_NORMAL;
		}
		else
		{
			cm = ZipCommon::CM_DEFLATE;
		}
	}

	std::string fn = ZipUtil::validZipEntryFileName(fileName);

	if (!in.good())
		throw ZipException("Invalid input stream");

	// Check if stream is empty.
	// In this case, we have to set compression to STORE, otherwise
	// extraction will fail with various tools.
	const int eof = std::char_traits<char>::eof();
	int firstChar = in.get();
	if (firstChar == eof)
	{
		cm = ZipCommon::CM_STORE;
		cl = ZipCommon::CL_NORMAL;
	}

	std::streamoff localHeaderOffset = _offset;
	ZipLocalFileHeader hdr(fileName, lastModifiedAt, cm, cl, _forceZip64);
	hdr.setStartPos(localHeaderOffset);

	ZipOutputStream zipOut(_out, hdr, _seekableOut);
	if (firstChar != eof)
	{
		zipOut.put(static_cast<char>(firstChar));
		Poco::StreamCopier::copyStream(in, zipOut);
	}
	Poco::UInt64 extraDataSize;
	zipOut.close(extraDataSize);
	_offset = hdr.getEndPos();
	_offset += extraDataSize;
	_files.insert(std::make_pair(fileName.toString(Poco::Path::PATH_UNIX), hdr));
	if (!_out) throw Poco::IOException("Bad output stream");
	ZipFileInfo nfo(hdr);
	nfo.setOffset(localHeaderOffset);
	nfo.setZip64Data();
	_infos.insert(std::make_pair(fileName.toString(Poco::Path::PATH_UNIX), nfo));
	EDone.notify(this, hdr);
}


void Compress::addFileRaw(std::istream& in, const ZipLocalFileHeader& h, const Poco::Path& fileName)
{
	if (!in.good())
		throw ZipException("Invalid input stream");

	std::string fn = ZipUtil::validZipEntryFileName(fileName);
	//bypass the header of the input stream and point to the first byte of the data payload
	in.seekg(h.getDataStartPos(), std::ios_base::beg);
	if (!in.good()) throw Poco::IOException("Failed to seek on input stream");

	std::streamoff localHeaderOffset = _offset;
	ZipLocalFileHeader hdr(h);
	hdr.setFileName(fn, h.isDirectory());
	hdr.setStartPos(localHeaderOffset);
	if (hdr.needsZip64())
		hdr.setZip64Data();
	//bypass zipoutputstream
	//write the header directly
	std::string header = hdr.createHeader();
	_out.write(header.c_str(), static_cast<std::streamsize>(header.size()));
	// now fwd the payload to _out in chunks of size CHUNKSIZE
	Poco::UInt64 totalSize = hdr.getCompressedSize();
	if (totalSize > 0)
	{
		Poco::Buffer<char> buffer(COMPRESS_CHUNK_SIZE);
		Poco::UInt64 remaining = totalSize;
		while (remaining > 0)
		{
			if (remaining > COMPRESS_CHUNK_SIZE)
			{
				in.read(buffer.begin(), COMPRESS_CHUNK_SIZE);
				std::streamsize n = in.gcount();
				poco_assert_dbg (n == COMPRESS_CHUNK_SIZE);
				_out.write(buffer.begin(), n);
				remaining -= COMPRESS_CHUNK_SIZE;
			}
			else
			{
				in.read(buffer.begin(), remaining);
				std::streamsize n = in.gcount();
				poco_assert_dbg (n == remaining);
				_out.write(buffer.begin(), n);
				remaining = 0;
			}
		}
	}
	hdr.setStartPos(localHeaderOffset); // This resets EndPos now that compressed Size is known
	_offset = hdr.getEndPos();
	//write optional block afterwards
	if (hdr.searchCRCAndSizesAfterData())
	{
		if (hdr.needsZip64())
		{
			ZipDataInfo64 info(in, false);
			_out.write(info.getRawHeader(), static_cast<std::streamsize>(info.getFullHeaderSize()));
			_offset += ZipDataInfo::getFullHeaderSize();
		}
		else
		{
			ZipDataInfo info(in, false);
			_out.write(info.getRawHeader(), static_cast<std::streamsize>(info.getFullHeaderSize()));
			_offset += ZipDataInfo::getFullHeaderSize();
		}
	}
	else
	{
		if (hdr.hasExtraField())	 // Update sizes in header extension.
			hdr.setZip64Data();
		_out.seekp(hdr.getStartPos(), std::ios_base::beg);
		std::string header = hdr.createHeader();
		_out.write(header.c_str(), static_cast<std::streamsize>(header.size()));
		_out.seekp(0, std::ios_base::end);
	}

	_files.insert(std::make_pair(fileName.toString(Poco::Path::PATH_UNIX), hdr));
	if (!_out) throw Poco::IOException("Bad output stream");
	ZipFileInfo nfo(hdr);
	nfo.setOffset(localHeaderOffset);
	nfo.setZip64Data();
	_infos.insert(std::make_pair(fileName.toString(Poco::Path::PATH_UNIX), nfo));
	EDone.notify(this, hdr);
}


void Compress::addFile(std::istream& in, const Poco::DateTime& lastModifiedAt, const Poco::Path& fileName, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl)
{
	if (!fileName.isFile())
		throw ZipException("Not a file: "+ fileName.toString());

	if (fileName.depth() > 1)
	{
		addDirectory(fileName.parent(), lastModifiedAt);
	}
	addEntry(in, lastModifiedAt, fileName, cm, cl);
}


void Compress::addFile(const Poco::Path& file, const Poco::Path& fileName, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl)
{
	Poco::File aFile(file);
	Poco::FileInputStream in(file.toString());
	if (fileName.depth() > 1)
	{
		Poco::File aParent(file.parent());
		addDirectory(fileName.parent(), aParent.getLastModified());
	}
	addFile(in, aFile.getLastModified(), fileName, cm, cl);
}


void Compress::addDirectory(const Poco::Path& entryName, const Poco::DateTime& lastModifiedAt)
{
	if (!entryName.isDirectory())
		throw ZipException("Not a directory: "+ entryName.toString());

	std::string fileStr = entryName.toString(Poco::Path::PATH_UNIX);
	if (_files.find(fileStr) != _files.end())
		return; // ignore duplicate add
	if (fileStr == "/")
		throw ZipException("Illegal entry name /");
	if (fileStr.empty())
		throw ZipException("Illegal empty entry name");
	if (!ZipCommon::isValidPath(fileStr))
		throw ZipException("Illegal entry name " + fileStr + " containing parent directory reference");

	if (entryName.depth() > 1)
	{
		addDirectory(entryName.parent(), lastModifiedAt);
	}

	std::streamoff localHeaderOffset = _offset;
	ZipCommon::CompressionMethod cm = ZipCommon::CM_STORE;
	ZipCommon::CompressionLevel cl = ZipCommon::CL_NORMAL;
	ZipLocalFileHeader hdr(entryName, lastModifiedAt, cm, cl);
	hdr.setStartPos(localHeaderOffset);
	ZipOutputStream zipOut(_out, hdr, _seekableOut);
	Poco::UInt64 extraDataSize;
	zipOut.close(extraDataSize);
	hdr.setStartPos(localHeaderOffset); // reset again now that compressed Size is known
	_offset = hdr.getEndPos();
	if (hdr.searchCRCAndSizesAfterData())
		_offset += extraDataSize;
	_files.insert(std::make_pair(entryName.toString(Poco::Path::PATH_UNIX), hdr));
	if (!_out) throw Poco::IOException("Bad output stream");
	ZipFileInfo nfo(hdr);
	nfo.setOffset(localHeaderOffset);
	nfo.setZip64Data();
	_infos.insert(std::make_pair(entryName.toString(Poco::Path::PATH_UNIX), nfo));
	EDone.notify(this, hdr);
}


void Compress::addRecursive(const Poco::Path& entry, ZipCommon::CompressionLevel cl, bool excludeRoot, const Poco::Path& name)
{
	addRecursive(entry, ZipCommon::CM_DEFLATE, cl, excludeRoot, name);
}


void Compress::addRecursive(const Poco::Path& entry, ZipCommon::CompressionMethod cm, ZipCommon::CompressionLevel cl, bool excludeRoot, const Poco::Path& name)
{
	Poco::File aFile(entry);
	if (!aFile.isDirectory())
		throw ZipException("Not a directory: "+ entry.toString());
	Poco::Path aName(name);
	aName.makeDirectory();
	if (!excludeRoot)
	{
		if (aName.depth() == 0)
		{
			Poco::Path tmp(entry);
			tmp.makeAbsolute(); // eliminate ../
			aName = Poco::Path(tmp[tmp.depth()-1]);
			aName.makeDirectory();
		}

		addDirectory(aName, aFile.getLastModified());
	}

	// iterate over children
	std::vector<std::string> children;
	aFile.list(children);
	std::vector<std::string>::const_iterator it = children.begin();
	std::vector<std::string>::const_iterator itEnd = children.end();
	for (; it != itEnd; ++it)
	{
		Poco::Path realFile(entry, *it);
		Poco::Path renamedFile(aName, *it);
		Poco::File aFile(realFile);
		if (aFile.isDirectory())
		{
			realFile.makeDirectory();
			renamedFile.makeDirectory();
			addRecursive(realFile, cm, cl, false, renamedFile);
		}
		else
		{
			realFile.makeFile();
			renamedFile.makeFile();
			addFile(realFile, renamedFile, cm, cl);
		}
	}
}


ZipArchive Compress::close()
{
	if (!_dirs.empty() || ! _dirs64.empty())
		return ZipArchive(_files, _infos, _dirs, _dirs64);

	poco_assert (_infos.size() == _files.size());
	Poco::UInt64 centralDirSize64 = 0;
	Poco::UInt64 centralDirStart64 = _offset;
	// write all infos
	ZipArchive::FileInfos::const_iterator it = _infos.begin();
	ZipArchive::FileInfos::const_iterator itEnd = _infos.end();
	bool needZip64 = _forceZip64;
	needZip64 = needZip64  || _files.size() >= ZipCommon::ZIP64_MAGIC_SHORT || centralDirStart64 >= ZipCommon::ZIP64_MAGIC;
	for (; it != itEnd; ++it)
	{
		const ZipFileInfo& nfo = it->second;
		needZip64 = needZip64  || nfo.needsZip64();

		std::string info(nfo.createHeader());
		_out.write(info.c_str(), static_cast<std::streamsize>(info.size()));
		Poco::UInt32 entrySize = static_cast<Poco::UInt32>(info.size());
		centralDirSize64 += entrySize;
		_offset += entrySize;
	}
	if (!_out) throw Poco::IOException("Bad output stream");
	
	Poco::UInt64 numEntries64 = _infos.size();
	needZip64 = needZip64  || _offset >= ZipCommon::ZIP64_MAGIC;
	if (needZip64)
	{
		ZipArchiveInfo64 central;
		central.setCentralDirectorySize(centralDirSize64);
		central.setCentralDirectoryOffset(centralDirStart64);
		central.setNumberOfEntries(numEntries64);
		central.setTotalNumberOfEntries(numEntries64);
		central.setHeaderOffset(_offset);
		central.setTotalNumberOfDisks(1);
		std::string centr(central.createHeader());
		_out.write(centr.c_str(), static_cast<std::streamsize>(centr.size()));
		_out.flush();
		_offset += centr.size();
		_dirs64.insert(std::make_pair(0, central));
	}

	Poco::UInt16 numEntries = numEntries64 >= ZipCommon::ZIP64_MAGIC_SHORT ? ZipCommon::ZIP64_MAGIC_SHORT : static_cast<Poco::UInt16>(numEntries64);
	Poco::UInt32 centralDirStart = centralDirStart64 >= ZipCommon::ZIP64_MAGIC ? ZipCommon::ZIP64_MAGIC : static_cast<Poco::UInt32>(centralDirStart64);
	Poco::UInt32 centralDirSize = centralDirSize64 >= ZipCommon::ZIP64_MAGIC ? ZipCommon::ZIP64_MAGIC : static_cast<Poco::UInt32>(centralDirSize64);
	Poco::UInt32 offset = _offset >= ZipCommon::ZIP64_MAGIC ? ZipCommon::ZIP64_MAGIC : static_cast<Poco::UInt32>(_offset);
	ZipArchiveInfo central;
	central.setCentralDirectorySize(centralDirSize);
	central.setCentralDirectoryOffset(centralDirStart);
	central.setNumberOfEntries(numEntries);
	central.setTotalNumberOfEntries(numEntries);
	central.setHeaderOffset(offset);
	if (!_comment.empty() && _comment.size() <= 65535)
	{
		central.setZipComment(_comment);
	}
	std::string centr(central.createHeader());
	_out.write(centr.c_str(), static_cast<std::streamsize>(centr.size()));
	_out.flush();
	_offset += centr.size();
	_dirs.insert(std::make_pair(0, central));
	return ZipArchive(_files, _infos, _dirs, _dirs64);
}


void Compress::setStoreExtensions(const std::set<std::string>& extensions)
{
	_storeExtensions.clear();
	for (std::set<std::string>::const_iterator it = extensions.begin(); it != extensions.end(); ++it)
	{
		_storeExtensions.insert(Poco::toLower(*it));
	}
}


} } // namespace Poco::Zip
