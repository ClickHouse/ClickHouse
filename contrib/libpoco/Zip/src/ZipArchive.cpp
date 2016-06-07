//
// ZipArchive.cpp
//
// $Id: //poco/1.4/Zip/src/ZipArchive.cpp#1 $
//
// Library: Zip
// Package: Zip
// Module:  ZipArchive
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/SkipCallback.h"
#include "Poco/Exception.h"
#include <cstring>


namespace Poco {
namespace Zip {


ZipArchive::ZipArchive(std::istream& in):
	_entries(),
	_infos(),
	_disks()
{
	poco_assert_dbg (in);
	SkipCallback skip;
	parse(in, skip);
}


ZipArchive::ZipArchive(const FileHeaders& entries, const FileInfos& infos, const DirectoryInfos& dirs):
	_entries(entries),
	_infos(infos),
	_disks(dirs)
{
}


ZipArchive::ZipArchive(std::istream& in, ParseCallback& pc):
	_entries(),
	_infos(),
	_disks()
{
	poco_assert_dbg (in);
	parse(in, pc);
}


ZipArchive::~ZipArchive()
{
}


void ZipArchive::parse(std::istream& in, ParseCallback& pc)
{
	// read 4 bytes
	while (in.good() && !in.eof())
	{
		char header[ZipCommon::HEADER_SIZE]={'\x00', '\x00', '\x00', '\x00'};
		in.read(header, ZipCommon::HEADER_SIZE);
		if (in.eof())
			return;
		if (std::memcmp(header, ZipLocalFileHeader::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipLocalFileHeader entry(in, true, pc);
			poco_assert (_entries.insert(std::make_pair(entry.getFileName(), entry)).second);
		}
		else if (std::memcmp(header, ZipFileInfo::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipFileInfo info(in, true);
			FileHeaders::iterator it = _entries.find(info.getFileName());
			if (it != _entries.end())
			{
				it->second.setStartPos(info.getRelativeOffsetOfLocalHeader());
			}
			poco_assert (_infos.insert(std::make_pair(info.getFileName(), info)).second);
		}
		else if (std::memcmp(header, ZipArchiveInfo::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipArchiveInfo nfo(in, true);
			poco_assert (_disks.insert(std::make_pair(nfo.getDiskNumber(), nfo)).second);
		}
		else
		{
			if (_disks.empty())
				throw Poco::IllegalStateException("Illegal header in zip file");
			else
				throw Poco::IllegalStateException("Garbage after directory header");
		}
	}
}


const std::string& ZipArchive::getZipComment() const
{
	// It seems that only the "first" disk is populated (look at Compress::close()), so getting the first ZipArchiveInfo
	DirectoryInfos::const_iterator it = _disks.begin();
	return it->second.getZipComment();
}


} } // namespace Poco::Zip
