//
// ZipArchive.cpp
//
// Library: Zip
// Package: Zip
// Module:	ZipArchive
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier: BSL-1.0
//


#include "Poco/Zip/ZipArchive.h"
#include "Poco/Zip/SkipCallback.h"
#include "Poco/Exception.h"
#include <cstring>


namespace Poco {
namespace Zip {


const std::string ZipArchive::EMPTY_COMMENT;


ZipArchive::ZipArchive(std::istream& in):
	_entries(),
	_infos(),
	_disks(),
	_disks64()
{
	poco_assert_dbg (in);
	SkipCallback skip;
	parse(in, skip);
}


ZipArchive::ZipArchive(const FileHeaders& entries, const FileInfos& infos, const DirectoryInfos& dirs, const DirectoryInfos64& dirs64):
	_entries(entries),
	_infos(infos),
	_disks(dirs),
	_disks64(dirs64)
{
}


ZipArchive::ZipArchive(std::istream& in, ParseCallback& pc):
	_entries(),
	_infos(),
	_disks(),
	_disks64()
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
	bool haveSynced = false;
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
			haveSynced = false;
		}
		else if (std::memcmp(header, ZipFileInfo::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipFileInfo info(in, true);
			FileHeaders::iterator it = _entries.find(info.getFileName());
			if (it != _entries.end())
			{
				it->second.setStartPos(info.getOffset());
			}
			poco_assert (_infos.insert(std::make_pair(info.getFileName(), info)).second);
			haveSynced = false;
		}
		else if (std::memcmp(header, ZipArchiveInfo::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipArchiveInfo nfo(in, true);
			poco_assert (_disks.insert(std::make_pair(nfo.getDiskNumber(), nfo)).second);
			haveSynced = false;
		}
		else if (std::memcmp(header, ZipArchiveInfo64::HEADER, ZipCommon::HEADER_SIZE) == 0)
		{
			ZipArchiveInfo64 nfo(in, true);
			poco_assert (_disks64.insert(std::make_pair(nfo.getDiskNumber(), nfo)).second);
			haveSynced = false;
		}
		else
		{
			if (!haveSynced)
			{
				// Some Zip files have extra data behind the ZipLocalFileHeader.
				// Try to re-sync.
				ZipUtil::sync(in);
				haveSynced = true;
			}
			else
			{
				if (_disks.empty() && _disks64.empty())
					throw Poco::IllegalStateException("Illegal header in zip file");
				else
					throw Poco::IllegalStateException("Garbage after directory header");
			}
		}
	}
}


const std::string& ZipArchive::getZipComment() const
{
	// It seems that only the "first" disk is populated (look at Compress::close()), so getting the first ZipArchiveInfo
	DirectoryInfos::const_iterator it = _disks.begin();
	if (it != _disks.end())
	{
		return it->second.getZipComment();
	}
	else
	{
		DirectoryInfos64::const_iterator it64 = _disks64.begin();
		if (it64 != _disks64.end())
			return it->second.getZipComment();
		else
			return EMPTY_COMMENT;
	}
}


} } // namespace Poco::Zip
