//
// ArchiveStrategy.cpp
//
// $Id: //poco/1.4/Foundation/src/ArchiveStrategy.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  FileChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/ArchiveStrategy.h"
#include "Poco/NumberFormatter.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/DeflatingStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/Exception.h"
#include "Poco/ActiveDispatcher.h"
#include "Poco/ActiveMethod.h"
#include "Poco/Void.h"
#include "Poco/FileStream.h"


namespace Poco {


//
// ArchiveCompressor
//


class ArchiveCompressor: public ActiveDispatcher
{
public:
	ArchiveCompressor():
		compress(this, &ArchiveCompressor::compressImpl)
	{
	}
	
	~ArchiveCompressor()
	{
	}
	
	ActiveMethod<Void, std::string, ArchiveCompressor, ActiveStarter<ActiveDispatcher> > compress;

protected:
	Void compressImpl(const std::string& path)
	{
		std::string gzPath(path);
		gzPath.append(".gz");
		FileInputStream istr(path, std::ios::binary | std::ios::in);
		if (!istr.good()) throw OpenFileException(path);
		FileOutputStream ostr(gzPath, std::ios::binary | std::ios::out);
		if (ostr.good())
		{
			DeflatingOutputStream deflater(ostr, DeflatingStreamBuf::STREAM_GZIP);
			StreamCopier::copyStream(istr, deflater);
			deflater.close();
			ostr.close();
			istr.close();
			File f(path);
			f.remove();
		}
		else throw CreateFileException(gzPath);
		return Void();
	}
};


//
// ArchiveStrategy
//


ArchiveStrategy::ArchiveStrategy(): 
	_compress(false),
	_pCompressor(0)
{
}


ArchiveStrategy::~ArchiveStrategy()
{
	delete _pCompressor;
}


void ArchiveStrategy::compress(bool flag)
{
	_compress = flag;
}


void ArchiveStrategy::moveFile(const std::string& oldPath, const std::string& newPath)
{
	bool compressed = false;
	Path p(oldPath);
	File f(oldPath);
	if (!f.exists())
	{
		f = oldPath + ".gz";
		compressed = true;
	}
	std::string mvPath(newPath);
	if (_compress || compressed)
		mvPath.append(".gz");
	if (!_compress || compressed)
	{
		f.renameTo(mvPath);
	}
	else
	{
		f.renameTo(newPath);
		if (!_pCompressor) _pCompressor = new ArchiveCompressor;
		_pCompressor->compress(newPath);
	}
}


bool ArchiveStrategy::exists(const std::string& name)
{
	File f(name);
	if (f.exists())
	{
		return true;
	}
	else if (_compress)
	{
		std::string gzName(name);
		gzName.append(".gz");
		File gzf(gzName);
		return gzf.exists();
	}
	else return false;
}


//
// ArchiveByNumberStrategy
//


ArchiveByNumberStrategy::ArchiveByNumberStrategy()
{
}


ArchiveByNumberStrategy::~ArchiveByNumberStrategy()
{
}


LogFile* ArchiveByNumberStrategy::archive(LogFile* pFile)
{
	std::string basePath = pFile->path();
	delete pFile;
	int n = -1;
	std::string path;
	do
	{
		path = basePath;
		path.append(".");
		NumberFormatter::append(path, ++n);
	}
	while (exists(path));
	
	while (n >= 0)
	{
		std::string oldPath = basePath;
		if (n > 0)
		{
			oldPath.append(".");
			NumberFormatter::append(oldPath, n - 1);
		}
		std::string newPath = basePath;
		newPath.append(".");
		NumberFormatter::append(newPath, n);
		moveFile(oldPath, newPath);
		--n;
	}
	return new LogFile(basePath);
}


} // namespace Poco
