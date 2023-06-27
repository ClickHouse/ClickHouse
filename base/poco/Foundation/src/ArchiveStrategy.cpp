//
// ArchiveStrategy.cpp
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
	
	ActiveMethod<void, std::string, ArchiveCompressor, ActiveStarter<ActiveDispatcher> > compress;

protected:
	void compressImpl(const std::string& path)
	{
		std::string gzPath(path);
		gzPath.append(".gz");
		FileInputStream istr(path);
		FileOutputStream ostr(gzPath);
		try
		{
			DeflatingOutputStream deflater(ostr, DeflatingStreamBuf::STREAM_GZIP);
			StreamCopier::copyStream(istr, deflater);
			if (!deflater.good() || !ostr.good()) throw WriteFileException(gzPath);
			deflater.close();
			ostr.close();
			istr.close();
		}
		catch (Poco::Exception&)
		{
			// deflating failed - remove gz file and leave uncompressed log file
			ostr.close();
			Poco::File gzf(gzPath);
			gzf.remove();
			return;
		}
		File f(path);
		f.remove();
		return;
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


LogFile* ArchiveByNumberStrategy::archive(LogFile* pFile, bool streamCompress)
{
	std::string base = pFile->path();
	std::string ext = "";

	if (base.ends_with(".lz4"))
	{
		base.resize(base.size() - 4);
		ext = ".lz4";
	}

	delete pFile;
	int n = -1;
	std::string path;
	do
	{
		path = base;
		path.append(".");
		NumberFormatter::append(path, ++n);
		path.append(ext);
	}
	while (exists(path));
	
	while (n >= 0)
	{
		std::string oldPath = base;
		if (n > 0)
		{
			oldPath.append(".");
			NumberFormatter::append(oldPath, n - 1);
		}
		oldPath.append(ext);

		std::string newPath = base;
		newPath.append(".");
		NumberFormatter::append(newPath, n);
		newPath.append(ext);
		moveFile(oldPath, newPath);
		--n;
	}

	if (streamCompress)
		return new CompressedLogFile(base);
	else
		return new LogFile(base);
}


} // namespace Poco
