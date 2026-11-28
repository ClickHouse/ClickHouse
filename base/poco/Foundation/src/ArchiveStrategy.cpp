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
#include <utility>


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


std::unique_ptr<LogFile> ArchiveStrategy::openFile(const std::string& base, bool streamCompress)
{
	if (streamCompress)
		return std::make_unique<CompressedLogFile>(base);
	return std::make_unique<LogFile>(base);
}


void ArchiveStrategy::rotateFile(
	std::unique_ptr<LogFile>& file,
	const std::string& base,
	const std::string& ext,
	const std::string& archived,
	bool streamCompress,
	const std::function<void()>& makeRoom)
{
	const std::string current = base + ext;
	if (!File(current).exists())
	{
		/// The file was removed from outside, so there is nothing to archive.
		file = openFile(base, streamCompress);
		return;
	}

	/// Open the new file first: if this fails, nothing is renamed and the current file stays as it is.
	const std::string nextBase = base + ".new.incomplete";
	File nextFile(nextBase + ext);
	if (nextFile.exists())
		nextFile.remove();
	std::unique_ptr<LogFile> next = openFile(nextBase, streamCompress);

	bool currentArchived = false;
	try
	{
		makeRoom();
		File(current).renameTo(archived);
		currentArchived = true;
		next->renameTo(current);
	}
	catch (...)
	{
		next.reset();
		try
		{
			nextFile.remove();
		}
		catch (...)
		{
			/// The original error is more useful; the next attempt removes the leftover.
		}
		if (currentArchived)
		{
			try
			{
				File(archived).renameTo(current);
			}
			catch (...)
			{
				/// The current file stays archived, so the caller must open a new one.
				file.reset();
			}
		}
		throw;
	}

	file = std::move(next);

	if (_compress)
	{
		if (!_pCompressor) _pCompressor = new ArchiveCompressor;
		_pCompressor->compress(archived);
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


void ArchiveByNumberStrategy::archive(std::unique_ptr<LogFile> & file, bool streamCompress)
{
	std::string base = file->path();
	std::string ext = "";

	if (base.ends_with(".lz4"))
	{
		base.resize(base.size() - 4);
		ext = ".lz4";
	}

	rotateFile(file, base, ext, base + ".0" + ext, streamCompress, [&]
	{
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

		while (n > 0)
		{
			std::string oldPath = base;
			oldPath.append(".");
			NumberFormatter::append(oldPath, n - 1);
			oldPath.append(ext);

			std::string newPath = base;
			newPath.append(".");
			NumberFormatter::append(newPath, n);
			newPath.append(ext);
			moveFile(oldPath, newPath);
			--n;
		}
	});
}


} // namespace Poco
