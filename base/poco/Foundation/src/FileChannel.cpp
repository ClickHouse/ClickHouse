//
// FileChannel.cpp
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


#include "Poco/FileChannel.h"
#include "Poco/ArchiveStrategy.h"
#include "Poco/RotateStrategy.h"
#include "Poco/PurgeStrategy.h"
#include "Poco/Message.h"
#include "Poco/NumberParser.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTime.h"
#include "Poco/LocalDateTime.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include "Poco/Ascii.h"


namespace Poco {


const std::string FileChannel::PROP_PATH           = "path";
const std::string FileChannel::PROP_ROTATION       = "rotation";
const std::string FileChannel::PROP_ARCHIVE        = "archive";
const std::string FileChannel::PROP_TIMES          = "times";
const std::string FileChannel::PROP_COMPRESS       = "compress";
const std::string FileChannel::PROP_STREAMCOMPRESS = "streamCompress";
const std::string FileChannel::PROP_PURGEAGE       = "purgeAge";
const std::string FileChannel::PROP_PURGECOUNT     = "purgeCount";
const std::string FileChannel::PROP_FLUSH          = "flush";
const std::string FileChannel::PROP_ROTATEONOPEN   = "rotateOnOpen";

FileChannel::FileChannel(): 
	_times("utc"),
	_compress(false),
	_streamCompress(false),
	_flush(true),
	_rotateOnOpen(false),
	_pFile(0),
	_pRotateStrategy(0),
	_pArchiveStrategy(new ArchiveByNumberStrategy),
	_pPurgeStrategy(0)
{
}


FileChannel::FileChannel(const std::string& path):
	_path(path),
	_times("utc"),
	_compress(false),
	_streamCompress(false),
	_flush(true),
	_rotateOnOpen(false),
	_pFile(0),
	_pRotateStrategy(0),
	_pArchiveStrategy(new ArchiveByNumberStrategy),
	_pPurgeStrategy(0)
{
}


FileChannel::~FileChannel()
{
	try
	{
		close();
		delete _pRotateStrategy;
		delete _pArchiveStrategy;
		delete _pPurgeStrategy;
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void FileChannel::open()
{
	FastMutex::ScopedLock lock(_mutex);
	
	unsafeOpen();
}


void FileChannel::close()
{
	FastMutex::ScopedLock lock(_mutex);

	delete _pFile;
	_pFile = 0;
}


void FileChannel::log(const Message& msg)
{
	FastMutex::ScopedLock lock(_mutex);

	unsafeOpen();

	if (_pRotateStrategy && _pArchiveStrategy && _pRotateStrategy->mustRotate(_pFile))
	{
		try
		{
			_pFile = _pArchiveStrategy->archive(_pFile, _streamCompress);
			purge();
		}
		catch (...)
		{
			_pFile = newLogFile();
		}
		// we must call mustRotate() again to give the
		// RotateByIntervalStrategy a chance to write its timestamp
		// to the new file.
		_pRotateStrategy->mustRotate(_pFile);
	}

	try
	{
	_pFile->write(msg.getText(), _flush);
    }
    catch (const WriteFileException & e)
    {
        // In case of no space left on device,
        // we try to purge old files or truncate current file.

        // NOTE: error reason is not preserved in WriteFileException, we need to check errno manually.
        // NOTE: other reasons like quota exceeded are not handled.
        // NOTE: current log message will be lost.

        if (errno == ENOSPC)
        {
            PurgeOneFileStrategy().purge(_path);
        }
    }
}

	
void FileChannel::setProperty(const std::string& name, const std::string& value)
{
	FastMutex::ScopedLock lock(_mutex);

	if (name == PROP_TIMES)
	{
		_times = value;

		if (!_rotation.empty())
			setRotation(_rotation);

		if (!_archive.empty())
			setArchive(_archive);
	}
	else if (name == PROP_PATH)
		_path = value;
	else if (name == PROP_ROTATION)
		setRotation(value);
	else if (name == PROP_ARCHIVE)
		setArchive(value);
	else if (name == PROP_COMPRESS)
		setCompress(value);
	else if (name == PROP_STREAMCOMPRESS)
		setStreamCompress(value);
	else if (name == PROP_PURGEAGE)
		setPurgeAge(value);
	else if (name == PROP_PURGECOUNT)
		setPurgeCount(value);
	else if (name == PROP_FLUSH)
		setFlush(value);
	else if (name == PROP_ROTATEONOPEN)
		setRotateOnOpen(value);
	else
		Channel::setProperty(name, value);
}


std::string FileChannel::getProperty(const std::string& name) const
{
	if (name == PROP_TIMES)
		return _times;
	else if (name == PROP_PATH)
		return _path;
	else if (name == PROP_ROTATION)
		return _rotation;
	else if (name == PROP_ARCHIVE)
		return _archive;
	else if (name == PROP_COMPRESS)
		return std::string(_compress ? "true" : "false");
	else if (name == PROP_STREAMCOMPRESS)
		return std::string(_streamCompress ? "true" : "false");
	else if (name == PROP_PURGEAGE)
		return _purgeAge;
	else if (name == PROP_PURGECOUNT)
		return _purgeCount;
	else if (name == PROP_FLUSH)
		return std::string(_flush ? "true" : "false");
	else if (name == PROP_ROTATEONOPEN)
		return std::string(_rotateOnOpen ? "true" : "false");
	else
		return Channel::getProperty(name);
}


Timestamp FileChannel::creationDate() const
{
	if (_pFile)
		return _pFile->creationDate();
	else
		return 0;
}

	
UInt64 FileChannel::size() const
{
	if (_pFile)
		return _pFile->size();
	else
		return 0;
}


const std::string& FileChannel::path() const
{
	return _path;
}


void FileChannel::setRotation(const std::string& rotation)
{
	std::string::const_iterator it  = rotation.begin();
	std::string::const_iterator end = rotation.end();
	UInt64 n = 0;
	while (it != end && Ascii::isSpace(*it)) ++it;
	while (it != end && Ascii::isDigit(*it)) { n *= 10; n += *it++ - '0'; }
	while (it != end && Ascii::isSpace(*it)) ++it;
	std::string unit;
	while (it != end && Ascii::isAlpha(*it)) unit += *it++;
	
	RotateStrategy* pStrategy = 0;
	if ((rotation.find(',') != std::string::npos) || (rotation.find(':') != std::string::npos))
	{
		if (_times == "utc")
			pStrategy = new RotateAtTimeStrategy<DateTime>(rotation);
		else if (_times == "local")
			pStrategy = new RotateAtTimeStrategy<LocalDateTime>(rotation);
		else
			throw PropertyNotSupportedException("times", _times);
	}
	else if (unit == "daily")
		pStrategy = new RotateByIntervalStrategy(Timespan(1*Timespan::DAYS));
	else if (unit == "weekly")
		pStrategy = new RotateByIntervalStrategy(Timespan(7*Timespan::DAYS));
	else if (unit == "monthly")
		pStrategy = new RotateByIntervalStrategy(Timespan(30*Timespan::DAYS));
	else if (unit == "seconds") // for testing only
		pStrategy = new RotateByIntervalStrategy(Timespan(n*Timespan::SECONDS));
	else if (unit == "minutes")
		pStrategy = new RotateByIntervalStrategy(Timespan(n*Timespan::MINUTES));
	else if (unit == "hours")
		pStrategy = new RotateByIntervalStrategy(Timespan(n*Timespan::HOURS));
	else if (unit == "days")
		pStrategy = new RotateByIntervalStrategy(Timespan(n*Timespan::DAYS));
	else if (unit == "weeks")
		pStrategy = new RotateByIntervalStrategy(Timespan(n*7*Timespan::DAYS));
	else if (unit == "months")
		pStrategy = new RotateByIntervalStrategy(Timespan(n*30*Timespan::DAYS));
	else if (unit == "K")
		pStrategy = new RotateBySizeStrategy(n*1024);
	else if (unit == "M")
		pStrategy = new RotateBySizeStrategy(n*1024*1024);
	else if (unit == "G")
		pStrategy = new RotateBySizeStrategy(n*1024*1024*1024);
	else if (unit.empty())
		pStrategy = new RotateBySizeStrategy(n);
	else if (unit != "never")
		throw InvalidArgumentException("rotation", rotation);
	delete _pRotateStrategy;
	_pRotateStrategy = pStrategy;
	_rotation = rotation;
}


void FileChannel::setArchive(const std::string& archive)
{
	ArchiveStrategy* pStrategy = 0;
	if (archive == "number")
	{
		pStrategy = new ArchiveByNumberStrategy;
	}
	else if (archive == "timestamp")
	{
		if (_times == "utc")
			pStrategy = new ArchiveByTimestampStrategy<DateTime>;
		else if (_times == "local")
			pStrategy = new ArchiveByTimestampStrategy<LocalDateTime>;
		else
			throw PropertyNotSupportedException("times", _times);
	}
	else throw InvalidArgumentException("archive", archive);
	delete _pArchiveStrategy;
	pStrategy->compress(_compress && !_streamCompress);
	_pArchiveStrategy = pStrategy;
	_archive = archive;
}


void FileChannel::setCompress(const std::string& compress)
{
	_compress = icompare(compress, "true") == 0;
	if (_pArchiveStrategy)
		_pArchiveStrategy->compress(_compress && !_streamCompress);
}


void FileChannel::setStreamCompress(const std::string& streamCompress)
{
	_streamCompress = icompare(streamCompress, "true") == 0;

	if (_pArchiveStrategy)
		_pArchiveStrategy->compress(_compress && !_streamCompress);
}


void FileChannel::setPurgeAge(const std::string& age)
{
	if (setNoPurge(age)) return;

	std::string::const_iterator nextToDigit;
	int num = extractDigit(age, &nextToDigit);
	Timespan::TimeDiff factor = extractFactor(age, nextToDigit);

	setPurgeStrategy(new PurgeByAgeStrategy(Timespan(num * factor)));
	_purgeAge = age;
}


void FileChannel::setPurgeCount(const std::string& count)
{
	if (setNoPurge(count)) return;

	setPurgeStrategy(new PurgeByCountStrategy(extractDigit(count)));
	_purgeCount = count;
}


void FileChannel::setFlush(const std::string& flush)
{
	_flush = icompare(flush, "true") == 0;
}


void FileChannel::setRotateOnOpen(const std::string& rotateOnOpen)
{
	_rotateOnOpen = icompare(rotateOnOpen, "true") == 0;
}


void FileChannel::purge()
{
	if (_pPurgeStrategy)
	{
		try
		{
			_pPurgeStrategy->purge(_path);
		}
		catch (...)
		{
		}
	}
}


void FileChannel::unsafeOpen()
{
	if (!_pFile)
	{
		_pFile = newLogFile();
		if (_rotateOnOpen && _pFile->size() > 0)
		{
			try
			{
				_pFile = _pArchiveStrategy->archive(_pFile, _streamCompress);
				purge();
			}
			catch (...)
			{
				_pFile = newLogFile();
			}
		}
	}
}


void archiveByNumber(const std::string& basePath)
	/// A monotonic increasing number is appended to the
	/// log file name. The most recent archived file
	/// always has the number zero.
{
	std::string base = basePath;
	std::string ext = "";

	if (base.ends_with(".lz4"))
	{
		base.resize(base.size() - 4);
		ext = ".lz4";
	}
	
	int n = -1;
	std::string path;
	File f;
	do
	{
		path = base;
		path.append(".");
		NumberFormatter::append(path, ++n);
		path.append(ext);
		f = path;
	}
	while (f.exists());
	
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
		f = oldPath;
		if (f.exists())
			f.renameTo(newPath);
		--n;
	}
}


void FileChannel::archiveCorrupted(const std::string& path)
{
	Poco::File file(path + ".lz4");
	if (file.exists())
	{
		Poco::File::FileSize size = file.getSize();
		if (size > 0)
		{
			bool err = false;

			if (size < 4)
			{
				err = true;
			}
			else
			{
				Poco::Buffer<char> buffer(4);
				Poco::FileInputStream istr(path + ".lz4");
				istr.seekg(-4, std::ios_base::end);
				istr.read(buffer.begin(), 4);

				if (istr.gcount() != size)
					err = true;

				if (std::memcmp(buffer.begin(), "\0\0\0\0", 4) != 0)
					err = true;
			}

			if (err)
			{
				file.renameTo(path + ".incomplete.lz4");
				archiveByNumber(path + ".incomplete.lz4");
			}
				
		}
	}
}

LogFile * FileChannel::newLogFile()
{
	if (_streamCompress) {
		archiveCorrupted(_path);
		return new CompressedLogFile(_path);
	}
	else
	{
		return new LogFile(_path);
	}
}


bool FileChannel::setNoPurge(const std::string& value)
{
	if (value.empty() || 0 == icompare(value, "none"))
	{
		delete _pPurgeStrategy;
		_pPurgeStrategy = 0;
		_purgeAge = "none";
		return true;
	}
	else return false;
}


int FileChannel::extractDigit(const std::string& value, std::string::const_iterator* nextToDigit) const
{
	std::string::const_iterator it  = value.begin();
	std::string::const_iterator end = value.end();
	int digit 						= 0;

	while (it != end && Ascii::isSpace(*it)) ++it;
	while (it != end && Ascii::isDigit(*it))
	{ 
		digit *= 10;
		digit += *it++ - '0';
	}

	if (digit == 0)
		throw InvalidArgumentException("Zero is not valid purge age.");	

	if (nextToDigit) *nextToDigit = it;
	return digit;
}


void FileChannel::setPurgeStrategy(PurgeStrategy* strategy)
{
	delete _pPurgeStrategy;
	_pPurgeStrategy = strategy;
}


Timespan::TimeDiff FileChannel::extractFactor(const std::string& value, std::string::const_iterator start) const
{
	while (start != value.end() && Ascii::isSpace(*start)) ++start;

	std::string unit;
	while (start != value.end() && Ascii::isAlpha(*start)) unit += *start++;
 
	if (unit == "seconds")
		return Timespan::SECONDS;
	if (unit == "minutes")
		return Timespan::MINUTES;
	else if (unit == "hours")
		return Timespan::HOURS;
	else if (unit == "days")
		return Timespan::DAYS;
	else if (unit == "weeks")
		return 7 * Timespan::DAYS;
	else if (unit == "months")
		return 30 * Timespan::DAYS;
	else throw InvalidArgumentException("purgeAge", value);

	return Timespan::TimeDiff();
}



} // namespace Poco
