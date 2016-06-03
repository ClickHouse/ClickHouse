//
// File_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/File_VX.cpp#1 $
//
// Library: Foundation
// Package: Filesystem
// Module:  File
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/File_VX.h"
#include "Poco/Buffer.h"
#include "Poco/Exception.h"
#include <algorithm>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <utime.h>
#include <cstring>


namespace Poco {


FileImpl::FileImpl()
{
}


FileImpl::FileImpl(const std::string& path): _path(path)
{
	std::string::size_type n = _path.size();
	if (n > 1 && _path[n - 1] == '/')
		_path.resize(n - 1);
}


FileImpl::~FileImpl()
{
}


void FileImpl::swapImpl(FileImpl& file)
{
	std::swap(_path, file._path);
}


void FileImpl::setPathImpl(const std::string& path)
{
	_path = path;
	std::string::size_type n = _path.size();
	if (n > 1 && _path[n - 1] == '/')
		_path.resize(n - 1);
}


bool FileImpl::existsImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	return stat(const_cast<char*>(_path.c_str()), &st) == 0;
}


bool FileImpl::canReadImpl() const
{
	poco_assert (!_path.empty());

	return true;
}


bool FileImpl::canWriteImpl() const
{
	poco_assert (!_path.empty());

	return true;
}


bool FileImpl::canExecuteImpl() const
{
	return false;
}


bool FileImpl::isFileImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return S_ISREG(st.st_mode);
	else
		handleLastErrorImpl(_path);
	return false;
}


bool FileImpl::isDirectoryImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return S_ISDIR(st.st_mode);
	else
		handleLastErrorImpl(_path);
	return false;
}


bool FileImpl::isLinkImpl() const
{
	return false;
}


bool FileImpl::isDeviceImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return S_ISCHR(st.st_mode) || S_ISBLK(st.st_mode);
	else
		handleLastErrorImpl(_path);
	return false;
}


bool FileImpl::isHiddenImpl() const
{
	poco_assert (!_path.empty());
	Path p(_path);
	p.makeFile();

	return p.getFileName()[0] == '.';
}


Timestamp FileImpl::createdImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return Timestamp::fromEpochTime(st.st_ctime);
	else
		handleLastErrorImpl(_path);
	return 0;
}


Timestamp FileImpl::getLastModifiedImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return Timestamp::fromEpochTime(st.st_mtime);
	else
		handleLastErrorImpl(_path);
	return 0;
}


void FileImpl::setLastModifiedImpl(const Timestamp& ts)
{
	poco_assert (!_path.empty());

	struct utimbuf tb;
	tb.actime  = ts.epochTime();
	tb.modtime = ts.epochTime();
	if (utime(const_cast<char*>(_path.c_str()), &tb) != 0)
		handleLastErrorImpl(_path);
}


FileImpl::FileSizeImpl FileImpl::getSizeImpl() const
{
	poco_assert (!_path.empty());

	struct stat st;
	if (stat(const_cast<char*>(_path.c_str()), &st) == 0)
		return st.st_size;
	else
		handleLastErrorImpl(_path);
	return 0;
}


void FileImpl::setSizeImpl(FileSizeImpl size)
{
	poco_assert (!_path.empty());

	int fd = open(_path.c_str(), O_WRONLY, S_IRWXU);
	if (fd != -1)
	{
		try
		{
			if (ftruncate(fd, size) != 0)
				handleLastErrorImpl(_path);
		}
		catch (...)
		{
			close(fd);
			throw;
		}
	}
}


void FileImpl::setWriteableImpl(bool flag)
{
	poco_assert (!_path.empty());
}


void FileImpl::setExecutableImpl(bool flag)
{
	poco_assert (!_path.empty());
}


void FileImpl::copyToImpl(const std::string& path) const
{
	poco_assert (!_path.empty());

	int sd = open(_path.c_str(), O_RDONLY, 0);
	if (sd == -1) handleLastErrorImpl(_path);

	struct stat st;
	if (fstat(sd, &st) != 0) 
	{
		close(sd);
		handleLastErrorImpl(_path);
	}
	const long blockSize = st.st_blksize;

	int dd = open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, st.st_mode & S_IRWXU);
	if (dd == -1)
	{
		close(sd);
		handleLastErrorImpl(path);
	}
	Buffer<char> buffer(blockSize);
	try
	{
		int n;
		while ((n = read(sd, buffer.begin(), blockSize)) > 0)
		{
			if (write(dd, buffer.begin(), n) != n) 
				handleLastErrorImpl(path);
		}
		if (n < 0)
			handleLastErrorImpl(_path);
	}
	catch (...)
	{
		close(sd);
		close(dd);
		throw;
	}
	close(sd);
	close(dd);
}


void FileImpl::renameToImpl(const std::string& path)
{
	poco_assert (!_path.empty());

	if (rename(_path.c_str(), path.c_str()) != 0)
		handleLastErrorImpl(_path);
}


void FileImpl::removeImpl()
{
	poco_assert (!_path.empty());

	int rc;
	if (!isLinkImpl() && isDirectoryImpl())
		rc = rmdir(_path.c_str());
	else
		rc = unlink(const_cast<char*>(_path.c_str()));
	if (rc) handleLastErrorImpl(_path);
}


bool FileImpl::createFileImpl()
{
	poco_assert (!_path.empty());
	
	int n = open(_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
	if (n != -1)
	{
		close(n);
		return true;
	}
	if (n == -1 && errno == EEXIST)
		return false;
	else
		handleLastErrorImpl(_path);
	return false;
}


bool FileImpl::createDirectoryImpl()
{
	poco_assert (!_path.empty());

	if (existsImpl() && isDirectoryImpl())
		return false;
	if (mkdir(_path.c_str()) != 0) 
		handleLastErrorImpl(_path);
	return true;
}


void FileImpl::handleLastErrorImpl(const std::string& path)
{
	switch (errno)
	{
	case EIO:
		throw IOException(path);
	case EPERM:
		throw FileAccessDeniedException("insufficient permissions", path);
	case EACCES:
		throw FileAccessDeniedException(path);
	case ENOENT:
		throw FileNotFoundException(path);
	case ENOTDIR:
		throw OpenFileException("not a directory", path);
	case EISDIR:
		throw OpenFileException("not a file", path);
	case EROFS:
		throw FileReadOnlyException(path);
	case EEXIST:
		throw FileExistsException(path);
	case ENOSPC:
		throw FileException("no space left on device", path);
	case ENOTEMPTY:
		throw FileException("directory not empty", path);
	case ENAMETOOLONG:
		throw PathSyntaxException(path);
	case ENFILE:
	case EMFILE:
		throw FileException("too many open files", path);
	default:
		throw FileException(std::strerror(errno), path);
	}
}


} // namespace Poco
