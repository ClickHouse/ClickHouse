//
// File_WIN32U.cpp
//
// Library: Foundation
// Package: Filesystem
// Module:  File
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/File_WINCE.h"
#include "Poco/Exception.h"
#include "Poco/String.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Path.h"
#include "Poco/UnWindows.h"


namespace Poco {


class FileHandle
{
public:
	FileHandle(const std::string& path, const std::wstring& upath, DWORD access, DWORD share, DWORD disp)
	{
		_h = CreateFileW(upath.c_str(), access, share, 0, disp, 0, 0);
		if (_h == INVALID_HANDLE_VALUE)
		{
			FileImpl::handleLastErrorImpl(path);
		}
	}

	~FileHandle()
	{
		if (_h != INVALID_HANDLE_VALUE) CloseHandle(_h);
	}

	HANDLE get() const
	{
		return _h;
	}

private:
	HANDLE _h;
};


FileImpl::FileImpl()
{
}


FileImpl::FileImpl(const std::string& path): _path(path)
{
	std::string::size_type n = _path.size();
	if (n > 1 && (_path[n - 1] == '\\' || _path[n - 1] == '/') && !((n == 3 && _path[1]==':')))
	{
		_path.resize(n - 1);
	}
	convertPath(_path, _upath);
}


FileImpl::~FileImpl()
{
}


void FileImpl::swapImpl(FileImpl& file)
{
	std::swap(_path, file._path);
	std::swap(_upath, file._upath);
}


void FileImpl::setPathImpl(const std::string& path)
{
	_path = path;
	std::string::size_type n = _path.size();
	if (n > 1 && (_path[n - 1] == '\\' || _path[n - 1] == '/') && !((n == 3 && _path[1]==':')))
	{
		_path.resize(n - 1);
	}
	convertPath(_path, _upath);
}


bool FileImpl::existsImpl() const
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		switch (GetLastError())
		{
		case ERROR_FILE_NOT_FOUND:
		case ERROR_PATH_NOT_FOUND:
		case ERROR_NOT_READY:
		case ERROR_INVALID_DRIVE:
			return false;
		default:
			handleLastErrorImpl(_path);
		}
	}
	return true;
}


bool FileImpl::canReadImpl() const
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		switch (GetLastError())
		{
		case ERROR_ACCESS_DENIED:
			return false;
		default:
			handleLastErrorImpl(_path);
		}
	}
	return true;
}


bool FileImpl::canWriteImpl() const
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == INVALID_FILE_ATTRIBUTES)
		handleLastErrorImpl(_path);
	return (attr & FILE_ATTRIBUTE_READONLY) == 0;
}


bool FileImpl::canExecuteImpl() const
{
	Path p(_path);
	return icompare(p.getExtension(), "exe") == 0;
}


bool FileImpl::isFileImpl() const
{
	return !isDirectoryImpl() && !isDeviceImpl();
}


bool FileImpl::isDirectoryImpl() const
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == INVALID_FILE_ATTRIBUTES)
		handleLastErrorImpl(_path);
	return (attr & FILE_ATTRIBUTE_DIRECTORY) != 0;
}


bool FileImpl::isLinkImpl() const
{
	return false;
}


bool FileImpl::isDeviceImpl() const
{
	return false;
}


bool FileImpl::isHiddenImpl() const
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == INVALID_FILE_ATTRIBUTES)
		handleLastErrorImpl(_path);
	return (attr & FILE_ATTRIBUTE_HIDDEN) != 0;
}


Timestamp FileImpl::createdImpl() const
{
	poco_assert (!_path.empty());

	WIN32_FILE_ATTRIBUTE_DATA fad;
	if (GetFileAttributesExW(_upath.c_str(), GetFileExInfoStandard, &fad) == 0)
		handleLastErrorImpl(_path);
	return Timestamp::fromFileTimeNP(fad.ftCreationTime.dwLowDateTime, fad.ftCreationTime.dwHighDateTime);
}


Timestamp FileImpl::getLastModifiedImpl() const
{
	poco_assert (!_path.empty());

	WIN32_FILE_ATTRIBUTE_DATA fad;
	if (GetFileAttributesExW(_upath.c_str(), GetFileExInfoStandard, &fad) == 0)
		handleLastErrorImpl(_path);
	return Timestamp::fromFileTimeNP(fad.ftLastWriteTime.dwLowDateTime, fad.ftLastWriteTime.dwHighDateTime);
}


void FileImpl::setLastModifiedImpl(const Timestamp& ts)
{
	poco_assert (!_path.empty());

	UInt32 low;
	UInt32 high;
	ts.toFileTimeNP(low, high);
	FILETIME ft;
	ft.dwLowDateTime  = low;
	ft.dwHighDateTime = high;
	FileHandle fh(_path, _upath, GENERIC_WRITE, FILE_SHARE_WRITE, OPEN_EXISTING);
	if (SetFileTime(fh.get(), 0, &ft, &ft) == 0)
		handleLastErrorImpl(_path);
}


FileImpl::FileSizeImpl FileImpl::getSizeImpl() const
{
	poco_assert (!_path.empty());

	WIN32_FILE_ATTRIBUTE_DATA fad;
	if (GetFileAttributesExW(_upath.c_str(), GetFileExInfoStandard, &fad) == 0)
		handleLastErrorImpl(_path);
	LARGE_INTEGER li;
	li.LowPart  = fad.nFileSizeLow;
	li.HighPart = fad.nFileSizeHigh;
	return li.QuadPart;
}


void FileImpl::setSizeImpl(FileSizeImpl size)
{
	poco_assert (!_path.empty());

	FileHandle fh(_path, _upath, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, OPEN_EXISTING);
	LARGE_INTEGER li;
	li.QuadPart = size;
	if (SetFilePointer(fh.get(), li.LowPart, &li.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER)
		handleLastErrorImpl(_path);
	if (SetEndOfFile(fh.get()) == 0)
		handleLastErrorImpl(_path);
}


void FileImpl::setWriteableImpl(bool flag)
{
	poco_assert (!_path.empty());

	DWORD attr = GetFileAttributesW(_upath.c_str());
	if (attr == -1)
		handleLastErrorImpl(_path);
	if (flag)
		attr &= ~FILE_ATTRIBUTE_READONLY;
	else
		attr |= FILE_ATTRIBUTE_READONLY;
	if (SetFileAttributesW(_upath.c_str(), attr) == 0)
		handleLastErrorImpl(_path);
}


void FileImpl::setExecutableImpl(bool flag)
{
	// not supported
}


void FileImpl::copyToImpl(const std::string& path) const
{
	poco_assert (!_path.empty());

	std::wstring upath;
	convertPath(path, upath);
	if (CopyFileW(_upath.c_str(), upath.c_str(), FALSE) == 0)
		handleLastErrorImpl(_path);
}


void FileImpl::renameToImpl(const std::string& path)
{
	poco_assert (!_path.empty());

	std::wstring upath;
	convertPath(path, upath);
	if (MoveFileW(_upath.c_str(), upath.c_str()) == 0)
		handleLastErrorImpl(_path);
}


void FileImpl::linkToImpl(const std::string& path, int type) const
{
	throw Poco::NotImplementedException("File::linkTo() is not available on this platform");
}


void FileImpl::removeImpl()
{
	poco_assert (!_path.empty());

	if (isDirectoryImpl())
	{
		if (RemoveDirectoryW(_upath.c_str()) == 0)
			handleLastErrorImpl(_path);
	}
	else
	{
		if (DeleteFileW(_upath.c_str()) == 0)
			handleLastErrorImpl(_path);
	}
}


bool FileImpl::createFileImpl()
{
	poco_assert (!_path.empty());

	HANDLE hFile = CreateFileW(_upath.c_str(), GENERIC_WRITE, 0, 0, CREATE_NEW, 0, 0);
	if (hFile != INVALID_HANDLE_VALUE)
	{
		CloseHandle(hFile);
		return true;
	}
	else if (GetLastError() == ERROR_FILE_EXISTS)
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
	if (CreateDirectoryW(_upath.c_str(), 0) == 0)
		handleLastErrorImpl(_path);
	return true;
}


FileImpl::FileSizeImpl FileImpl::totalSpaceImpl() const
{
	poco_assert(!_path.empty());

	ULARGE_INTEGER space;
	if (!GetDiskFreeSpaceExW(_upath.c_str(), NULL, &space, NULL))
		handleLastErrorImpl(_path);
	return space.QuadPart;
}


FileImpl::FileSizeImpl FileImpl::usableSpaceImpl() const
{
	poco_assert(!_path.empty());

	ULARGE_INTEGER space;
	if (!GetDiskFreeSpaceExW(_upath.c_str(), &space, NULL, NULL))
		handleLastErrorImpl(_path);
	return space.QuadPart;
}


FileImpl::FileSizeImpl FileImpl::freeSpaceImpl() const
{
	poco_assert(!_path.empty());

	ULARGE_INTEGER space;
	if (!GetDiskFreeSpaceExW(_upath.c_str(), NULL, NULL, &space))
		handleLastErrorImpl(_path);
	return space.QuadPart;
}


void FileImpl::handleLastErrorImpl(const std::string& path)
{
	switch (GetLastError())
	{
	case ERROR_FILE_NOT_FOUND:
		throw FileNotFoundException(path);
	case ERROR_PATH_NOT_FOUND:
	case ERROR_BAD_NETPATH:
	case ERROR_CANT_RESOLVE_FILENAME:
	case ERROR_INVALID_DRIVE:
		throw PathNotFoundException(path);
	case ERROR_ACCESS_DENIED:
		throw FileAccessDeniedException(path);
	case ERROR_ALREADY_EXISTS:
	case ERROR_FILE_EXISTS:
		throw FileExistsException(path);
	case ERROR_INVALID_NAME:
	case ERROR_DIRECTORY:
	case ERROR_FILENAME_EXCED_RANGE:
	case ERROR_BAD_PATHNAME:
		throw PathSyntaxException(path);
	case ERROR_FILE_READ_ONLY:
		throw FileReadOnlyException(path);
	case ERROR_CANNOT_MAKE:
		throw CreateFileException(path);
	case ERROR_DIR_NOT_EMPTY:
		throw DirectoryNotEmptyException(path);
	case ERROR_WRITE_FAULT:
		throw WriteFileException(path);
	case ERROR_READ_FAULT:
		throw ReadFileException(path);
	case ERROR_SHARING_VIOLATION:
		throw FileException("sharing violation", path);
	case ERROR_LOCK_VIOLATION:
		throw FileException("lock violation", path);
	case ERROR_HANDLE_EOF:
		throw ReadFileException("EOF reached", path);
	case ERROR_HANDLE_DISK_FULL:
	case ERROR_DISK_FULL:
		throw WriteFileException("disk is full", path);
	case ERROR_NEGATIVE_SEEK:
		throw FileException("negative seek", path);
	default:
		throw FileException(path);
	}
}


void FileImpl::convertPath(const std::string& utf8Path, std::wstring& utf16Path)
{
	UnicodeConverter::toUTF16(utf8Path, utf16Path);
}

} // namespace Poco
