//
// Archive.cpp
//
// Library: SevenZip
// Package: Archive
// Module:  Archive
//
// Definition of the Archive class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SevenZip/Archive.h"
#include "Poco/TextConverter.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/UTF16Encoding.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/FileStream.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/Mutex.h"
#include "7z.h"
#include "7zAlloc.h"
#include "7zCrc.h"
#include "7zFile.h"


namespace Poco {
namespace SevenZip {


class ArchiveImpl
{
public:
	ArchiveImpl(const std::string& path):
		_path(path)
	{
		initialize();
		open();
	}
	
	~ArchiveImpl()
	{
		try
		{
			close();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
	
	const std::string& path() const
	{
		return _path;
	}
	
	std::size_t size() const
	{
		return _entries.size();
	}
	
	Archive::ConstIterator begin() const
	{
		return _entries.begin();
	}
	
	Archive::ConstIterator end() const
	{
		return _entries.end();
	}
	
	std::string extract(const ArchiveEntry& entry, const std::string& destPath)
	{
		Poco::Path basePath;
		if (destPath.empty())
		{
			basePath = Poco::Path::current();
		}
		else
		{
			basePath = destPath;
		}
		basePath.makeDirectory();
		Poco::Path entryPath(entry.path(), Poco::Path::PATH_UNIX);
		Poco::Path extractedPath(basePath);
		extractedPath.append(entryPath);
		extractedPath.makeAbsolute();
		
		if (entry.isFile())
		{	
			Poco::UInt32 blockIndex = 0;
			Byte* pOutBuffer = 0;
			std::size_t outBufferSize = 0;
			std::size_t offset = 0;
			std::size_t extractedSize = 0;
			int err = SzArEx_Extract(
				&_db, 
				&_lookStream.s, 
				entry.index(), 
				&blockIndex, 
				&pOutBuffer,
				&outBufferSize,
				&offset,
				&extractedSize,
				&_szAlloc,
				&_szAllocTemp);
			if (err == SZ_OK)
			{
				try
				{
					poco_assert (extractedSize == entry.size());
				
					Poco::Path parent = extractedPath.parent();
					Poco::File dir(parent.toString());
					dir.createDirectories();

					Poco::FileOutputStream ostr(extractedPath.toString());
					ostr.write(reinterpret_cast<const char*>(pOutBuffer) + offset, extractedSize);
					IAlloc_Free(&_szAlloc, pOutBuffer);
				}
				catch (...)
				{
					IAlloc_Free(&_szAlloc, pOutBuffer);
					throw;
				}
			}
			else
			{
				handleError(err, entry.path());
			}
		}
		else
		{
			Poco::File dir(extractedPath.toString());
			dir.createDirectories();
		}
		
		return extractedPath.toString();
	}
	
protected:
	void initialize()
	{
		FileInStream_CreateVTable(&_archiveStream);
		LookToRead_CreateVTable(&_lookStream, False);

		Poco::FastMutex::ScopedLock lock(_initMutex);
		if (!_initialized)
		{
			CrcGenerateTable();
			_initialized = true;
		}
	}

	void open()
	{
		checkFile();

#if defined(_WIN32) && defined(POCO_WIN32_UTF8)
		std::wstring wpath;
		Poco::UnicodeConverter::toUTF16(_path, wpath);
		if (InFile_OpenW(&_archiveStream.file, wpath.c_str()) != SZ_OK)
		{
			throw Poco::OpenFileException(_path);
		}
#else
		if (InFile_Open(&_archiveStream.file, _path.c_str()) != SZ_OK)
		{
			throw Poco::OpenFileException(_path);
		}
#endif
	
		_lookStream.realStream = &_archiveStream.s;
		LookToRead_Init(&_lookStream);

		SzArEx_Init(&_db);
		int err = SzArEx_Open(&_db, &_lookStream.s, &_szAlloc, &_szAllocTemp);
		if (err == SZ_OK)
		{
			loadEntries();
		}
		else 
		{
			handleError(err);
		}
	}
	
	void close()
	{
		SzArEx_Free(&_db, &_szAlloc);
		File_Close(&_archiveStream.file);
	}
	
	void loadEntries()
	{
		_entries.reserve(_db.db.NumFiles);
		for (Poco::UInt32 i = 0; i < _db.db.NumFiles; i++)
		{
			const CSzFileItem *f = _db.db.Files + i;
			
			ArchiveEntry::EntryType type = f->IsDir ? ArchiveEntry::ENTRY_DIRECTORY : ArchiveEntry::ENTRY_FILE;
			Poco::UInt32 attributes = f->AttribDefined ? f->Attrib : 0;
			Poco::UInt64 size = f->Size;
			
			std::vector<Poco::UInt16> utf16Path;
			std::size_t utf16PathLen = SzArEx_GetFileNameUtf16(&_db, i, 0);
			utf16Path.resize(utf16PathLen, 0);
			utf16PathLen--; // we don't need terminating 0 later on
			SzArEx_GetFileNameUtf16(&_db, i, &utf16Path[0]);
			Poco::UTF8Encoding utf8Encoding;
			Poco::UTF16Encoding utf16Encoding;
			Poco::TextConverter converter(utf16Encoding, utf8Encoding);
			std::string utf8Path;
			converter.convert(&utf16Path[0], (int) utf16PathLen*sizeof(Poco::UInt16), utf8Path);
						
			Poco::Timestamp lastModified(0);
			if (f->MTimeDefined)
			{
				Poco::Timestamp::TimeVal tv(0);
				tv = (static_cast<Poco::UInt64>(f->MTime.High) << 32) + f->MTime.Low;
				tv -= (static_cast<Poco::Int64>(0x019DB1DE) << 32) + 0xD53E8000;
				tv /= 10;
				lastModified = tv;
			}
			
			ArchiveEntry entry(type, utf8Path, size, lastModified, attributes, i);
			_entries.push_back(entry);
		}
	}
	
	void checkFile()
	{
		Poco::File f(_path);
		if (!f.exists())
		{
			throw Poco::FileNotFoundException(_path);
		}
		if (!f.isFile())
		{
			throw Poco::OpenFileException("not a file", _path);
		}
		if (!f.canRead())
		{
			throw Poco::FileAccessDeniedException(_path);
		}
	}
	
	void handleError(int err)
	{
		std::string arg;
		handleError(err, arg);
	}
	
	void handleError(int err, const std::string& arg)
	{
		switch (err)
		{
		case SZ_ERROR_DATA:
			throw Poco::DataFormatException("archive", _path, err);
		case SZ_ERROR_MEM:
			throw Poco::RuntimeException("7z library out of memory", err);
		case SZ_ERROR_CRC:
			throw Poco::DataException("CRC error", arg, err);
		case SZ_ERROR_UNSUPPORTED:
			throw Poco::DataException("unsupported archive format", arg, err);
		case SZ_ERROR_PARAM:
			throw Poco::InvalidArgumentException("7z library", err);
		case SZ_ERROR_INPUT_EOF:
			throw Poco::ReadFileException("EOF while reading archive", _path, err);
		case SZ_ERROR_OUTPUT_EOF:
			throw Poco::WriteFileException(arg, err);
		case SZ_ERROR_READ:
			throw Poco::ReadFileException(_path, err);
		case SZ_ERROR_WRITE:
			throw Poco::WriteFileException(arg, err);
		default:
			throw Poco::IOException("7z error", arg, err);
		}
	}

private:
	std::string _path;
	Archive::EntryVec _entries;
	CFileInStream _archiveStream;
	CLookToRead _lookStream;
	CSzArEx _db;
	static ISzAlloc _szAlloc;
	static ISzAlloc _szAllocTemp;
	static Poco::FastMutex _initMutex;
	static bool _initialized;
};


ISzAlloc ArchiveImpl::_szAlloc     = { SzAlloc, SzFree };
ISzAlloc ArchiveImpl::_szAllocTemp = { SzAlloc, SzFree };
Poco::FastMutex ArchiveImpl::_initMutex;
bool ArchiveImpl::_initialized(false);


Archive::Archive(const std::string& path):
	_pImpl(new ArchiveImpl(path))
{
}

	
Archive::~Archive()
{
	delete _pImpl;
}


const std::string& Archive::path() const
{
	return _pImpl->path();
}

	
std::size_t Archive::size() const
{
	return _pImpl->size();
}


Archive::ConstIterator Archive::begin() const
{
	return _pImpl->begin();
}

	
Archive::ConstIterator Archive::end() const
{
	return _pImpl->end();
}


void Archive::extract(const std::string& destPath)
{
	for (ConstIterator it = begin(); it != end(); ++it)
	{
		ExtractedEventArgs extractedArgs;
		extractedArgs.entry = *it;
		bool success = true;
		try
		{
			extractedArgs.extractedPath = _pImpl->extract(*it, destPath);
		}
		catch (Poco::Exception& exc)
		{
			success = false;
			FailedEventArgs failedArgs;
			failedArgs.entry = *it;
			failedArgs.pException = &exc;
			failed(this, failedArgs);
		}
		if (success)
		{
			extracted(this, extractedArgs);
		}
	}
}

	
std::string Archive::extract(const ArchiveEntry& entry, const std::string& destPath)
{
	return _pImpl->extract(entry, destPath);
}


} } // namespace Poco::SevenZip
