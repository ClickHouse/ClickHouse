//
// PartStore.cpp
//
// Library: Net
// Package: Messages
// Module:  PartStore
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/PartStore.h"
#include "Poco/TemporaryFile.h"
#include "Poco/File.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Net {


//
// PartStore
//


PartStore::PartStore(const std::string& mediaType): PartSource(mediaType)
{
}


PartStore::~PartStore()
{
}


//
// FilePartStore
//


FilePartStore::FilePartStore(const std::string& content, const std::string& mediaType, const std::string& filename):
	PartStore(mediaType),
	_filename(filename),
	_path(TemporaryFile::tempName()),
	_fstr(_path)
{
	_fstr << content << std::flush;
	_fstr.seekg(0, std::ios::beg);
}


FilePartStore::~FilePartStore()
{
	try
	{
		_fstr.close();
		File(_path).remove();
	}
	catch (...)
	{
	}
}


std::istream& FilePartStore::stream()
{
	return _fstr;
}


const std::string& FilePartStore::filename() const
{
	return _filename;
}


const std::string& FilePartStore::path() const
{
	return _path;
}


} } // namespace Poco::Net
