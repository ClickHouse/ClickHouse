//
// FilesystemConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  FilesystemConfiguration
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/FilesystemConfiguration.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/StringTokenizer.h"
#include "Poco/FileStream.h"


using Poco::Path;
using Poco::File;
using Poco::DirectoryIterator;
using Poco::StringTokenizer;


namespace Poco {
namespace Util {


FilesystemConfiguration::FilesystemConfiguration(const std::string& path):
	_path(path)
{
	_path.makeDirectory();
}


FilesystemConfiguration::~FilesystemConfiguration()
{
}


void FilesystemConfiguration::clear()
{
	File regDir(_path);
	regDir.remove(true);
}


bool FilesystemConfiguration::getRaw(const std::string& key, std::string& value) const
{
	Path p(keyToPath(key));
	p.setFileName("data");
	File f(p);
	if (f.exists())
	{
		value.reserve((std::string::size_type) f.getSize());
		Poco::FileInputStream istr(p.toString());
		int c = istr.get();
		while (c != std::char_traits<char>::eof())
		{
			value += (char) c;
			c = istr.get();
		}
		return true;
	}
	else return false;
}


void FilesystemConfiguration::setRaw(const std::string& key, const std::string& value)
{
	Path p(keyToPath(key));
	File dir(p);
	dir.createDirectories();
	p.setFileName("data");
	Poco::FileOutputStream ostr(p.toString());
	ostr.write(value.data(), (std::streamsize) value.length());
}


void FilesystemConfiguration::enumerate(const std::string& key, Keys& range) const
{
	Path p(keyToPath(key));
	File dir(p);
	if (!dir.exists())
	{
		return;
	}

	DirectoryIterator it(p);
	DirectoryIterator end;
	while (it != end)
	{
		 if (it->isDirectory())
			range.push_back(it.name());
		++it;
	}
}


void FilesystemConfiguration::removeRaw(const std::string& key)
{
	Path p(keyToPath(key));
	File dir(p);
	if (dir.exists())
	{
		dir.remove(true);
	}
}


Path FilesystemConfiguration::keyToPath(const std::string& key) const
{
	Path result(_path);
	StringTokenizer tokenizer(key, ".", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
	for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); ++it)
	{
		result.pushDirectory(*it);
	}	
	return result;
}


} } // namespace Poco::Util
