//
// dir.cpp
//
// $Id: //poco/1.4/Foundation/samples/dir/src/dir.cpp#1 $
//
// This sample demonstrates the DirectoryIterator, File and Path classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DirectoryIterator.h"
#include "Poco/File.h"
#include "Poco/Path.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::DirectoryIterator;
using Poco::File;
using Poco::Path;
using Poco::DateTimeFormatter;
using Poco::DateTimeFormat;


int main(int argc, char** argv)
{
	std::string dir;
	if (argc > 1)
		dir = argv[1];
	else
		dir = Path::current();
		
	try
	{
		DirectoryIterator it(dir);
		DirectoryIterator end;
		while (it != end)
		{
			Path p(it->path());
			std::cout << (it->isDirectory() ? 'd' : '-')
					  << (it->canRead() ? 'r' : '-')
					  << (it->canWrite() ? 'w' : '-')
					  << ' '
					  << DateTimeFormatter::format(it->getLastModified(), DateTimeFormat::SORTABLE_FORMAT)
					  << ' '
					  << p.getFileName()
					  << std::endl;
			++it;
		}
	}
	catch (Poco::Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
		
	return 0;
}
