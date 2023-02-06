//
// ApacheApplication.cpp
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheApplication.h"
#include "ApacheChannel.h"
#include "Poco/Logger.h"
#include "Poco/SingletonHolder.h"
#include <vector>


using Poco::Logger;
using Poco::FastMutex;


ApacheApplication::ApacheApplication():
	_ready(false)
{
	Logger::root().setChannel(new ApacheChannel);
}


ApacheApplication::~ApacheApplication()
{
	Logger::shutdown();
}


void ApacheApplication::setup()
{
	FastMutex::ScopedLock lock(_mutex);
	
	if (!_ready)
	{
		std::vector<std::string> cmdLine;
		cmdLine.push_back("mod_poco");
		init(cmdLine);
		_ready = true;
	}
}


ApacheApplication& ApacheApplication::instance()
{
	static Poco::SingletonHolder<ApacheApplication> sh;
	return *sh.get();
}
