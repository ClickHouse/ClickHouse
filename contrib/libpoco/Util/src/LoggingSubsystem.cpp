//
// LoggingSubsystem.cpp
//
// $Id: //poco/1.4/Util/src/LoggingSubsystem.cpp#1 $
//
// Library: Util
// Package: Application
// Module:  LoggingSubsystem
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/LoggingSubsystem.h"
#include "Poco/Util/LoggingConfigurator.h"
#include "Poco/Util/Application.h"
#include "Poco/Logger.h"


using Poco::Logger;


namespace Poco {
namespace Util {


LoggingSubsystem::LoggingSubsystem()
{
}


LoggingSubsystem::~LoggingSubsystem()
{
}


const char* LoggingSubsystem::name() const
{
	return "Logging Subsystem";
}

	
void LoggingSubsystem::initialize(Application& app)
{
	LoggingConfigurator configurator;
	configurator.configure(&app.config());
	std::string logger = app.config().getString("application.logger", "Application");
	app.setLogger(Logger::get(logger));
}


void LoggingSubsystem::uninitialize()
{
}


} } // namespace Poco::Util
