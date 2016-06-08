//
// Subsystem.cpp
//
// $Id: //poco/1.4/Util/src/Subsystem.cpp#1 $
//
// Library: Util
// Package: Application
// Module:  Subsystem
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Subsystem.h"


namespace Poco {
namespace Util {


Subsystem::Subsystem()
{
}


Subsystem::~Subsystem()
{
}


void Subsystem::reinitialize(Application& app)
{
	uninitialize();
	initialize(app);
}


void Subsystem::defineOptions(OptionSet& options)
{
}


} } // namespace Poco::Util
