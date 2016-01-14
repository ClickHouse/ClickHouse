//
// FPEnvironment.cpp
//
// $Id: //poco/1.4/Foundation/src/FPEnvironment.cpp#1 $
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


// pull in platform identification macros needed below
#include "Poco/Platform.h"


#if defined(POCO_NO_FPENVIRONMENT)
#include "FPEnvironment_DUMMY.cpp"
#elif defined(__osf__) || defined(__VMS)
#include "FPEnvironment_DEC.cpp"
#elif defined(sun) || defined(__sun)
#include "FPEnvironment_SUN.cpp"
#elif defined(POCO_OS_FAMILY_UNIX)
#include "FPEnvironment_C99.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "FPEnvironment_WIN32.cpp"
#else
#include "FPEnvironment_DUMMY.cpp"
#endif


// header file must be included after platform-specific part
// due to _XOPEN_SOURCE conflict on Tru64 (see FPEnvironment_DEC.cpp)
#include "Poco/FPEnvironment.h"


namespace Poco {


FPEnvironment::FPEnvironment()
{
}


FPEnvironment::FPEnvironment(RoundingMode rm)
{
	setRoundingMode(rm);
}

	
FPEnvironment::FPEnvironment(const FPEnvironment& env): FPEnvironmentImpl(env)
{
}

	
FPEnvironment::~FPEnvironment()
{
}

	
FPEnvironment& FPEnvironment::operator = (const FPEnvironment& env)
{
	if (&env != this)
	{
		FPEnvironmentImpl::operator = (env);
	}
	return *this;
}


void FPEnvironment::keepCurrent()
{
	keepCurrentImpl();
}


void FPEnvironment::clearFlags()
{
	clearFlagsImpl();
}


} // namespace Poco
