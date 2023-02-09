//
// FPEnvironment.cpp
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
#include "Poco/FPEnvironment.h"


#if defined(POCO_NO_FPENVIRONMENT)
#include "FPEnvironment_DUMMY.cpp"
#elif defined(__osf__)
#include "FPEnvironment_DEC.cpp"
#elif defined(sun) || defined(__sun)
#include "FPEnvironment_SUN.cpp"
#elif defined(__QNX__)
#include "FPEnvironment_QNX.cpp"
#elif defined(POCO_OS_FAMILY_UNIX)
#include "FPEnvironment_C99.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "FPEnvironment_WIN32.cpp"
#else
#include "FPEnvironment_DUMMY.cpp"
#endif


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
