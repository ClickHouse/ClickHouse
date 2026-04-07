//
// FPEnvironment_C99.cpp
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


#include "Poco/FPEnvironment_C99.h"


namespace Poco {


FPEnvironmentImpl::FPEnvironmentImpl()
{
	fegetenv(&_env);
}


FPEnvironmentImpl::FPEnvironmentImpl(const FPEnvironmentImpl& env)
{
	_env = env._env;
}


FPEnvironmentImpl::~FPEnvironmentImpl()
{
	fesetenv(&_env);
}


FPEnvironmentImpl& FPEnvironmentImpl::operator = (const FPEnvironmentImpl& env)
{
	_env = env._env;
	return *this;
}


void FPEnvironmentImpl::keepCurrentImpl()
{
	fegetenv(&_env);
}


void FPEnvironmentImpl::clearFlagsImpl()
{
	feclearexcept(FE_ALL_EXCEPT);
}


bool FPEnvironmentImpl::isFlagImpl(FlagImpl flag)
{
	return fetestexcept(flag) != 0;
}


void FPEnvironmentImpl::setRoundingModeImpl(RoundingModeImpl mode)
{
	fesetround(mode);
}


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::getRoundingModeImpl()
{
	return (RoundingModeImpl) fegetround();
}


long double FPEnvironmentImpl::copySignImpl(long double target, long double source)
{
	return (source >= 0 && target >= 0) || (source < 0 && target < 0) ? target : -target;
}


} // namespace Poco
