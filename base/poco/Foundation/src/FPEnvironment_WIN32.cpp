//
// FPEnvironment_WIN32.cpp
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


#include "Poco/FPEnvironment_WIN32.h"


namespace Poco {


FPEnvironmentImpl::FPEnvironmentImpl()
{
	_env = _controlfp(0, 0);
}


FPEnvironmentImpl::FPEnvironmentImpl(const FPEnvironmentImpl& env)
{
	_env = env._env;
}


FPEnvironmentImpl::~FPEnvironmentImpl()
{
	_controlfp(_env, _MCW_RC);
}


FPEnvironmentImpl& FPEnvironmentImpl::operator = (const FPEnvironmentImpl& env)
{
	_env = env._env;
	return *this;
}


void FPEnvironmentImpl::keepCurrentImpl()
{
	_env = _controlfp(0, 0);
}


void FPEnvironmentImpl::clearFlagsImpl()
{
	_clearfp();
}


bool FPEnvironmentImpl::isFlagImpl(FlagImpl flag)
{
	return (_statusfp() & flag) != 0;
}


void FPEnvironmentImpl::setRoundingModeImpl(RoundingModeImpl mode)
{
	_controlfp(mode, _MCW_RC);
}


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::getRoundingModeImpl()
{
	return RoundingModeImpl(_controlfp(0, 0) & _MCW_RC);
}


} // namespace Poco
