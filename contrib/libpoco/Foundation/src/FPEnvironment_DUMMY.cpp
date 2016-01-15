//
// FPEnvironment_C99.cpp
//
// $Id: //poco/1.4/Foundation/src/FPEnvironment_DUMMY.cpp#1 $
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


#include "Poco/FPEnvironment_DUMMY.h"


namespace Poco {


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::_roundingMode;


FPEnvironmentImpl::FPEnvironmentImpl()
{
}


FPEnvironmentImpl::FPEnvironmentImpl(const FPEnvironmentImpl& env)
{
}


FPEnvironmentImpl::~FPEnvironmentImpl()
{
}


FPEnvironmentImpl& FPEnvironmentImpl::operator = (const FPEnvironmentImpl& env)
{
	return *this;
}


void FPEnvironmentImpl::keepCurrentImpl()
{
}


void FPEnvironmentImpl::clearFlagsImpl()
{
}


bool FPEnvironmentImpl::isFlagImpl(FlagImpl flag)
{
	return false;
}


void FPEnvironmentImpl::setRoundingModeImpl(RoundingModeImpl mode)
{
	_roundingMode = mode;
}


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::getRoundingModeImpl()
{
	return _roundingMode;
}


long double FPEnvironmentImpl::copySignImpl(long double target, long double source)
{
	return (source >= 0 && target >= 0) || (source < 0 && target < 0) ? target : -target;
}


} // namespace Poco
