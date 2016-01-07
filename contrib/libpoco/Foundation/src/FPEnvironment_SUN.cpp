//
// FPEnvironment_SUN.cpp
//
// $Id: //poco/1.4/Foundation/src/FPEnvironment_SUN.cpp#1 $
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include <math.h>
#include "Poco/FPEnvironment_SUN.h"


namespace Poco {


FPEnvironmentImpl::FPEnvironmentImpl()
{
	_rnd = fpgetround();
	_exc = fpgetmask();
}


FPEnvironmentImpl::FPEnvironmentImpl(const FPEnvironmentImpl& env)
{
	_rnd = env._rnd;
	_exc = env._exc;
}


FPEnvironmentImpl::~FPEnvironmentImpl()
{
	fpsetround(_rnd);
	fpsetmask(_exc);
}


FPEnvironmentImpl& FPEnvironmentImpl::operator = (const FPEnvironmentImpl& env)
{
	_rnd = env._rnd;
	_exc = env._exc;
	return *this;
}


bool FPEnvironmentImpl::isInfiniteImpl(float value)
{
	int cls = fpclass(value);
	return cls == FP_PINF || cls == FP_NINF;
}


bool FPEnvironmentImpl::isInfiniteImpl(double value)
{
	int cls = fpclass(value);
	return cls == FP_PINF || cls == FP_NINF;
}


bool FPEnvironmentImpl::isInfiniteImpl(long double value)
{
	int cls = fpclass(value);
	return cls == FP_PINF || cls == FP_NINF;
}


bool FPEnvironmentImpl::isNaNImpl(float value)
{
	return isnanf(value) != 0;
}


bool FPEnvironmentImpl::isNaNImpl(double value)
{
	return isnan(value) != 0;
}


bool FPEnvironmentImpl::isNaNImpl(long double value)
{
	return isnan((double) value) != 0;
}


float FPEnvironmentImpl::copySignImpl(float target, float source)
{
	return (float) copysign(target, source);
}


double FPEnvironmentImpl::copySignImpl(double target, double source)
{
	return (float) copysign(target, source);
}


long double FPEnvironmentImpl::copySignImpl(long double target, long double source)
{
	return (source > 0 && target > 0) || (source < 0 && target < 0) ? target : -target;
}


void FPEnvironmentImpl::keepCurrentImpl()
{
	fpsetround(_rnd);
	fpsetmask(_exc);
}


void FPEnvironmentImpl::clearFlagsImpl()
{
	fpsetsticky(0);
}


bool FPEnvironmentImpl::isFlagImpl(FlagImpl flag)
{
	return (fpgetsticky() & flag) != 0;
}


void FPEnvironmentImpl::setRoundingModeImpl(RoundingModeImpl mode)
{
	fpsetround((fp_rnd) mode);
}


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::getRoundingModeImpl()
{
	return (FPEnvironmentImpl::RoundingModeImpl) fpgetround();
}


} // namespace Poco
