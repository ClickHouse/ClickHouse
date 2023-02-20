//
// FPEnvironment_DEC.cpp
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


//
// _XOPEN_SOURCE disables the ieee fp functions
// in <math.h>, therefore we undefine it for this file.
//
#undef _XOPEN_SOURCE


#include <math.h>
#include <fp.h>
#include <fp_class.h>
#include "Poco/FPEnvironment_DEC.h"


namespace Poco {


FPEnvironmentImpl::FPEnvironmentImpl()
{
	_env = ieee_get_fp_control();
}


FPEnvironmentImpl::FPEnvironmentImpl(const FPEnvironmentImpl& env)
{
	_env = env._env;
}


FPEnvironmentImpl::~FPEnvironmentImpl()
{
	ieee_set_fp_control(_env);
}


FPEnvironmentImpl& FPEnvironmentImpl::operator = (const FPEnvironmentImpl& env)
{
	_env = env._env;
	return *this;
}


bool FPEnvironmentImpl::isInfiniteImpl(float value)
{
	int cls = fp_classf(value);
	return cls == FP_POS_INF || cls == FP_NEG_INF;
}


bool FPEnvironmentImpl::isInfiniteImpl(double value)
{
	int cls = fp_class(value);
	return cls == FP_POS_INF || cls == FP_NEG_INF;
}


bool FPEnvironmentImpl::isInfiniteImpl(long double value)
{
	int cls = fp_classl(value);
	return cls == FP_POS_INF || cls == FP_NEG_INF;
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
	return isnanl(value) != 0;
}


float FPEnvironmentImpl::copySignImpl(float target, float source)
{
	return copysignf(target, source);
}


double FPEnvironmentImpl::copySignImpl(double target, double source)
{
	return copysign(target, source);
}


long double FPEnvironmentImpl::copySignImpl(long double target, long double source)
{
	return copysignl(target, source);
}


void FPEnvironmentImpl::keepCurrentImpl()
{
	ieee_set_fp_control(_env);
}


void FPEnvironmentImpl::clearFlagsImpl()
{
	ieee_set_fp_control(0);
}


bool FPEnvironmentImpl::isFlagImpl(FlagImpl flag)
{
	return (ieee_get_fp_control() & flag) != 0;
}


void FPEnvironmentImpl::setRoundingModeImpl(RoundingModeImpl mode)
{
	// not supported
}


FPEnvironmentImpl::RoundingModeImpl FPEnvironmentImpl::getRoundingModeImpl()
{
	// not supported
	return FPEnvironmentImpl::RoundingModeImpl(0);
}


} // namespace Poco
