//
// FPEnvironment_WIN32.h
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Definitions of class FPEnvironmentImpl for WIN32.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FPEnvironment_WIN32_INCLUDED
#define Foundation_FPEnvironment_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include <float.h>
#include <math.h>

#ifndef _SW_INEXACT
#	define _SW_INEXACT 0x00000001 // inexact (precision)
#endif
#ifndef _SW_UNDERFLOW
#	define _SW_UNDERFLOW 0x00000002 // underflow
#endif
#ifndef _SW_OVERFLOW
#	define _SW_OVERFLOW 0x00000004 // overflow
#endif
#ifndef _SW_ZERODIVIDE
#	define _SW_ZERODIVIDE 0x00000008 // zero divide
#endif
#ifndef _SW_INVALID
#	define _SW_INVALID 0x00000010 // invalid
#endif
#ifndef _SW_DENORMAL
#	define _SW_DENORMAL 0x00080000 // denormal status bit
#endif


namespace Poco {


class Foundation_API FPEnvironmentImpl
{
protected:
	enum RoundingModeImpl
	{
		FP_ROUND_DOWNWARD_IMPL   = _RC_DOWN,
		FP_ROUND_UPWARD_IMPL     = _RC_UP,
		FP_ROUND_TONEAREST_IMPL  = _RC_NEAR,
		FP_ROUND_TOWARDZERO_IMPL = _RC_CHOP
	};
	enum FlagImpl
	{
		FP_DIVIDE_BY_ZERO_IMPL = _SW_ZERODIVIDE,
		FP_INEXACT_IMPL        = _SW_INEXACT,
		FP_OVERFLOW_IMPL       = _SW_OVERFLOW,
		FP_UNDERFLOW_IMPL      = _SW_UNDERFLOW,
		FP_INVALID_IMPL        = _SW_INVALID
	};
	FPEnvironmentImpl();
	FPEnvironmentImpl(const FPEnvironmentImpl& env);
	~FPEnvironmentImpl();
	FPEnvironmentImpl& operator = (const FPEnvironmentImpl& env);
	void keepCurrentImpl();
	static void clearFlagsImpl();
	static bool isFlagImpl(FlagImpl flag);	
	static void setRoundingModeImpl(RoundingModeImpl mode);
	static RoundingModeImpl getRoundingModeImpl();
	static bool isInfiniteImpl(float value);		
	static bool isInfiniteImpl(double value);
	static bool isInfiniteImpl(long double value);
	static bool isNaNImpl(float value);		
	static bool isNaNImpl(double value);
	static bool isNaNImpl(long double value);
	static float copySignImpl(float target, float source);		
	static double copySignImpl(double target, double source);
	static long double copySignImpl(long double target, long double source);

private:
	unsigned _env;
};


//
// inlines
//
inline bool FPEnvironmentImpl::isInfiniteImpl(float value)
{
	return _finite(value) == 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(double value)
{
	return _finite(value) == 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(long double value)
{
	return _finite(value) == 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(float value)
{
	return _isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(double value)
{
	return _isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(long double value)
{
	return _isnan(value) != 0;
}


inline float FPEnvironmentImpl::copySignImpl(float target, float source)
{
	return float(_copysign(target, source));
}


inline double FPEnvironmentImpl::copySignImpl(double target, double source)
{
	return _copysign(target, source);
}


inline long double FPEnvironmentImpl::copySignImpl(long double target, long double source)
{
	return (source > 0 && target > 0) || (source < 0 && target < 0) ? target : -target;
}


} // namespace Poco


#endif // Foundation_FPEnvironment_WIN32_INCLUDED
