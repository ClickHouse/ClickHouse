//
// FPEnvironment_DUMMY.h
//
// $Id: //poco/1.4/Foundation/include/Poco/FPEnvironment_DUMMY.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Definition of class FPEnvironmentImpl for platforms that do not
// support IEEE 754 extensions.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FPEnvironment_DUMMY_INCLUDED
#define Foundation_FPEnvironment_DUMMY_INCLUDED


#include "Poco/Foundation.h"
#include <cmath>


namespace Poco {


class Foundation_API FPEnvironmentImpl
{
protected:
	enum RoundingModeImpl
	{
		FP_ROUND_DOWNWARD_IMPL,
		FP_ROUND_UPWARD_IMPL,
		FP_ROUND_TONEAREST_IMPL,
		FP_ROUND_TOWARDZERO_IMPL
	};
	enum FlagImpl
	{
		FP_DIVIDE_BY_ZERO_IMPL,
		FP_INEXACT_IMPL,
		FP_OVERFLOW_IMPL,
		FP_UNDERFLOW_IMPL,
		FP_INVALID_IMPL
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
	static RoundingModeImpl _roundingMode;
};


//
// inlines
//
inline bool FPEnvironmentImpl::isInfiniteImpl(float value)
{
	return std::isinf(value) != 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(double value)
{
	return std::isinf(value) != 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(long double value)
{
	return std::isinf((double) value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(float value)
{
	return std::isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(double value)
{
	return std::isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(long double value)
{
	return std::isnan((double) value) != 0;
}


inline float FPEnvironmentImpl::copySignImpl(float target, float source)
{
	return copysignf(target, source);
}


inline double FPEnvironmentImpl::copySignImpl(double target, double source)
{
	return copysign(target, source);
}


} // namespace Poco


#endif // Foundation_FPEnvironment_DUMMY_INCLUDED
