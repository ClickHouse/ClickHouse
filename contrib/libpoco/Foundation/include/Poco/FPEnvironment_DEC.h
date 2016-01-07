//
// FPEnvironment_DEC.h
//
// $Id: //poco/1.4/Foundation/include/Poco/FPEnvironment_DEC.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Definitions of class FPEnvironmentImpl for Tru64 and OpenVMS Alpha.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FPEnvironment_DEC_INCLUDED
#define Foundation_FPEnvironment_DEC_INCLUDED


#include "Poco/Foundation.h"
#if defined(__VMS)
#include <ieeedef.h>
#else
#include <machine/fpu.h>
#endif


namespace Poco {


class FPEnvironmentImpl
{
protected:
	enum RoundingModeImpl
	{
		FP_ROUND_DOWNWARD_IMPL   = 0,
		FP_ROUND_UPWARD_IMPL     = 0,
		FP_ROUND_TONEAREST_IMPL  = 0,
		FP_ROUND_TOWARDZERO_IMPL = 0
	};
	enum FlagImpl
	{
#if defined(__VMS)
		FP_DIVIDE_BY_ZERO_IMPL = IEEE$M_STATUS_DZE,
		FP_INEXACT_IMPL        = IEEE$M_STATUS_INE,
		FP_OVERFLOW_IMPL       = IEEE$M_STATUS_OVF,
		FP_UNDERFLOW_IMPL      = IEEE$M_STATUS_UNF,
		FP_INVALID_IMPL        = IEEE$M_STATUS_INV
#else
		FP_DIVIDE_BY_ZERO_IMPL = IEEE_STATUS_DZE,
		FP_INEXACT_IMPL        = IEEE_STATUS_INE,
		FP_OVERFLOW_IMPL       = IEEE_STATUS_OVF,
		FP_UNDERFLOW_IMPL      = IEEE_STATUS_UNF,
		FP_INVALID_IMPL        = IEEE_STATUS_INV
#endif
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
#if defined(__VMS)
	struct _ieee _env;
#else
	unsigned long _env;
#endif
};


} // namespace Poco


#endif // Foundation_FPEnvironment_DEC_INCLUDED
