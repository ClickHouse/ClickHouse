//
// FPEnvironment_C99.h
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Definitions of class FPEnvironmentImpl for C99.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FPEnvironment_C99_INCLUDED
#define Foundation_FPEnvironment_C99_INCLUDED


#include <cmath>
#include <fenv.h>
#include "Poco/Foundation.h"


// WebAssembly has no floating-point exception flags, so Emscripten's <fenv.h> defines
// FE_ALL_EXCEPT as 0 and omits the individual ones. Define them as no-op flags so the C99
// implementation still compiles; the corresponding fe*except() calls are no-ops there.
#ifndef FE_DIVBYZERO
#    define FE_DIVBYZERO 0
#endif
#ifndef FE_INEXACT
#    define FE_INEXACT 0
#endif
#ifndef FE_OVERFLOW
#    define FE_OVERFLOW 0
#endif
#ifndef FE_UNDERFLOW
#    define FE_UNDERFLOW 0
#endif
#ifndef FE_INVALID
#    define FE_INVALID 0
#endif


namespace Poco
{


class FPEnvironmentImpl
{
protected:
    enum RoundingModeImpl
    {
        FP_ROUND_DOWNWARD_IMPL = FE_DOWNWARD,
        FP_ROUND_UPWARD_IMPL = FE_UPWARD,
        FP_ROUND_TONEAREST_IMPL = FE_TONEAREST,
        FP_ROUND_TOWARDZERO_IMPL = FE_TOWARDZERO
    };
    enum FlagImpl
    {
        FP_DIVIDE_BY_ZERO_IMPL = FE_DIVBYZERO,
        FP_INEXACT_IMPL = FE_INEXACT,
        FP_OVERFLOW_IMPL = FE_OVERFLOW,
        FP_UNDERFLOW_IMPL = FE_UNDERFLOW,
        FP_INVALID_IMPL = FE_INVALID
    };
    FPEnvironmentImpl();
    FPEnvironmentImpl(const FPEnvironmentImpl & env);
    ~FPEnvironmentImpl();
    FPEnvironmentImpl & operator=(const FPEnvironmentImpl & env);
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
    fenv_t _env;
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
    return std::isinf((double)value) != 0;
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
    return std::isnan((double)value) != 0;
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


#endif // Foundation_FPEnvironment_C99_INCLUDED
