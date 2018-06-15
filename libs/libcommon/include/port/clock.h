#pragma once
/** Allows to build on MacOS X
  *
  * Highly experimental, not recommended, disabled by default.
  *
  * To use, include this file with -include compiler parameter.
  */

#include <time.h>

#ifdef __APPLE__

#include <AvailabilityMacros.h>
#ifndef MAC_OS_X_VERSION_10_12
#define MAC_OS_X_VERSION_10_12 101200
#endif
#define APPLE_HAVE_CLOCK_GETTIME MAC_OS_X_VERSION_MIN_REQUIRED >= MAC_OS_X_VERSION_10_12

#if !APPLE_HAVE_CLOCK_GETTIME || !defined(CLOCK_MONOTONIC)
/**
 * MacOS X doesn't support different clock sources
 *
 * Mapping all of them to 0, except for
 * CLOCK_THREAD_CPUTIME_ID, because there is a way
 * to implement it using in-kernel stats about threads
 */
#if !defined(CLOCK_MONOTONIC)
#define CLOCK_MONOTONIC 0
#endif
#if !defined(CLOCK_REALTIME)
#define CLOCK_REALTIME CLOCK_MONOTONIC
#endif
#if !defined(CLOCK_THREAD_CPUTIME_ID)
#define CLOCK_THREAD_CPUTIME_ID 3
#endif

typedef int clockid_t;
int clock_gettime(int clk_id, struct timespec* t);
#else

#endif

#if !defined(CLOCK_MONOTONIC_COARSE)
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

#endif
