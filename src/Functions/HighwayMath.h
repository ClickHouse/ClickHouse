#pragma once

#include "config.h"

#if USE_HIGHWAY

#include <cstddef>

/// Vectorized `exp`, `log`, `tanh` and `sigmoid` over arrays, implemented with Google Highway
/// (`hwy/contrib/math`) with runtime dispatch to the best instruction set of the CPU.
///
/// The maximum error is about 1 ULP for `exp`, `log` and `sigmoid` and about 3 ULP for `tanh`.
/// Special values (NaN, infinities, zero, negative arguments of `log`, overflow of `exp`)
/// give the same results as libm.
///
/// `src` and `dst` may be the same array.
namespace DB::HighwayMath
{

void exp(const float * src, size_t size, float * dst);
void exp(const double * src, size_t size, double * dst);

void log(const float * src, size_t size, float * dst);
void log(const double * src, size_t size, double * dst);

void tanh(const float * src, size_t size, float * dst);
void tanh(const double * src, size_t size, double * dst);

void sigmoid(const float * src, size_t size, float * dst);
void sigmoid(const double * src, size_t size, double * dst);

}

#endif
