/*
 * Double-precision x^y function.
 *
 * Copyright (c) 2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#include <math.h>
#include <stdint.h>
#include "libm.h"
#include "exp_data.h"
#include "pow_data.h"

/*
Worst-case error: 0.54 ULP (~= ulperr_exp + 1024*Ln2*relerr_log*2^53)
relerr_log: 1.3 * 2^-68 (Relative error of log, 1.5 * 2^-68 without fma)
ulperr_exp: 0.509 ULP (ULP error of exp, 0.511 ULP without fma)
*/

#define T __pow_log_data.tab
#define A __pow_log_data.poly
#define Ln2hi __pow_log_data.ln2hi
#define Ln2lo __pow_log_data.ln2lo
#define N (1 << POW_LOG_TABLE_BITS)
#define OFF 0x3fe6955500000000

/* Top 12 bits of a double (sign and exponent bits).  */
static inline uint32_t top12(double x)
{
	return asuint64(x) >> 52;
}

/* Compute y+TAIL = log(x) where the rounded result is y and TAIL has about
   additional 15 bits precision.  IX is the bit representation of x, but
   normalized in the subnormal range using the sign bit for the exponent.  */
static inline double_t log_inline(uint64_t ix, double_t *tail)
{
	/* double_t for better performance on targets with FLT_EVAL_METHOD==2.  */
	double_t z, r, y, invc, logc, logctail, kd, hi, t1, t2, lo, lo1, lo2, p;
	uint64_t iz, tmp;
	int k, i;

	/* x = 2^k z; where z is in range [OFF,2*OFF) and exact.
	   The range is split into N subintervals.
	   The ith subinterval contains z and c is near its center.  */
	tmp = ix - OFF;
	i = (tmp >> (52 - POW_LOG_TABLE_BITS)) % N;
	k = (int64_t)tmp >> 52; /* arithmetic shift */
	iz = ix - (tmp & 0xfffULL << 52);
	z = asdouble(iz);
	kd = (double_t)k;

	/* log(x) = k*Ln2 + log(c) + log1p(z/c-1).  */
	invc = T[i].invc;
	logc = T[i].logc;
	logctail = T[i].logctail;

	/* Note: 1/c is j/N or j/N/2 where j is an integer in [N,2N) and
     |z/c - 1| < 1/N, so r = z/c - 1 is exactly representible.  */
#if __FP_FAST_FMA
	r = __builtin_fma(z, invc, -1.0);
#else
	/* Split z such that rhi, rlo and rhi*rhi are exact and |rlo| <= |r|.  */
	double_t zhi = asdouble((iz + (1ULL << 31)) & (-1ULL << 32));
	double_t zlo = z - zhi;
	double_t rhi = zhi * invc - 1.0;
	double_t rlo = zlo * invc;
	r = rhi + rlo;
#endif

	/* k*Ln2 + log(c) + r.  */
	t1 = kd * Ln2hi + logc;
	t2 = t1 + r;
	lo1 = kd * Ln2lo + logctail;
	lo2 = t1 - t2 + r;

	/* Evaluation is optimized assuming superscalar pipelined execution.  */
	double_t ar, ar2, ar3, lo3, lo4;
	ar = A[0] * r; /* A[0] = -0.5.  */
	ar2 = r * ar;
	ar3 = r * ar2;
	/* k*Ln2 + log(c) + r + A[0]*r*r.  */
#if __FP_FAST_FMA
	hi = t2 + ar2;
	lo3 = __builtin_fma(ar, r, -ar2);
	lo4 = t2 - hi + ar2;
#else
	double_t arhi = A[0] * rhi;
	double_t arhi2 = rhi * arhi;
	hi = t2 + arhi2;
	lo3 = rlo * (ar + arhi);
	lo4 = t2 - hi + arhi2;
#endif
	/* p = log1p(r) - r - A[0]*r*r.  */
	p = (ar3 * (A[1] + r * A[2] +
		    ar2 * (A[3] + r * A[4] + ar2 * (A[5] + r * A[6]))));
	lo = lo1 + lo2 + lo3 + lo4 + p;
	y = hi + lo;
	*tail = hi - y + lo;
	return y;
}

#undef N
#undef T
#define N (1 << EXP_TABLE_BITS)
#define InvLn2N __exp_data.invln2N
#define NegLn2hiN __exp_data.negln2hiN
#define NegLn2loN __exp_data.negln2loN
#define Shift __exp_data.shift
#define T __exp_data.tab
#define C2 __exp_data.poly[5 - EXP_POLY_ORDER]
#define C3 __exp_data.poly[6 - EXP_POLY_ORDER]
#define C4 __exp_data.poly[7 - EXP_POLY_ORDER]
#define C5 __exp_data.poly[8 - EXP_POLY_ORDER]
#define C6 __exp_data.poly[9 - EXP_POLY_ORDER]

/* Handle cases that may overflow or underflow when computing the result that
   is scale*(1+TMP) without intermediate rounding.  The bit representation of
   scale is in SBITS, however it has a computed exponent that may have
   overflown into the sign bit so that needs to be adjusted before using it as
   a double.  (int32_t)KI is the k used in the argument reduction and exponent
   adjustment of scale, positive k here means the result may overflow and
   negative k means the result may underflow.  */
static inline double specialcase(double_t tmp, uint64_t sbits, uint64_t ki)
{
	double_t scale, y;

	if ((ki & 0x80000000) == 0) {
		/* k > 0, the exponent of scale might have overflowed by <= 460.  */
		sbits -= 1009ull << 52;
		scale = asdouble(sbits);
		y = 0x1p1009 * (scale + scale * tmp);
		return eval_as_double(y);
	}
	/* k < 0, need special care in the subnormal range.  */
	sbits += 1022ull << 52;
	/* Note: sbits is signed scale.  */
	scale = asdouble(sbits);
	y = scale + scale * tmp;
	if (fabs(y) < 1.0) {
		/* Round y to the right precision before scaling it into the subnormal
		   range to avoid double rounding that can cause 0.5+E/2 ulp error where
		   E is the worst-case ulp error outside the subnormal range.  So this
		   is only useful if the goal is better than 1 ulp worst-case error.  */
		double_t hi, lo, one = 1.0;
		if (y < 0.0)
			one = -1.0;
		lo = scale - y + scale * tmp;
		hi = one + y;
		lo = one - hi + y + lo;
		y = eval_as_double(hi + lo) - one;
		/* Fix the sign of 0.  */
		if (y == 0.0)
			y = asdouble(sbits & 0x8000000000000000);
		/* The underflow exception needs to be signaled explicitly.  */
		fp_force_eval(fp_barrier(0x1p-1022) * 0x1p-1022);
	}
	y = 0x1p-1022 * y;
	return eval_as_double(y);
}

#define SIGN_BIAS (0x800 << EXP_TABLE_BITS)

/* Computes sign*exp(x+xtail) where |xtail| < 2^-8/N and |xtail| <= |x|.
   The sign_bias argument is SIGN_BIAS or 0 and sets the sign to -1 or 1.  */
static inline double exp_inline(double_t x, double_t xtail, uint32_t sign_bias)
{
	uint32_t abstop;
	uint64_t ki, idx, top, sbits;
	/* double_t for better performance on targets with FLT_EVAL_METHOD==2.  */
	double_t kd, z, r, r2, scale, tail, tmp;

	abstop = top12(x) & 0x7ff;
	if (predict_false(abstop - top12(0x1p-54) >=
			  top12(512.0) - top12(0x1p-54))) {
		if (abstop - top12(0x1p-54) >= 0x80000000) {
			/* Avoid spurious underflow for tiny x.  */
			/* Note: 0 is common input.  */
			double_t one = WANT_ROUNDING ? 1.0 + x : 1.0;
			return sign_bias ? -one : one;
		}
		if (abstop >= top12(1024.0)) {
			/* Note: inf and nan are already handled.  */
			if (asuint64(x) >> 63)
				return __math_uflow(sign_bias);
			else
				return __math_oflow(sign_bias);
		}
		/* Large x is special cased below.  */
		abstop = 0;
	}

	/* exp(x) = 2^(k/N) * exp(r), with exp(r) in [2^(-1/2N),2^(1/2N)].  */
	/* x = ln2/N*k + r, with int k and r in [-ln2/2N, ln2/2N].  */
	z = InvLn2N * x;
#if TOINT_INTRINSICS
	kd = roundtoint(z);
	ki = converttoint(z);
#elif EXP_USE_TOINT_NARROW
	/* z - kd is in [-0.5-2^-16, 0.5] in all rounding modes.  */
	kd = eval_as_double(z + Shift);
	ki = asuint64(kd) >> 16;
	kd = (double_t)(int32_t)ki;
#else
	/* z - kd is in [-1, 1] in non-nearest rounding modes.  */
	kd = eval_as_double(z + Shift);
	ki = asuint64(kd);
	kd -= Shift;
#endif
	r = x + kd * NegLn2hiN + kd * NegLn2loN;
	/* The code assumes 2^-200 < |xtail| < 2^-8/N.  */
	r += xtail;
	/* 2^(k/N) ~= scale * (1 + tail).  */
	idx = 2 * (ki % N);
	top = (ki + sign_bias) << (52 - EXP_TABLE_BITS);
	tail = asdouble(T[idx]);
	/* This is only a valid scale when -1023*N < k < 1024*N.  */
	sbits = T[idx + 1] + top;
	/* exp(x) = 2^(k/N) * exp(r) ~= scale + scale * (tail + exp(r) - 1).  */
	/* Evaluation is optimized assuming superscalar pipelined execution.  */
	r2 = r * r;
	/* Without fma the worst case error is 0.25/N ulp larger.  */
	/* Worst case error is less than 0.5+1.11/N+(abs poly error * 2^53) ulp.  */
	tmp = tail + r + r2 * (C2 + r * C3) + r2 * r2 * (C4 + r * C5);
	if (predict_false(abstop == 0))
		return specialcase(tmp, sbits, ki);
	scale = asdouble(sbits);
	/* Note: tmp == 0 or |tmp| > 2^-200 and scale > 2^-739, so there
	   is no spurious underflow here even without fma.  */
	return eval_as_double(scale + scale * tmp);
}

/* Returns 0 if not int, 1 if odd int, 2 if even int.  The argument is
   the bit representation of a non-zero finite floating-point value.  */
static inline int checkint(uint64_t iy)
{
	int e = iy >> 52 & 0x7ff;
	if (e < 0x3ff)
		return 0;
	if (e > 0x3ff + 52)
		return 2;
	if (iy & ((1ULL << (0x3ff + 52 - e)) - 1))
		return 0;
	if (iy & (1ULL << (0x3ff + 52 - e)))
		return 1;
	return 2;
}

/* Returns 1 if input is the bit representation of 0, infinity or nan.  */
static inline int zeroinfnan(uint64_t i)
{
	return 2 * i - 1 >= 2 * asuint64(INFINITY) - 1;
}

double pow(double x, double y)
{
	uint32_t sign_bias = 0;
	uint64_t ix, iy;
	uint32_t topx, topy;

	ix = asuint64(x);
	iy = asuint64(y);
	topx = top12(x);
	topy = top12(y);
	if (predict_false(topx - 0x001 >= 0x7ff - 0x001 ||
			  (topy & 0x7ff) - 0x3be >= 0x43e - 0x3be)) {
		/* Note: if |y| > 1075 * ln2 * 2^53 ~= 0x1.749p62 then pow(x,y) = inf/0
		   and if |y| < 2^-54 / 1075 ~= 0x1.e7b6p-65 then pow(x,y) = +-1.  */
		/* Special cases: (x < 0x1p-126 or inf or nan) or
		   (|y| < 0x1p-65 or |y| >= 0x1p63 or nan).  */
		if (predict_false(zeroinfnan(iy))) {
			if (2 * iy == 0)
				return issignaling_inline(x) ? x + y : 1.0;
			if (ix == asuint64(1.0))
				return issignaling_inline(y) ? x + y : 1.0;
			if (2 * ix > 2 * asuint64(INFINITY) ||
			    2 * iy > 2 * asuint64(INFINITY))
				return x + y;
			if (2 * ix == 2 * asuint64(1.0))
				return 1.0;
			if ((2 * ix < 2 * asuint64(1.0)) == !(iy >> 63))
				return 0.0; /* |x|<1 && y==inf or |x|>1 && y==-inf.  */
			return y * y;
		}
		if (predict_false(zeroinfnan(ix))) {
			double_t x2 = x * x;
			if (ix >> 63 && checkint(iy) == 1)
				x2 = -x2;
			/* Without the barrier some versions of clang hoist the 1/x2 and
			   thus division by zero exception can be signaled spuriously.  */
			return iy >> 63 ? fp_barrier(1 / x2) : x2;
		}
		/* Here x and y are non-zero finite.  */
		if (ix >> 63) {
			/* Finite x < 0.  */
			int yint = checkint(iy);
			if (yint == 0)
				return __math_invalid(x);
			if (yint == 1)
				sign_bias = SIGN_BIAS;
			ix &= 0x7fffffffffffffff;
			topx &= 0x7ff;
		}
		if ((topy & 0x7ff) - 0x3be >= 0x43e - 0x3be) {
			/* Note: sign_bias == 0 here because y is not odd.  */
			if (ix == asuint64(1.0))
				return 1.0;
			if ((topy & 0x7ff) < 0x3be) {
				/* |y| < 2^-65, x^y ~= 1 + y*log(x).  */
				if (WANT_ROUNDING)
					return ix > asuint64(1.0) ? 1.0 + y :
								    1.0 - y;
				else
					return 1.0;
			}
			return (ix > asuint64(1.0)) == (topy < 0x800) ?
				       __math_oflow(0) :
				       __math_uflow(0);
		}
		if (topx == 0) {
			/* Normalize subnormal x so exponent becomes negative.  */
			ix = asuint64(x * 0x1p52);
			ix &= 0x7fffffffffffffff;
			ix -= 52ULL << 52;
		}
	}

	double_t lo;
	double_t hi = log_inline(ix, &lo);
	double_t ehi, elo;
#if __FP_FAST_FMA
	ehi = y * hi;
	elo = y * lo + __builtin_fma(y, hi, -ehi);
#else
	double_t yhi = asdouble(iy & -1ULL << 27);
	double_t ylo = y - yhi;
	double_t lhi = asdouble(asuint64(hi) & -1ULL << 27);
	double_t llo = hi - lhi + lo;
	ehi = yhi * lhi;
	elo = ylo * lhi + y * llo; /* |elo| < |ehi| * 2^-25.  */
#endif
	return exp_inline(ehi, elo, sign_bias);
}
