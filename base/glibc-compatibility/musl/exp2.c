/*
 * Double-precision 2^x function.
 *
 * Copyright (c) 2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#include <math.h>
#include <stdint.h>
#include "libm.h"
#include "exp_data.h"

#define N (1 << EXP_TABLE_BITS)
#define Shift __exp_data.exp2_shift
#define T __exp_data.tab
#define C1 __exp_data.exp2_poly[0]
#define C2 __exp_data.exp2_poly[1]
#define C3 __exp_data.exp2_poly[2]
#define C4 __exp_data.exp2_poly[3]
#define C5 __exp_data.exp2_poly[4]

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
		/* k > 0, the exponent of scale might have overflowed by 1.  */
		sbits -= 1ull << 52;
		scale = asdouble(sbits);
		y = 2 * (scale + scale * tmp);
		return eval_as_double(y);
	}
	/* k < 0, need special care in the subnormal range.  */
	sbits += 1022ull << 52;
	scale = asdouble(sbits);
	y = scale + scale * tmp;
	if (y < 1.0) {
		/* Round y to the right precision before scaling it into the subnormal
		   range to avoid double rounding that can cause 0.5+E/2 ulp error where
		   E is the worst-case ulp error outside the subnormal range.  So this
		   is only useful if the goal is better than 1 ulp worst-case error.  */
		double_t hi, lo;
		lo = scale - y + scale * tmp;
		hi = 1.0 + y;
		lo = 1.0 - hi + y + lo;
		y = eval_as_double(hi + lo) - 1.0;
		/* Avoid -0.0 with downward rounding.  */
		if (WANT_ROUNDING && y == 0.0)
			y = 0.0;
		/* The underflow exception needs to be signaled explicitly.  */
		fp_force_eval(fp_barrier(0x1p-1022) * 0x1p-1022);
	}
	y = 0x1p-1022 * y;
	return eval_as_double(y);
}

/* Top 12 bits of a double (sign and exponent bits).  */
static inline uint32_t top12(double x)
{
	return asuint64(x) >> 52;
}

double exp2(double x)
{
	uint32_t abstop;
	uint64_t ki, idx, top, sbits;
	double_t kd, r, r2, scale, tail, tmp;

	abstop = top12(x) & 0x7ff;
	if (predict_false(abstop - top12(0x1p-54) >= top12(512.0) - top12(0x1p-54))) {
		if (abstop - top12(0x1p-54) >= 0x80000000)
			/* Avoid spurious underflow for tiny x.  */
			/* Note: 0 is common input.  */
			return WANT_ROUNDING ? 1.0 + x : 1.0;
		if (abstop >= top12(1024.0)) {
			if (asuint64(x) == asuint64(-INFINITY))
				return 0.0;
			if (abstop >= top12(INFINITY))
				return 1.0 + x;
			if (!(asuint64(x) >> 63))
				return __math_oflow(0);
			else if (asuint64(x) >= asuint64(-1075.0))
				return __math_uflow(0);
		}
		if (2 * asuint64(x) > 2 * asuint64(928.0))
			/* Large x is special cased below.  */
			abstop = 0;
	}

	/* exp2(x) = 2^(k/N) * 2^r, with 2^r in [2^(-1/2N),2^(1/2N)].  */
	/* x = k/N + r, with int k and r in [-1/2N, 1/2N].  */
	kd = eval_as_double(x + Shift);
	ki = asuint64(kd); /* k.  */
	kd -= Shift; /* k/N for int k.  */
	r = x - kd;
	/* 2^(k/N) ~= scale * (1 + tail).  */
	idx = 2 * (ki % N);
	top = ki << (52 - EXP_TABLE_BITS);
	tail = asdouble(T[idx]);
	/* This is only a valid scale when -1023*N < k < 1024*N.  */
	sbits = T[idx + 1] + top;
	/* exp2(x) = 2^(k/N) * 2^r ~= scale + scale * (tail + 2^r - 1).  */
	/* Evaluation is optimized assuming superscalar pipelined execution.  */
	r2 = r * r;
	/* Without fma the worst case error is 0.5/N ulp larger.  */
	/* Worst case error is less than 0.5+0.86/N+(abs poly error * 2^53) ulp.  */
	tmp = tail + r * C1 + r2 * (C2 + r * C3) + r2 * r2 * (C4 + r * C5);
	if (predict_false(abstop == 0))
		return specialcase(tmp, sbits, ki);
	scale = asdouble(sbits);
	/* Note: tmp == 0 or |tmp| > 2^-65 and scale > 2^-928, so there
	   is no spurious underflow here even without fma.  */
	return eval_as_double(scale + scale * tmp);
}
