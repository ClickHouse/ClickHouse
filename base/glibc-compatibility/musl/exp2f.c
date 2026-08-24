/*
 * Single-precision 2^x function.
 *
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#include <math.h>
#include <stdint.h>
#include "libm.h"
#include "exp2f_data.h"

/*
EXP2F_TABLE_BITS = 5
EXP2F_POLY_ORDER = 3

ULP error: 0.502 (nearest rounding.)
Relative error: 1.69 * 2^-34 in [-1/64, 1/64] (before rounding.)
Wrong count: 168353 (all nearest rounding wrong results with fma.)
Non-nearest ULP error: 1 (rounded ULP error)
*/

#define N (1 << EXP2F_TABLE_BITS)
#define T __exp2f_data.tab
#define C __exp2f_data.poly
#define SHIFT __exp2f_data.shift_scaled

static inline uint32_t top12(float x)
{
	return asuint(x) >> 20;
}

float exp2f(float x)
{
	uint32_t abstop;
	uint64_t ki, t;
	double_t kd, xd, z, r, r2, y, s;

	xd = (double_t)x;
	abstop = top12(x) & 0x7ff;
	if (predict_false(abstop >= top12(128.0f))) {
		/* |x| >= 128 or x is nan.  */
		if (asuint(x) == asuint(-INFINITY))
			return 0.0f;
		if (abstop >= top12(INFINITY))
			return x + x;
		if (x > 0.0f)
			return __math_oflowf(0);
		if (x <= -150.0f)
			return __math_uflowf(0);
	}

	/* x = k/N + r with r in [-1/(2N), 1/(2N)] and int k.  */
	kd = eval_as_double(xd + SHIFT);
	ki = asuint64(kd);
	kd -= SHIFT; /* k/N for int k.  */
	r = xd - kd;

	/* exp2(x) = 2^(k/N) * 2^r ~= s * (C0*r^3 + C1*r^2 + C2*r + 1) */
	t = T[ki % N];
	t += ki << (52 - EXP2F_TABLE_BITS);
	s = asdouble(t);
	z = C[0] * r + C[1];
	r2 = r * r;
	y = C[2] * r + 1;
	y = z * r2 + y;
	y = y * s;
	return eval_as_float(y);
}
