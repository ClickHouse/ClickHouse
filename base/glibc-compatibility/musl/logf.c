/*
 * Single-precision log function.
 *
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */

#include <math.h>
#include <stdint.h>
#include "libm.h"
#include "logf_data.h"

/*
LOGF_TABLE_BITS = 4
LOGF_POLY_ORDER = 4

ULP error: 0.818 (nearest rounding.)
Relative error: 1.957 * 2^-26 (before rounding.)
*/

#define T __logf_data.tab
#define A __logf_data.poly
#define Ln2 __logf_data.ln2
#define N (1 << LOGF_TABLE_BITS)
#define OFF 0x3f330000

float logf(float x)
{
	double_t z, r, r2, y, y0, invc, logc;
	uint32_t ix, iz, tmp;
	int k, i;

	ix = asuint(x);
	/* Fix sign of zero with downward rounding when x==1.  */
	if (WANT_ROUNDING && predict_false(ix == 0x3f800000))
		return 0;
	if (predict_false(ix - 0x00800000 >= 0x7f800000 - 0x00800000)) {
		/* x < 0x1p-126 or inf or nan.  */
		if (ix * 2 == 0)
			return __math_divzerof(1);
		if (ix == 0x7f800000) /* log(inf) == inf.  */
			return x;
		if ((ix & 0x80000000) || ix * 2 >= 0xff000000)
			return __math_invalidf(x);
		/* x is subnormal, normalize it.  */
		ix = asuint(x * 0x1p23f);
		ix -= 23 << 23;
	}

	/* x = 2^k z; where z is in range [OFF,2*OFF] and exact.
	   The range is split into N subintervals.
	   The ith subinterval contains z and c is near its center.  */
	tmp = ix - OFF;
	i = (tmp >> (23 - LOGF_TABLE_BITS)) % N;
	k = (int32_t)tmp >> 23; /* arithmetic shift */
	iz = ix - (tmp & 0x1ff << 23);
	invc = T[i].invc;
	logc = T[i].logc;
	z = (double_t)asfloat(iz);

	/* log(x) = log1p(z/c-1) + log(c) + k*Ln2 */
	r = z * invc - 1;
	y0 = logc + (double_t)k * Ln2;

	/* Pipelined polynomial evaluation to approximate log1p(r).  */
	r2 = r * r;
	y = A[1] * r + A[2];
	y = A[0] * r2 + y;
	y = y * r2 + (y0 + r);
	return eval_as_float(y);
}
