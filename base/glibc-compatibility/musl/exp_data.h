/*
 * Copyright (c) 2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */
#ifndef _EXP_DATA_H
#define _EXP_DATA_H

#include "musl_features.h"
#include <stdint.h>

#define EXP_TABLE_BITS 7
#define EXP_POLY_ORDER 5
#define EXP_USE_TOINT_NARROW 0
#define EXP2_POLY_ORDER 5
extern hidden const struct exp_data {
	double invln2N;
	double shift;
	double negln2hiN;
	double negln2loN;
	double poly[4]; /* Last four coefficients.  */
	double exp2_shift;
	double exp2_poly[EXP2_POLY_ORDER];
	uint64_t tab[2*(1 << EXP_TABLE_BITS)];
} __exp_data;

#endif
