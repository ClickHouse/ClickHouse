/*
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */
#ifndef _POWF_DATA_H
#define _POWF_DATA_H

#include "libm.h"
#include "exp2f_data.h"

#define POWF_LOG2_TABLE_BITS 4
#define POWF_LOG2_POLY_ORDER 5
#if TOINT_INTRINSICS
#define POWF_SCALE_BITS EXP2F_TABLE_BITS
#else
#define POWF_SCALE_BITS 0
#endif
#define POWF_SCALE ((double)(1 << POWF_SCALE_BITS))
extern hidden const struct powf_log2_data {
	struct {
		double invc, logc;
	} tab[1 << POWF_LOG2_TABLE_BITS];
	double poly[POWF_LOG2_POLY_ORDER];
} __powf_log2_data;

#endif
