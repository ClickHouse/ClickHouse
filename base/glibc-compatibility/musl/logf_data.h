/*
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */
#ifndef _LOGF_DATA_H
#define _LOGF_DATA_H

#include "musl_features.h"

#define LOGF_TABLE_BITS 4
#define LOGF_POLY_ORDER 4
extern hidden const struct logf_data {
	struct {
		double invc, logc;
	} tab[1 << LOGF_TABLE_BITS];
	double ln2;
	double poly[LOGF_POLY_ORDER - 1]; /* First order coefficient is 1.  */
} __logf_data;

#endif
