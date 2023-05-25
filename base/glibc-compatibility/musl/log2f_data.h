/*
 * Copyright (c) 2017-2018, Arm Limited.
 * SPDX-License-Identifier: MIT
 */
#ifndef _LOG2F_DATA_H
#define _LOG2F_DATA_H

#include "musl_features.h"

#define LOG2F_TABLE_BITS 4
#define LOG2F_POLY_ORDER 4
extern hidden const struct log2f_data {
	struct {
		double invc, logc;
	} tab[1 << LOG2F_TABLE_BITS];
	double poly[LOG2F_POLY_ORDER];
} __log2f_data;

#endif
