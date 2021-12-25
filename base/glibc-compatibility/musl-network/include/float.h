#ifndef _FLOAT_H
#define _FLOAT_H

#ifdef __cplusplus
extern "C" {
#endif

int __flt_rounds(void);
#define FLT_ROUNDS (__flt_rounds())

#define FLT_RADIX 2

#define FLT_TRUE_MIN 1.40129846432481707092e-45F
#define FLT_MIN 1.17549435082228750797e-38F
#define FLT_MAX 3.40282346638528859812e+38F
#define FLT_EPSILON 1.1920928955078125e-07F

#define FLT_MANT_DIG 24
#define FLT_MIN_EXP (-125)
#define FLT_MAX_EXP 128
#define FLT_HAS_SUBNORM 1

#define FLT_DIG 6
#define FLT_DECIMAL_DIG 9
#define FLT_MIN_10_EXP (-37)
#define FLT_MAX_10_EXP 38

#define DBL_TRUE_MIN 4.94065645841246544177e-324
#define DBL_MIN 2.22507385850720138309e-308
#define DBL_MAX 1.79769313486231570815e+308
#define DBL_EPSILON 2.22044604925031308085e-16

#define DBL_MANT_DIG 53
#define DBL_MIN_EXP (-1021)
#define DBL_MAX_EXP 1024
#define DBL_HAS_SUBNORM 1

#define DBL_DIG 15
#define DBL_DECIMAL_DIG 17
#define DBL_MIN_10_EXP (-307)
#define DBL_MAX_10_EXP 308

#define LDBL_HAS_SUBNORM 1
#define LDBL_DECIMAL_DIG DECIMAL_DIG

#include <bits/float.h>

#ifdef __cplusplus
}
#endif

#endif
