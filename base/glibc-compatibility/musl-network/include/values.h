#ifndef _VALUES_H
#define _VALUES_H

#include <limits.h>

#define CHARBITS   (sizeof(char)   * 8)
#define SHORTBITS  (sizeof(short)  * 8)
#define INTBITS    (sizeof(int)    * 8)
#define LONGBITS   (sizeof(long)   * 8)
#define PTRBITS    (sizeof(char *) * 8)
#define DOUBLEBITS (sizeof(double) * 8)
#define FLOATBITS  (sizeof(float)  * 8)

#define MINSHORT SHRT_MIN
#define MININT   INT_MIN
#define MINLONG  LONG_MIN

#define MAXSHORT SHRT_MAX
#define MAXINT   INT_MAX
#define MAXLONG  LONG_MAX

#define HIBITS   MINSHORT
#define HIBITL   MINLONG

#include <float.h>

#define MAXDOUBLE DBL_MAX
#undef  MAXFLOAT
#define MAXFLOAT  FLT_MAX
#define MINDOUBLE DBL_MIN
#define MINFLOAT  FLT_MIN
#define DMINEXP   DBL_MIN_EXP
#define FMINEXP   FLT_MIN_EXP
#define DMAXEXP   DBL_MAX_EXP
#define FMAXEXP   FLT_MAX_EXP

#define BITSPERBYTE CHAR_BIT

#endif
