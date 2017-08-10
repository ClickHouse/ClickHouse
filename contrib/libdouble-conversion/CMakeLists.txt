add_library (double-conversion
double-conversion/bignum.cc
double-conversion/bignum-dtoa.cc
double-conversion/bignum-dtoa.h
double-conversion/bignum.h
double-conversion/cached-powers.cc
double-conversion/cached-powers.h
double-conversion/diy-fp.cc
double-conversion/diy-fp.h
double-conversion/double-conversion.cc
double-conversion/double-conversion.h
double-conversion/fast-dtoa.cc
double-conversion/fast-dtoa.h
double-conversion/fixed-dtoa.cc
double-conversion/fixed-dtoa.h
double-conversion/ieee.h
double-conversion/strtod.cc
double-conversion/strtod.h
double-conversion/utils.h
)

target_include_directories (double-conversion PUBLIC .)
