include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_definitions( -Wno-shift-negative-value )

add_library(vectorclass
	instrset_detect.cpp

	instrset.h
	vectorclass.h
	vectorf128.h
	vectorf256e.h
	vectorf256.h
	vectorf512e.h
	vectorf512.h
	vectori128.h
	vectori256e.h
	vectori256.h
	vectori512e.h
	vectori512.h
	vectormath_common.h
	vectormath_exp.h
	vectormath_hyp.h
	vectormath_lib.h
	vectormath_trig.h)
