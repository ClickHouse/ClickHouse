#pragma once

/* Wraps musl's <features.h> for librseq and every target that links it (see
 * contrib/librseq-cmake/CMakeLists.txt). librseq's arch/<arch>/thread-pointer.h
 * includes <features.h> and tests glibc's __GNUC_PREREQ, which musl does not
 * provide. Drop this once librseq checks `defined(__GNUC_PREREQ)` itself. */
#include_next <features.h>

#ifndef __GNUC_PREREQ
#    define __GNUC_PREREQ(maj, min) ((__GNUC__ << 16) + __GNUC_MINOR__ >= (((maj) << 16) + (min)))
#endif
