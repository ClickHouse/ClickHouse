#pragma once

/* librseq headers use glibc's __GNUC_PREREQ from <features.h>, which musl does
 * not provide. Force-included (-include) for librseq and its users under musl. */
#ifndef __GNUC_PREREQ
#    define __GNUC_PREREQ(maj, min) ((__GNUC__ << 16) + __GNUC_MINOR__ >= (((maj) << 16) + (min)))
#endif
