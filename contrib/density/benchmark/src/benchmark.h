/*
 * Density benchmark
 *
 * Copyright (c) 2015, Guillaume Voirin
 * All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * 6/04/15 0:11
 */

#ifndef DENSITY_BENCHMARK_H
#define DENSITY_BENCHMARK_H

#define _CRT_SECURE_NO_DEPRECATE

#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <time.h>
#include "../../src/density_api.h"
#include "../libs/cputime/src/cputime_api.h"
#include "../libs/spookyhash/src/spookyhash_api.h"

#if defined(_WIN64) || defined(_WIN32)
#else
#define DENSITY_BENCHMARK_ALLOW_ANSI_ESCAPE_SEQUENCES
#endif

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define DENSITY_BENCHMARK_ENDIAN_STRING           "Little"
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define DENSITY_BENCHMARK_ENDIAN_STRING           "Big"
#endif

#if defined(__clang__)
#define DENSITY_BENCHMARK_COMPILER                "Clang %d.%d.%d"
#define DENSITY_BENCHMARK_COMPILER_VERSION        __clang_major__, __clang_minor__, __clang_patchlevel__
#elif defined(__GNUC__)
#define DENSITY_BENCHMARK_COMPILER                "GCC %d.%d.%d"
#define DENSITY_BENCHMARK_COMPILER_VERSION        __GNUC__, __GNUC_MINOR__, __GNUC_PATCHLEVEL__
#elif defined(_MSC_VER)
#define DENSITY_BENCHMARK_COMPILER                "MSVC"
#define DENSITY_BENCHMARK_COMPILER_VERSION		  ""
#elif defined(__INTEL_COMPILER)
#define DENSITY_BENCHMARK_COMPILER                "ICC"
#define DENSITY_BENCHMARK_COMPILER_VERSION		  ""
#else
#define DENSITY_BENCHMARK_COMPILER                "an unknown compiler"
#define DENSITY_BENCHMARK_COMPILER_VERSION		  ""
#endif

#if defined(_WIN64) || defined(_WIN32)
#define DENSITY_BENCHMARK_PLATFORM_STRING         "Microsoft Windows"
#elif defined(__APPLE__)
#include "TargetConditionals.h"
#if TARGET_IPHONE_SIMULATOR
#define DENSITY_BENCHMARK_PLATFORM_STRING         "iOS Simulator"
#elif TARGET_OS_IPHONE
#define DENSITY_BENCHMARK_PLATFORM_STRING         "iOS"
#elif TARGET_OS_MAC
#define DENSITY_BENCHMARK_PLATFORM_STRING         "MacOS"
#else
#define DENSITY_BENCHMARK_PLATFORM_STRING         "an unknown Apple platform"
#endif
#elif defined(__FreeBSD__)
#define DENSITY_BENCHMARK_PLATFORM_STRING         "FreeBSD"
#elif defined(__linux__)
#define DENSITY_BENCHMARK_PLATFORM_STRING         "GNU/Linux"
#elif defined(__unix__)
#define DENSITY_BENCHMARK_PLATFORM_STRING         "Unix"
#elif defined(__posix__)
#define DENSITY_BENCHMARK_PLATFORM_STRING         "Posix"
#else
#define DENSITY_BENCHMARK_PLATFORM_STRING         "an unknown platform"
#endif

#define DENSITY_ESCAPE_CHARACTER            ((char)27)

#ifdef DENSITY_BENCHMARK_ALLOW_ANSI_ESCAPE_SEQUENCES
#define DENSITY_BENCHMARK_BOLD(op)          printf("%c[1m", DENSITY_ESCAPE_CHARACTER);\
                                            op;\
                                            printf("%c[0m", DENSITY_ESCAPE_CHARACTER);

#define DENSITY_BENCHMARK_BLUE(op)          printf("%c[0;34m", DENSITY_ESCAPE_CHARACTER);\
                                            op;\
                                            printf("%c[0m", DENSITY_ESCAPE_CHARACTER);

#define DENSITY_BENCHMARK_RED(op)           printf("%c[0;31m", DENSITY_ESCAPE_CHARACTER);\
                                            op;\
                                            printf("%c[0m", DENSITY_ESCAPE_CHARACTER);
#else
#define DENSITY_BENCHMARK_BOLD(op)          op;
#define DENSITY_BENCHMARK_BLUE(op)          op;
#define DENSITY_BENCHMARK_RED(op)           op;
#endif

#define DENSITY_BENCHMARK_UNDERLINE(n)      printf("\n");\
                                            for(int i = 0; i < n; i++)  printf("=");

#define DENSITY_BENCHMARK_ERROR(op, issue)  DENSITY_BENCHMARK_RED(DENSITY_BENCHMARK_BOLD(printf("\nAn error has occured !\n")));\
                                            op;\
                                            printf("\n");\
                                            if(issue) {\
                                                printf("Please open an issue at <https://github.com/k0dai/density/issues>, with your platform information and any relevant file.\n");\
                                                DENSITY_BENCHMARK_BOLD(printf("Thank you !\n"));\
                                            }\
                                            fflush(stdout);\
                                            exit(EXIT_FAILURE);

#endif

#define DENSITY_BENCHMARK_HASH_SEED_1       0x0123456789abcdefllu
#define DENSITY_BENCHMARK_HASH_SEED_2       0xfedcba9876543210llu
