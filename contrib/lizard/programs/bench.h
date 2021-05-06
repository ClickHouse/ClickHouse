/*
    bench.h - Demo program to benchmark open-source compression algorithm
    Copyright (C) Yann Collet 2012-2016
    Copyright (C) Przemyslaw Skibinski 2016-2017

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    You can contact the author at :
    - Lizard source repository : https://github.com/inikep/lizard
*/
#ifndef BENCH_H_12
#define BENCH_H_12

#include <stddef.h>

int BMK_benchFiles(const char** fileNamesTable, unsigned nbFiles,
                   int cLevel, int cLevelLast);

/* Set Parameters */
void BMK_SetNbSeconds(unsigned nbLoops);
void BMK_SetBlockSize(size_t blockSize);
void BMK_setAdditionalParam(int additionalParam);
void BMK_setNotificationLevel(unsigned level);

#endif   /* BENCH_H_125623623633 */
