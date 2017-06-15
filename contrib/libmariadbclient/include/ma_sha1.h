/****************************************************************************
   Copyright (C) 2012 Monty Program AB
                 2016 MariaDB Corporation AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*****************************************************************************/

/* This code came from the PHP project, initially written by
   Stefan Esser */

#ifndef SHA1_H
#define SHA1_H

#define SHA1_MAX_LENGTH 20
#define SCRAMBLE_LENGTH 20
#define SCRAMBLE_LENGTH_323 8

/* SHA1 context. */
typedef struct {
	uint32 state[5];		/* state (ABCD) */
	uint32 count[2];		/* number of bits, modulo 2^64 (lsb first) */
	unsigned char buffer[64];	/* input buffer */
} _MA_SHA1_CTX;

void ma_SHA1Init(_MA_SHA1_CTX *);
void ma_SHA1Update(_MA_SHA1_CTX *, const unsigned char *, size_t);
void ma_SHA1Final(unsigned char[20], _MA_SHA1_CTX *);

#endif
