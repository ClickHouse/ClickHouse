/****************************  vectorclass.h   ********************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2014-10-24
* Version:       1.16
* Project:       vector classes
* Description:
* Header file defining vector classes as interface to intrinsic functions 
* in x86 microprocessors with SSE2 and later instruction sets up to AVX512.
*
* Instructions:
* Use Gnu, Clang, Intel or Microsoft C++ compiler. Compile for the desired 
* instruction set, which must be at least SSE2. Specify the supported 
* instruction set by a command line define, e.g. __SSE4_1__ if the 
* compiler does not automatically do so.
*
* Each vector object is represented internally in the CPU as a vector
* register with 128, 256 or 512 bits.
*
* This header file includes the appropriate header files depending on the
* supported instruction set
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2012 - 2014 GNU General Public License www.gnu.org/licenses
******************************************************************************/
#ifndef VECTORCLASS_H
#define VECTORCLASS_H  116

// Maximum vector size, bits. Allowed values are 128, 256, 512
#ifndef MAX_VECTOR_SIZE
#define MAX_VECTOR_SIZE 256
#endif

#include "instrset.h"        // Select supported instruction set

#if INSTRSET < 2             // SSE2 required
  #error Please compile for the SSE2 instruction set or higher
#else

#include "vectori128.h"      // 128-bit integer vectors
#include "vectorf128.h"      // 128-bit floating point vectors

#if MAX_VECTOR_SIZE >= 256
#if INSTRSET >= 8
  #include "vectori256.h"    // 256-bit integer vectors, requires AVX2 instruction set
#else
  #include "vectori256e.h"   // 256-bit integer vectors, emulated
#endif  // INSTRSET >= 8
#if INSTRSET >= 7
  #include "vectorf256.h"    // 256-bit floating point vectors, requires AVX instruction set
#else
  #include "vectorf256e.h"   // 256-bit floating point vectors, emulated
#endif  //  INSTRSET >= 7
#endif  //  MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
#if INSTRSET >= 9
  #include "vectori512.h"    // 512-bit integer vectors, requires AVX512 instruction set
  #include "vectorf512.h"    // 512-bit floating point vectors, requires AVX512 instruction set
#else
  #include "vectori512e.h"   // 512-bit integer vectors, emulated
  #include "vectorf512e.h"   // 512-bit floating point vectors, emulated
#endif  //  INSTRSET >= 9
#endif  //  MAX_VECTOR_SIZE >= 512

#endif  // INSTRSET < 2 

#endif  // VECTORCLASS_H
