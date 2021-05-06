/*
Copyright (c) 2016, Brian Marshall
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef LZSSE2_PLATFORM_H__
#define LZSSE2_PLATFORM_H__

#pragma once

/*
 Compiler/Platform detection based on the table from:
 https://blogs.msdn.microsoft.com/vcblog/2015/12/04/clang-with-microsoft-codegen-in-vs-2015-update-1/
*/

#ifdef	_MSC_VER

/*
 Microsoft Visual Studio Support.
 C1xx/C2, Clang/C2 and Clang/LLVM all support the Microsoft header files and _BitScanForward

 Note: if you receive errors with the intrinsics make sure that you have SSE4.1 support enabled.
 For example with Clang include "-msse4.1" on the command line
*/
#include <intrin.h>

#else	/* _MSC_VER */

#ifdef __GNUC__

/*
 GCC
*/

/*
 Note: including just <smmintrin.h> would be sufficient, but including x86intrin is a better match to intrin.h on Visual Studio as
 both include all intrinsics for the enabled processor, rather than just SSE4.1.
*/
#include <x86intrin.h>
/* _BitScanForward is Visual Studio specific. */
#define _BitScanForward(x, m) *(x) = __builtin_ctz(m)

#else

/*
If you hit the error below, then add detection for your compiler/platform to this header file.
*/
#error Platform not supported

#endif	/* __GNUC__ */
#endif  /* _MSC_VER */

#endif /* -- LZSSE2_PLATFORM_H__ */
