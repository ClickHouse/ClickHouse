/*
                            __  __            _
                         ___\ \/ /_ __   __ _| |_
                        / _ \\  /| '_ \ / _` | __|
                       |  __//  \| |_) | (_| | |_
                        \___/_/\_\ .__/ \__,_|\__|
                                 |_| XML parser

   Copyright (c) 2026 Sebastian Pipping <sebastian@pipping.org>
   Licensed under the MIT license:

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software  and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to permit
   persons to whom  the Software is furnished to do so, subject to the
   following conditions:

   The above copyright notice and this permission notice shall be included
   in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS  IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
   NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
   DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
   OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
   USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#if ! defined(EXPAT_XCS_H)

#  include <stddef.h>                  // size_t
#  include "Poco/XML/expat_external.h" // for  XML_Char

// `xcs.c` is compiled as C while `xmlparse.cpp` is compiled as C++, so the
// declarations need C linkage to match the definitions.
#ifdef __cplusplus
extern "C" {
#endif

extern size_t xcslen(const XML_Char *s);

extern int xcscmp(const XML_Char *a, const XML_Char *b);
extern int xcsncmp(const XML_Char *a, const XML_Char *b, size_t len);

#ifdef __cplusplus
}
#endif

#endif // ! defined(EXPAT_XCS_H)
