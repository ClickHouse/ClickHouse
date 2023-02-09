/*************************************************
*      Perl-Compatible Regular Expressions       *
*************************************************/

/* PCRE is a library of functions to support regular expressions whose syntax
and semantics are as close as possible to those of the Perl 5 language.

                       Written by Philip Hazel
           Copyright (c) 1997-2012 University of Cambridge

-----------------------------------------------------------------------------
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    * Neither the name of the University of Cambridge nor the names of its
      contributors may be used to endorse or promote products derived from
      this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
-----------------------------------------------------------------------------
*/


/* This module contains the external function pcre_maketables(), which builds
character tables for PCRE in the current locale. The file is compiled on its
own as part of the PCRE library. However, it is also included in the
compilation of dftables.c, in which case the macro DFTABLES is defined. */

#pragma warning( disable : 4244)  // conversion from 'int' to 'unsigned short', possible loss of data

#ifndef DFTABLES
#  include "pcre_config.h"
#  include "pcre_internal.h"
#endif


/*************************************************
*           Create PCRE character tables         *
*************************************************/

/* This function builds a set of character tables for use by PCRE and returns
a pointer to them. They are build using the ctype functions, and consequently
their contents will depend upon the current locale setting. When compiled as
part of the library, the store is obtained via PUBL(malloc)(), but when
compiled inside dftables, use malloc().

Arguments:   none
Returns:     pointer to the contiguous block of data
*/

#if defined COMPILE_PCRE8
const unsigned char *
pcre_maketables(void)
#elif defined COMPILE_PCRE16
const unsigned char *
pcre16_maketables(void)
#elif defined COMPILE_PCRE32
const unsigned char *
pcre32_maketables(void)
#endif
{
unsigned char *yield, *p;
int i;

#ifndef DFTABLES
yield = (unsigned char*)(PUBL(malloc))(tables_length);
#else
yield = (unsigned char*)malloc(tables_length);
#endif

if (yield == NULL) return NULL;
p = yield;

/* First comes the lower casing table */

for (i = 0; i < 256; i++) *p++ = tolower(i);

/* Next the case-flipping table */

for (i = 0; i < 256; i++) *p++ = islower(i)? toupper(i) : tolower(i);

/* Then the character class tables. Don't try to be clever and save effort on
exclusive ones - in some locales things may be different.

Note that the table for "space" includes everything "isspace" gives, including
VT in the default locale. This makes it work for the POSIX class [:space:].
From release 8.34 is is also correct for Perl space, because Perl added VT at
release 5.18.

Note also that it is possible for a character to be alnum or alpha without
being lower or upper, such as "male and female ordinals" (\xAA and \xBA) in the
fr_FR locale (at least under Debian Linux's locales as of 12/2005). So we must
test for alnum specially. */

memset(p, 0, cbit_length);
for (i = 0; i < 256; i++)
  {
  if (isdigit(i)) p[cbit_digit  + i/8] |= 1 << (i&7);
  if (isupper(i)) p[cbit_upper  + i/8] |= 1 << (i&7);
  if (islower(i)) p[cbit_lower  + i/8] |= 1 << (i&7);
  if (isalnum(i)) p[cbit_word   + i/8] |= 1 << (i&7);
  if (i == '_')   p[cbit_word   + i/8] |= 1 << (i&7);
  if (isspace(i)) p[cbit_space  + i/8] |= 1 << (i&7);
  if (isxdigit(i))p[cbit_xdigit + i/8] |= 1 << (i&7);
  if (isgraph(i)) p[cbit_graph  + i/8] |= 1 << (i&7);
  if (isprint(i)) p[cbit_print  + i/8] |= 1 << (i&7);
  if (ispunct(i)) p[cbit_punct  + i/8] |= 1 << (i&7);
  if (iscntrl(i)) p[cbit_cntrl  + i/8] |= 1 << (i&7);
  }
p += cbit_length;

/* Finally, the character type table. In this, we used to exclude VT from the
white space chars, because Perl didn't recognize it as such for \s and for
comments within regexes. However, Perl changed at release 5.18, so PCRE changed
at release 8.34. */

for (i = 0; i < 256; i++)
  {
  int x = 0;
  if (isspace(i)) x += ctype_space;
  if (isalpha(i)) x += ctype_letter;
  if (isdigit(i)) x += ctype_digit;
  if (isxdigit(i)) x += ctype_xdigit;
  if (isalnum(i) || i == '_') x += ctype_word;

  /* Note: strchr includes the terminating zero in the characters it considers.
  In this instance, that is ok because we want binary zero to be flagged as a
  meta-character, which in this sense is any character that terminates a run
  of data characters. */

  if (strchr("\\*+?{^.$|()[", i) != 0) x += ctype_meta;
  *p++ = x;
  }

return yield;
}

/* End of pcre_maketables.c */
