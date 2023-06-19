/*************************************************
*      Perl-Compatible Regular Expressions       *
*************************************************/

/* PCRE is a library of functions to support regular expressions whose syntax
and semantics are as close as possible to those of the Perl 5 language.

                       Written by Philip Hazel
           Copyright (c) 1997-2013 University of Cambridge

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


/* This module contains an internal function for validating UTF-8 character
strings. */


#include "pcre_config.h"
#include "pcre_internal.h"


/*************************************************
*         Validate a UTF-8 string                *
*************************************************/

/* This function is called (optionally) at the start of compile or match, to
check that a supposed UTF-8 string is actually valid. The early check means
that subsequent code can assume it is dealing with a valid string. The check
can be turned off for maximum performance, but the consequences of supplying an
invalid string are then undefined.

Originally, this function checked according to RFC 2279, allowing for values in
the range 0 to 0x7fffffff, up to 6 bytes long, but ensuring that they were in
the canonical format. Once somebody had pointed out RFC 3629 to me (it
obsoletes 2279), additional restrictions were applied. The values are now
limited to be between 0 and 0x0010ffff, no more than 4 bytes long, and the
subrange 0xd000 to 0xdfff is excluded. However, the format of 5-byte and 6-byte
characters is still checked.

From release 8.13 more information about the details of the error are passed
back in the returned value:

PCRE_UTF8_ERR0   No error
PCRE_UTF8_ERR1   Missing 1 byte at the end of the string
PCRE_UTF8_ERR2   Missing 2 bytes at the end of the string
PCRE_UTF8_ERR3   Missing 3 bytes at the end of the string
PCRE_UTF8_ERR4   Missing 4 bytes at the end of the string
PCRE_UTF8_ERR5   Missing 5 bytes at the end of the string
PCRE_UTF8_ERR6   2nd-byte's two top bits are not 0x80
PCRE_UTF8_ERR7   3rd-byte's two top bits are not 0x80
PCRE_UTF8_ERR8   4th-byte's two top bits are not 0x80
PCRE_UTF8_ERR9   5th-byte's two top bits are not 0x80
PCRE_UTF8_ERR10  6th-byte's two top bits are not 0x80
PCRE_UTF8_ERR11  5-byte character is not permitted by RFC 3629
PCRE_UTF8_ERR12  6-byte character is not permitted by RFC 3629
PCRE_UTF8_ERR13  4-byte character with value > 0x10ffff is not permitted
PCRE_UTF8_ERR14  3-byte character with value 0xd000-0xdfff is not permitted
PCRE_UTF8_ERR15  Overlong 2-byte sequence
PCRE_UTF8_ERR16  Overlong 3-byte sequence
PCRE_UTF8_ERR17  Overlong 4-byte sequence
PCRE_UTF8_ERR18  Overlong 5-byte sequence (won't ever occur)
PCRE_UTF8_ERR19  Overlong 6-byte sequence (won't ever occur)
PCRE_UTF8_ERR20  Isolated 0x80 byte (not within UTF-8 character)
PCRE_UTF8_ERR21  Byte with the illegal value 0xfe or 0xff
PCRE_UTF8_ERR22  Unused (was non-character)

Arguments:
  string       points to the string
  length       length of string, or -1 if the string is zero-terminated
  errp         pointer to an error position offset variable

Returns:       = 0    if the string is a valid UTF-8 string
               > 0    otherwise, setting the offset of the bad character
*/

int
PRIV(valid_utf)(PCRE_PUCHAR string, int length, int *erroroffset)
{
#ifdef SUPPORT_UTF
register PCRE_PUCHAR p;

if (length < 0)
  {
  for (p = string; *p != 0; p++);
  length = (int)(p - string);
  }

for (p = string; length-- > 0; p++)
  {
  register pcre_uchar ab, c, d;

  c = *p;
  if (c < 128) continue;                /* ASCII character */

  if (c < 0xc0)                         /* Isolated 10xx xxxx byte */
    {
    *erroroffset = (int)(p - string);
    return PCRE_UTF8_ERR20;
    }

  if (c >= 0xfe)                        /* Invalid 0xfe or 0xff bytes */
    {
    *erroroffset = (int)(p - string);
    return PCRE_UTF8_ERR21;
    }

  ab = PRIV(utf8_table4)[c & 0x3f];     /* Number of additional bytes */
  if (length < ab)
    {
    *erroroffset = (int)(p - string);          /* Missing bytes */
    return ab - length;                 /* Codes ERR1 to ERR5 */
    }
  length -= ab;                         /* Length remaining */

  /* Check top bits in the second byte */

  if (((d = *(++p)) & 0xc0) != 0x80)
    {
    *erroroffset = (int)(p - string) - 1;
    return PCRE_UTF8_ERR6;
    }

  /* For each length, check that the remaining bytes start with the 0x80 bit
  set and not the 0x40 bit. Then check for an overlong sequence, and for the
  excluded range 0xd800 to 0xdfff. */

  switch (ab)
    {
    /* 2-byte character. No further bytes to check for 0x80. Check first byte
    for for xx00 000x (overlong sequence). */

    case 1: if ((c & 0x3e) == 0)
      {
      *erroroffset = (int)(p - string) - 1;
      return PCRE_UTF8_ERR15;
      }
    break;

    /* 3-byte character. Check third byte for 0x80. Then check first 2 bytes
      for 1110 0000, xx0x xxxx (overlong sequence) or
          1110 1101, 1010 xxxx (0xd800 - 0xdfff) */

    case 2:
    if ((*(++p) & 0xc0) != 0x80)     /* Third byte */
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR7;
      }
    if (c == 0xe0 && (d & 0x20) == 0)
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR16;
      }
    if (c == 0xed && d >= 0xa0)
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR14;
      }
    break;

    /* 4-byte character. Check 3rd and 4th bytes for 0x80. Then check first 2
       bytes for for 1111 0000, xx00 xxxx (overlong sequence), then check for a
       character greater than 0x0010ffff (f4 8f bf bf) */

    case 3:
    if ((*(++p) & 0xc0) != 0x80)     /* Third byte */
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR7;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Fourth byte */
      {
      *erroroffset = (int)(p - string) - 3;
      return PCRE_UTF8_ERR8;
      }
    if (c == 0xf0 && (d & 0x30) == 0)
      {
      *erroroffset = (int)(p - string) - 3;
      return PCRE_UTF8_ERR17;
      }
    if (c > 0xf4 || (c == 0xf4 && d > 0x8f))
      {
      *erroroffset = (int)(p - string) - 3;
      return PCRE_UTF8_ERR13;
      }
    break;

    /* 5-byte and 6-byte characters are not allowed by RFC 3629, and will be
    rejected by the length test below. However, we do the appropriate tests
    here so that overlong sequences get diagnosed, and also in case there is
    ever an option for handling these larger code points. */

    /* 5-byte character. Check 3rd, 4th, and 5th bytes for 0x80. Then check for
    1111 1000, xx00 0xxx */

    case 4:
    if ((*(++p) & 0xc0) != 0x80)     /* Third byte */
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR7;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Fourth byte */
      {
      *erroroffset = (int)(p - string) - 3;
      return PCRE_UTF8_ERR8;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Fifth byte */
      {
      *erroroffset = (int)(p - string) - 4;
      return PCRE_UTF8_ERR9;
      }
    if (c == 0xf8 && (d & 0x38) == 0)
      {
      *erroroffset = (int)(p - string) - 4;
      return PCRE_UTF8_ERR18;
      }
    break;

    /* 6-byte character. Check 3rd-6th bytes for 0x80. Then check for
    1111 1100, xx00 00xx. */

    case 5:
    if ((*(++p) & 0xc0) != 0x80)     /* Third byte */
      {
      *erroroffset = (int)(p - string) - 2;
      return PCRE_UTF8_ERR7;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Fourth byte */
      {
      *erroroffset = (int)(p - string) - 3;
      return PCRE_UTF8_ERR8;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Fifth byte */
      {
      *erroroffset = (int)(p - string) - 4;
      return PCRE_UTF8_ERR9;
      }
    if ((*(++p) & 0xc0) != 0x80)     /* Sixth byte */
      {
      *erroroffset = (int)(p - string) - 5;
      return PCRE_UTF8_ERR10;
      }
    if (c == 0xfc && (d & 0x3c) == 0)
      {
      *erroroffset = (int)(p - string) - 5;
      return PCRE_UTF8_ERR19;
      }
    break;
    }

  /* Character is valid under RFC 2279, but 4-byte and 5-byte characters are
  excluded by RFC 3629. The pointer p is currently at the last byte of the
  character. */

  if (ab > 3)
    {
    *erroroffset = (int)(p - string) - ab;
    return (ab == 4)? PCRE_UTF8_ERR11 : PCRE_UTF8_ERR12;
    }
  }

#else  /* Not SUPPORT_UTF */
(void)(string);  /* Keep picky compilers happy */
(void)(length);
(void)(erroroffset);
#endif

return PCRE_UTF8_ERR0;   /* This indicates success */
}

/* End of pcre_valid_utf8.c */
