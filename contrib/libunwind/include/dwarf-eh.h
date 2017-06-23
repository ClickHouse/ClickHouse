/* libunwind - a platform-independent unwind library
   Copyright (c) 2003 Hewlett-Packard Development Company, L.P.
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#ifndef dwarf_eh_h
#define dwarf_eh_h

#include "dwarf.h"

/* This header file defines the format of a DWARF exception-header
   section (.eh_frame_hdr, pointed to by program-header
   PT_GNU_EH_FRAME).  The exception-header is self-describing in the
   sense that the format of the addresses contained in it is expressed
   as a one-byte type-descriptor called a "pointer-encoding" (PE).

   The exception header encodes the address of the .eh_frame section
   and optionally contains a binary search table for the
   Frame Descriptor Entries (FDEs) in the .eh_frame.  The contents of
   .eh_frame has the format described by the DWARF v3 standard
   (http://www.eagercon.com/dwarf/dwarf3std.htm), except that code
   addresses may be encoded in different ways.  Also, .eh_frame has
   augmentations that allow encoding a language-specific data-area
   (LSDA) pointer and a pointer to a personality-routine.

   Details:

    The Common Information Entry (CIE) associated with an FDE may
    contain an augmentation string.  Each character in this string has
    a specific meaning and either one or two associated operands.  The
    operands are stored in an augmentation body which appears right
    after the "return_address_register" member and before the
    "initial_instructions" member.  The operands appear in the order
    in which the characters appear in the string.  For example, if the
    augmentation string is "zL", the operand for 'z' would be first in
    the augmentation body and the operand for 'L' would be second.
    The following characters are supported for the CIE augmentation
    string:

     'z': The operand for this character is a uleb128 value that gives the
          length of the CIE augmentation body, not counting the length
          of the uleb128 operand itself.  If present, this code must
          appear as the first character in the augmentation body.

     'L': Indicates that the FDE's augmentation body contains an LSDA
          pointer.  The operand for this character is a single byte
          that specifies the pointer-encoding (PE) that is used for
          the LSDA pointer.

     'R': Indicates that the code-pointers (FDE members
          "initial_location" and "address_range" and the operand for
          DW_CFA_set_loc) in the FDE have a non-default encoding.  The
          operand for this character is a single byte that specifies
          the pointer-encoding (PE) that is used for the
          code-pointers.  Note: the "address_range" member is always
          encoded as an absolute value.  Apart from that, the specified
          FDE pointer-encoding applies.

     'P': Indicates the presence of a personality routine (handler).
          The first operand for this character specifies the
          pointer-encoding (PE) that is used for the second operand,
          which specifies the address of the personality routine.

    If the augmentation string contains any other characters, the
    remainder of the augmentation string should be ignored.
    Furthermore, if the size of the augmentation body is unknown
    (i.e., 'z' is not the first character of the augmentation string),
    then the entire CIE as well all associated FDEs must be ignored.

    A Frame Descriptor Entries (FDE) may contain an augmentation body
    which, if present, appears right after the "address_range" member
    and before the "instructions" member.  The contents of this body
    is implicitly defined by the augmentation string of the associated
    CIE.  The meaning of the characters in the CIE's augmentation
    string as far as FDEs are concerned is as follows:

     'z': The first operand in the FDE's augmentation body specifies
          the total length of the augmentation body as a uleb128 (not
          counting the length of the uleb128 operand itself).

     'L': The operand for this character is an LSDA pointer, encoded
          in the format specified by the corresponding operand in the
          CIE's augmentation body.

*/

#define DW_EH_VERSION           1       /* The version we're implementing */

struct dwarf_eh_frame_hdr
  {
    unsigned char version;
    unsigned char eh_frame_ptr_enc;
    unsigned char fde_count_enc;
    unsigned char table_enc;
    /* The rest of the header is variable-length and consists of the
       following members:

        encoded_t eh_frame_ptr;
        encoded_t fde_count;
        struct
          {
            encoded_t start_ip; // first address covered by this FDE
            encoded_t fde_addr; // address of the FDE
          }
        binary_search_table[fde_count];  */
  };

#endif /* dwarf_eh_h */
