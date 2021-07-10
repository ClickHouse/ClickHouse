/* Copyright (C) 1996-2018 Free Software Foundation, Inc.

   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public License as
   published by the Free Software Foundation; either version 2.1 of the
   License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _AARCH64_FPU_CONTROL_H
#define _AARCH64_FPU_CONTROL_H

#include <features.h>

/* Macros for accessing the FPCR and FPSR.  */

#if __GNUC_PREREQ (6,0)
# define _FPU_GETCW(fpcr) (fpcr = __builtin_aarch64_get_fpcr ())
# define _FPU_SETCW(fpcr) __builtin_aarch64_set_fpcr (fpcr)
# define _FPU_GETFPSR(fpsr) (fpsr = __builtin_aarch64_get_fpsr ())
# define _FPU_SETFPSR(fpsr) __builtin_aarch64_set_fpsr (fpsr)
#else
# define _FPU_GETCW(fpcr) \
  __asm__ __volatile__ ("mrs	%0, fpcr" : "=r" (fpcr))

# define _FPU_SETCW(fpcr) \
  __asm__ __volatile__ ("msr	fpcr, %0" : : "r" (fpcr))

# define _FPU_GETFPSR(fpsr) \
  __asm__ __volatile__ ("mrs	%0, fpsr" : "=r" (fpsr))

# define _FPU_SETFPSR(fpsr) \
  __asm__ __volatile__ ("msr	fpsr, %0" : : "r" (fpsr))
#endif

/* Reserved bits should be preserved when modifying register
   contents. These two masks indicate which bits in each of FPCR and
   FPSR should not be changed.  */

#define _FPU_RESERVED		0xfe0fe0ff
#define _FPU_FPSR_RESERVED	0x0fffffe0

#define _FPU_DEFAULT		0x00000000
#define _FPU_FPSR_DEFAULT	0x00000000

/* Layout of FPCR and FPSR:

   |       |       |       |       |       |       |       |
   0 0 0 0 1 1 1 0 0 0 0 0 1 0 0 0 1 1 1 0 0 0 0 0 1 1 1 0 0 0 0 0
   s s s s s                                       s     s s s s s
             c c c c c c c               c c c c c
   N Z C V Q A D F R R S S S L L L I U U I U O D I I U U I U O D I
           C H N Z M M T T B E E E D N N X F F Z O D N N X F F Z O
             P     O O R R Z N N N E K K E E E E E C K K C C C C C
                   D D I I P
                   E E D D
                       E E
 */

#define _FPU_FPCR_RM_MASK  0xc00000

#define _FPU_FPCR_MASK_IXE 0x1000
#define _FPU_FPCR_MASK_UFE 0x0800
#define _FPU_FPCR_MASK_OFE 0x0400
#define _FPU_FPCR_MASK_DZE 0x0200
#define _FPU_FPCR_MASK_IOE 0x0100

#define _FPU_FPCR_IEEE                       \
  (_FPU_DEFAULT  | _FPU_FPCR_MASK_IXE |	     \
   _FPU_FPCR_MASK_UFE | _FPU_FPCR_MASK_OFE | \
   _FPU_FPCR_MASK_DZE | _FPU_FPCR_MASK_IOE)

#define _FPU_FPSR_IEEE 0

typedef unsigned int fpu_control_t;
typedef unsigned int fpu_fpsr_t;

/* Default control word set at startup.  */
extern fpu_control_t __fpu_control;

#endif
