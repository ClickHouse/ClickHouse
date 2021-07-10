/* Copyright (C) 2011-2018 Free Software Foundation, Inc.
   Contributed by ARM Ltd.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published
   by the Free Software Foundation; either version 3, or (at your
   option) any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

/* Definitions for option handling for AArch64.  */

#ifndef GCC_AARCH64_OPTS_H
#define GCC_AARCH64_OPTS_H

/* The various cores that implement AArch64.  */
enum aarch64_processor
{
#define AARCH64_CORE(NAME, INTERNAL_IDENT, SCHED, ARCH, FLAGS, COSTS, IMP, PART, VARIANT) \
  INTERNAL_IDENT,
#include "aarch64-cores.def"
  /* Used to indicate that no processor has been specified.  */
  generic,
  /* Used to mark the end of the processor table.  */
  aarch64_none
};

enum aarch64_arch
{
#define AARCH64_ARCH(NAME, CORE, ARCH_IDENT, ARCH_REV, FLAGS) \
  AARCH64_ARCH_##ARCH_IDENT,
#include "aarch64-arches.def"
  aarch64_no_arch
};

/* TLS types.  */
enum aarch64_tls_type {
  TLS_TRADITIONAL,
  TLS_DESCRIPTORS
};

/* The code model defines the address generation strategy.
   Most have a PIC and non-PIC variant.  */
enum aarch64_code_model {
  /* Static code and data fit within a 1MB region.
     Not fully implemented, mostly treated as SMALL.  */
  AARCH64_CMODEL_TINY,
  /* Static code, data and GOT/PLT fit within a 1MB region.
     Not fully implemented, mostly treated as SMALL_PIC.  */
  AARCH64_CMODEL_TINY_PIC,
  /* Static code and data fit within a 4GB region.
     The default non-PIC code model.  */
  AARCH64_CMODEL_SMALL,
  /* Static code, data and GOT/PLT fit within a 4GB region.
     The default PIC code model.  */
  AARCH64_CMODEL_SMALL_PIC,
  /* -fpic for small memory model.
     GOT size to 28KiB (4K*8-4K) or 3580 entries.  */
  AARCH64_CMODEL_SMALL_SPIC,
  /* No assumptions about addresses of code and data.
     The PIC variant is not yet implemented.  */
  AARCH64_CMODEL_LARGE
};

/* Function types -msign-return-address should sign.  */
enum aarch64_function_type {
  /* Don't sign any function.  */
  AARCH64_FUNCTION_NONE,
  /* Non-leaf functions.  */
  AARCH64_FUNCTION_NON_LEAF,
  /* All functions.  */
  AARCH64_FUNCTION_ALL
};

/* SVE vector register sizes.  */
enum aarch64_sve_vector_bits_enum {
  SVE_SCALABLE,
  SVE_128 = 128,
  SVE_256 = 256,
  SVE_512 = 512,
  SVE_1024 = 1024,
  SVE_2048 = 2048
};

#endif
