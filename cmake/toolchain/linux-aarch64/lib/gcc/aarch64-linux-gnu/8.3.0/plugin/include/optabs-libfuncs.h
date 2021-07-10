/* Mapping from optabs to underlying library functions
   Copyright (C) 2001-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_OPTABS_LIBFUNCS_H
#define GCC_OPTABS_LIBFUNCS_H

#include "insn-opinit.h"

rtx convert_optab_libfunc (convert_optab, machine_mode, machine_mode);
rtx optab_libfunc (optab, machine_mode);

void gen_int_libfunc (optab, const char *, char, machine_mode);
void gen_fp_libfunc (optab, const char *, char, machine_mode);
void gen_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_signed_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_unsigned_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_int_fp_libfunc (optab, const char *, char, machine_mode);
void gen_intv_fp_libfunc (optab, const char *, char, machine_mode);
void gen_int_fp_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_int_fp_signed_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_int_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_int_signed_fixed_libfunc (optab, const char *, char, machine_mode);
void gen_int_unsigned_fixed_libfunc (optab, const char *, char, machine_mode);

void gen_interclass_conv_libfunc (convert_optab, const char *,
				  machine_mode, machine_mode);
void gen_int_to_fp_conv_libfunc (convert_optab, const char *,
				 machine_mode, machine_mode);
void gen_ufloat_conv_libfunc (convert_optab, const char *,
			      machine_mode, machine_mode);
void gen_int_to_fp_nondecimal_conv_libfunc (convert_optab, const char *,
					    machine_mode, machine_mode);
void gen_fp_to_int_conv_libfunc (convert_optab, const char *,
				 machine_mode, machine_mode);
void gen_intraclass_conv_libfunc (convert_optab, const char *,
				  machine_mode, machine_mode);
void gen_trunc_conv_libfunc (convert_optab, const char *,
			     machine_mode, machine_mode);
void gen_extend_conv_libfunc (convert_optab, const char *,
			      machine_mode, machine_mode);
void gen_fract_conv_libfunc (convert_optab, const char *,
			     machine_mode, machine_mode);
void gen_fractuns_conv_libfunc (convert_optab, const char *,
				machine_mode, machine_mode);
void gen_satfract_conv_libfunc (convert_optab, const char *,
				machine_mode, machine_mode);
void gen_satfractuns_conv_libfunc (convert_optab, const char *,
				   machine_mode, machine_mode);

tree build_libfunc_function (const char *);
rtx init_one_libfunc (const char *);
rtx set_user_assembler_libfunc (const char *, const char *);

void set_optab_libfunc (optab, machine_mode, const char *);
void set_conv_libfunc (convert_optab, machine_mode,
		       machine_mode, const char *);

void init_optabs (void);
void init_sync_libfuncs (int max);

#endif
