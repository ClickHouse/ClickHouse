/* Header file for collect/tlink routines.
   Copyright (C) 1998-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_COLLECT2_H
#define GCC_COLLECT2_H

extern void do_tlink (char **, char **);

extern struct pex_obj *collect_execute (const char *, char **, const char *,
					const char *, int flags);

extern int collect_wait (const char *, struct pex_obj *);

extern void dump_ld_file (const char *, FILE *);

extern int file_exists (const char *);

extern const char *ldout;
extern const char *lderrout;
extern const char *c_file_name;
extern struct obstack temporary_obstack;
extern char *temporary_firstobj;
extern bool may_unlink_output_file;

extern void notice_translated (const char *, ...) ATTRIBUTE_PRINTF_1;
extern void notice (const char *, ...) ATTRIBUTE_PRINTF_1;

extern bool at_file_supplied;
#endif /* ! GCC_COLLECT2_H */
