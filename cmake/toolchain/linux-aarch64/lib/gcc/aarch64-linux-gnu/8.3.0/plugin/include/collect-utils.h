/* Utility functions used by tools like collect2 and lto-wrapper.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.

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

#ifndef GCC_COLLECT_UTILS_H
#define GCC_COLLECT_UTILS_H

/* Provided in collect-utils.c.  */
extern void notice (const char *, ...)
  __attribute__ ((format (printf, 1, 2)));
extern void fatal_signal (int);

extern struct pex_obj *collect_execute (const char *, char **,
					const char *, const char *,
					int, bool);
extern int collect_wait (const char *, struct pex_obj *);
extern void do_wait (const char *, struct pex_obj *);
extern void fork_execute (const char *, char **, bool);
extern void utils_cleanup (bool);


extern bool debug;
extern bool verbose;
extern bool save_temps;

/* Provided by the tool itself.  */

/* The name of the tool, printed in error messages.  */
extern const char tool_name[];
/* Called by utils_cleanup.  */
extern void tool_cleanup (bool);
extern void maybe_unlink (const char *);

#endif /* GCC_COLLECT_UTILS_H */
