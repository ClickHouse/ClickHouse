/* toplev.h - Various declarations for functions found in toplev.c
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

#ifndef GCC_TOPLEV_H
#define GCC_TOPLEV_H

/* Decoded options, and number of such options.  */
extern struct cl_decoded_option *save_decoded_options;
extern unsigned int save_decoded_options_count;

class timer;

/* Invoking the compiler.  */
class toplev
{
public:
  toplev (timer *external_timer,
	  bool init_signals);
  ~toplev ();

  int main (int argc, char **argv);

  void finalize ();

private:

  void start_timevars ();

  void run_self_tests ();

  bool m_use_TV_TOTAL;
  bool m_init_signals;
};

extern void rest_of_decl_compilation (tree, int, int);
extern void rest_of_type_compilation (tree, int);
extern void init_optimization_passes (void);
extern bool enable_rtl_dump_file (void);

/* In except.c.  Initialize exception handling.  This is used by the Ada
   and LTO front ends to initialize EH "on demand".  See lto-streamer-in.c
   and ada/gcc-interface/misc.c.  */
extern void init_eh (void);

extern void announce_function (tree);

extern void wrapup_global_declaration_1 (tree);
extern bool wrapup_global_declaration_2 (tree);
extern bool wrapup_global_declarations (tree *, int);

extern void global_decl_processing (void);

extern void dump_memory_report (bool);
extern void dump_profile_report (void);

extern void target_reinit (void);

/* A unique local time stamp, might be zero if none is available.  */
extern unsigned local_tick;

/* See toplev.c.  */
extern int flag_rerun_cse_after_global_opts;

extern void print_version (FILE *, const char *, bool);

/* The hashtable, so that the C front ends can pass it to cpplib.  */
extern struct ht *ident_hash;

/* Functions used to get and set GCC's notion of in what directory
   compilation was started.  */

extern const char *get_src_pwd	       (void);
extern bool set_src_pwd		       (const char *);

/* Functions used to manipulate the random seed.  */

extern HOST_WIDE_INT get_random_seed (bool);
extern void set_random_seed (const char *);

extern void initialize_rtl (void);

#endif /* ! GCC_TOPLEV_H */
