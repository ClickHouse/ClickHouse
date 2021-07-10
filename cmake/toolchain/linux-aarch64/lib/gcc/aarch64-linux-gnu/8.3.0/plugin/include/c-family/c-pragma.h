/* Pragma related interfaces.
   Copyright (C) 1995-2018 Free Software Foundation, Inc.

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

#ifndef GCC_C_PRAGMA_H
#define GCC_C_PRAGMA_H

#include "cpplib.h" /* For enum cpp_ttype.  */

/* Pragma identifiers built in to the front end parsers.  Identifiers
   for ancillary handlers will follow these.  */
enum pragma_kind {
  PRAGMA_NONE = 0,

  PRAGMA_OACC_ATOMIC,
  PRAGMA_OACC_CACHE,
  PRAGMA_OACC_DATA,
  PRAGMA_OACC_DECLARE,
  PRAGMA_OACC_ENTER_DATA,
  PRAGMA_OACC_EXIT_DATA,
  PRAGMA_OACC_HOST_DATA,
  PRAGMA_OACC_KERNELS,
  PRAGMA_OACC_LOOP,
  PRAGMA_OACC_PARALLEL,
  PRAGMA_OACC_ROUTINE,
  PRAGMA_OACC_UPDATE,
  PRAGMA_OACC_WAIT,

  PRAGMA_OMP_ATOMIC,
  PRAGMA_OMP_BARRIER,
  PRAGMA_OMP_CANCEL,
  PRAGMA_OMP_CANCELLATION_POINT,
  PRAGMA_OMP_CRITICAL,
  PRAGMA_OMP_DECLARE,
  PRAGMA_OMP_DISTRIBUTE,
  PRAGMA_OMP_END_DECLARE_TARGET,
  PRAGMA_OMP_FLUSH,
  PRAGMA_OMP_FOR,
  PRAGMA_OMP_MASTER,
  PRAGMA_OMP_ORDERED,
  PRAGMA_OMP_PARALLEL,
  PRAGMA_OMP_SECTION,
  PRAGMA_OMP_SECTIONS,
  PRAGMA_OMP_SIMD,
  PRAGMA_OMP_SINGLE,
  PRAGMA_OMP_TARGET,
  PRAGMA_OMP_TASK,
  PRAGMA_OMP_TASKGROUP,
  PRAGMA_OMP_TASKLOOP,
  PRAGMA_OMP_TASKWAIT,
  PRAGMA_OMP_TASKYIELD,
  PRAGMA_OMP_THREADPRIVATE,
  PRAGMA_OMP_TEAMS,

  PRAGMA_GCC_PCH_PREPROCESS,
  PRAGMA_IVDEP,
  PRAGMA_UNROLL,

  PRAGMA_FIRST_EXTERNAL
};


/* All clauses defined by OpenACC 2.0, and OpenMP 2.5, 3.0, 3.1, 4.0 and 4.5.
   Used internally by both C and C++ parsers.  */
enum pragma_omp_clause {
  PRAGMA_OMP_CLAUSE_NONE = 0,

  PRAGMA_OMP_CLAUSE_ALIGNED,
  PRAGMA_OMP_CLAUSE_COLLAPSE,
  PRAGMA_OMP_CLAUSE_COPYIN,
  PRAGMA_OMP_CLAUSE_COPYPRIVATE,
  PRAGMA_OMP_CLAUSE_DEFAULT,
  PRAGMA_OMP_CLAUSE_DEFAULTMAP,
  PRAGMA_OMP_CLAUSE_DEPEND,
  PRAGMA_OMP_CLAUSE_DEVICE,
  PRAGMA_OMP_CLAUSE_DIST_SCHEDULE,
  PRAGMA_OMP_CLAUSE_FINAL,
  PRAGMA_OMP_CLAUSE_FIRSTPRIVATE,
  PRAGMA_OMP_CLAUSE_FOR,
  PRAGMA_OMP_CLAUSE_FROM,
  PRAGMA_OMP_CLAUSE_GRAINSIZE,
  PRAGMA_OMP_CLAUSE_HINT,
  PRAGMA_OMP_CLAUSE_IF,
  PRAGMA_OMP_CLAUSE_INBRANCH,
  PRAGMA_OMP_CLAUSE_IS_DEVICE_PTR,
  PRAGMA_OMP_CLAUSE_LASTPRIVATE,
  PRAGMA_OMP_CLAUSE_LINEAR,
  PRAGMA_OMP_CLAUSE_LINK,
  PRAGMA_OMP_CLAUSE_MAP,
  PRAGMA_OMP_CLAUSE_MERGEABLE,
  PRAGMA_OMP_CLAUSE_NOGROUP,
  PRAGMA_OMP_CLAUSE_NOTINBRANCH,
  PRAGMA_OMP_CLAUSE_NOWAIT,
  PRAGMA_OMP_CLAUSE_NUM_TASKS,
  PRAGMA_OMP_CLAUSE_NUM_TEAMS,
  PRAGMA_OMP_CLAUSE_NUM_THREADS,
  PRAGMA_OMP_CLAUSE_ORDERED,
  PRAGMA_OMP_CLAUSE_PARALLEL,
  PRAGMA_OMP_CLAUSE_PRIORITY,
  PRAGMA_OMP_CLAUSE_PRIVATE,
  PRAGMA_OMP_CLAUSE_PROC_BIND,
  PRAGMA_OMP_CLAUSE_REDUCTION,
  PRAGMA_OMP_CLAUSE_SAFELEN,
  PRAGMA_OMP_CLAUSE_SCHEDULE,
  PRAGMA_OMP_CLAUSE_SECTIONS,
  PRAGMA_OMP_CLAUSE_SHARED,
  PRAGMA_OMP_CLAUSE_SIMD,
  PRAGMA_OMP_CLAUSE_SIMDLEN,
  PRAGMA_OMP_CLAUSE_TASKGROUP,
  PRAGMA_OMP_CLAUSE_THREAD_LIMIT,
  PRAGMA_OMP_CLAUSE_THREADS,
  PRAGMA_OMP_CLAUSE_TO,
  PRAGMA_OMP_CLAUSE_UNIFORM,
  PRAGMA_OMP_CLAUSE_UNTIED,
  PRAGMA_OMP_CLAUSE_USE_DEVICE_PTR,

  /* Clauses for OpenACC.  */
  PRAGMA_OACC_CLAUSE_ASYNC,
  PRAGMA_OACC_CLAUSE_AUTO,
  PRAGMA_OACC_CLAUSE_COPY,
  PRAGMA_OACC_CLAUSE_COPYOUT,
  PRAGMA_OACC_CLAUSE_CREATE,
  PRAGMA_OACC_CLAUSE_DELETE,
  PRAGMA_OACC_CLAUSE_DEVICEPTR,
  PRAGMA_OACC_CLAUSE_DEVICE_RESIDENT,
  PRAGMA_OACC_CLAUSE_GANG,
  PRAGMA_OACC_CLAUSE_HOST,
  PRAGMA_OACC_CLAUSE_INDEPENDENT,
  PRAGMA_OACC_CLAUSE_NUM_GANGS,
  PRAGMA_OACC_CLAUSE_NUM_WORKERS,
  PRAGMA_OACC_CLAUSE_PRESENT,
  PRAGMA_OACC_CLAUSE_PRESENT_OR_COPY,
  PRAGMA_OACC_CLAUSE_PRESENT_OR_COPYIN,
  PRAGMA_OACC_CLAUSE_PRESENT_OR_COPYOUT,
  PRAGMA_OACC_CLAUSE_PRESENT_OR_CREATE,
  PRAGMA_OACC_CLAUSE_SELF,
  PRAGMA_OACC_CLAUSE_SEQ,
  PRAGMA_OACC_CLAUSE_TILE,
  PRAGMA_OACC_CLAUSE_USE_DEVICE,
  PRAGMA_OACC_CLAUSE_VECTOR,
  PRAGMA_OACC_CLAUSE_VECTOR_LENGTH,
  PRAGMA_OACC_CLAUSE_WAIT,
  PRAGMA_OACC_CLAUSE_WORKER,
  PRAGMA_OACC_CLAUSE_COLLAPSE = PRAGMA_OMP_CLAUSE_COLLAPSE,
  PRAGMA_OACC_CLAUSE_COPYIN = PRAGMA_OMP_CLAUSE_COPYIN,
  PRAGMA_OACC_CLAUSE_DEVICE = PRAGMA_OMP_CLAUSE_DEVICE,
  PRAGMA_OACC_CLAUSE_DEFAULT = PRAGMA_OMP_CLAUSE_DEFAULT,
  PRAGMA_OACC_CLAUSE_FIRSTPRIVATE = PRAGMA_OMP_CLAUSE_FIRSTPRIVATE,
  PRAGMA_OACC_CLAUSE_IF = PRAGMA_OMP_CLAUSE_IF,
  PRAGMA_OACC_CLAUSE_PRIVATE = PRAGMA_OMP_CLAUSE_PRIVATE,
  PRAGMA_OACC_CLAUSE_REDUCTION = PRAGMA_OMP_CLAUSE_REDUCTION,
  PRAGMA_OACC_CLAUSE_LINK = PRAGMA_OMP_CLAUSE_LINK
};

extern struct cpp_reader* parse_in;

/* It's safe to always leave visibility pragma enabled as if
   visibility is not supported on the host OS platform the
   statements are ignored.  */
extern void push_visibility (const char *, int);
extern bool pop_visibility (int);

extern void init_pragma (void);

/* Front-end wrappers for pragma registration.  */
typedef void (*pragma_handler_1arg)(struct cpp_reader *);
/* A second pragma handler, which adds a void * argument allowing to pass extra
   data to the handler.  */
typedef void (*pragma_handler_2arg)(struct cpp_reader *, void *);

/* This union allows to abstract the different handlers.  */
union gen_pragma_handler {
  pragma_handler_1arg handler_1arg;
  pragma_handler_2arg handler_2arg;
};
/* Internally used to keep the data of the handler.  */
struct internal_pragma_handler {
  union gen_pragma_handler handler;
  /* Permits to know if handler is a pragma_handler_1arg (extra_data is false)
     or a pragma_handler_2arg (extra_data is true).  */
  bool extra_data;
  /* A data field which can be used when extra_data is true.  */
  void * data;
};

extern void c_register_pragma (const char *space, const char *name,
                               pragma_handler_1arg handler);
extern void c_register_pragma_with_data (const char *space, const char *name,
                                         pragma_handler_2arg handler,
                                         void *data);

extern void c_register_pragma_with_expansion (const char *space,
                                              const char *name,
                                              pragma_handler_1arg handler);
extern void c_register_pragma_with_expansion_and_data (const char *space,
                                                       const char *name,
                                                   pragma_handler_2arg handler,
                                                       void *data);
extern void c_invoke_pragma_handler (unsigned int);

extern void maybe_apply_pragma_weak (tree);
extern void maybe_apply_pending_pragma_weaks (void);
extern tree maybe_apply_renaming_pragma (tree, tree);
extern void maybe_apply_pragma_scalar_storage_order (tree);
extern void add_to_renaming_pragma_list (tree, tree);

extern enum cpp_ttype pragma_lex (tree *, location_t *loc = NULL);

/* Flags for use with c_lex_with_flags.  The values here were picked
   so that 0 means to translate and join strings.  */
#define C_LEX_STRING_NO_TRANSLATE 1 /* Do not lex strings into
				       execution character set.  */
#define C_LEX_STRING_NO_JOIN	  2 /* Do not concatenate strings
				       nor translate them into execution
				       character set.  */

/* This is not actually available to pragma parsers.  It's merely a
   convenient location to declare this function for c-lex, after
   having enum cpp_ttype declared.  */
extern enum cpp_ttype c_lex_with_flags (tree *, location_t *, unsigned char *,
					int);

extern void c_pp_lookup_pragma (unsigned int, const char **, const char **);

extern GTY(()) tree pragma_extern_prefix;

#endif /* GCC_C_PRAGMA_H */
