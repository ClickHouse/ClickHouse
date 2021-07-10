/* Timing variables for measuring compiler performance.
   Copyright (C) 2000-2018 Free Software Foundation, Inc.
   Contributed by Alex Samuel <samuel@codesourcery.com>

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_TIMEVAR_H
#define GCC_TIMEVAR_H

/* Timing variables are used to measure elapsed time in various
   portions of the compiler.  Each measures elapsed user, system, and
   wall-clock time, as appropriate to and supported by the host
   system.

   Timing variables are defined using the DEFTIMEVAR macro in
   timevar.def.  Each has an enumeral identifier, used when referring
   to the timing variable in code, and a character string name.

   Timing variables can be used in two ways:

     - On the timing stack, using timevar_push and timevar_pop.
       Timing variables may be pushed onto the stack; elapsed time is
       attributed to the topmost timing variable on the stack.  When
       another variable is pushed on, the previous topmost variable is
       `paused' until the pushed variable is popped back off.

     - As a standalone timer, using timevar_start and timevar_stop.
       All time elapsed between the two calls is attributed to the
       variable.
*/

/* This structure stores the various varieties of time that can be
   measured.  Times are stored in seconds.  The time may be an
   absolute time or a time difference; in the former case, the time
   base is undefined, except that the difference between two times
   produces a valid time difference.  */

struct timevar_time_def
{
  /* User time in this process.  */
  double user;

  /* System time (if applicable for this host platform) in this
     process.  */
  double sys;

  /* Wall clock time.  */
  double wall;

  /* Garbage collector memory.  */
  size_t ggc_mem;
};

/* An enumeration of timing variable identifiers.  Constructed from
   the contents of timevar.def.  */

#define DEFTIMEVAR(identifier__, name__) \
    identifier__,
typedef enum
{
  TV_NONE,
#include "timevar.def"
  TIMEVAR_LAST
}
timevar_id_t;
#undef DEFTIMEVAR

/* A class to hold all state relating to timing.  */

class timer;

/* The singleton instance of timing state.

   This is non-NULL if timevars should be used.  In GCC, this happens with
   the -ftime-report flag.  Hence this is NULL for the common,
   needs-to-be-fast case, with an early reject happening for this being
   NULL.  */
extern timer *g_timer;

/* Total amount of memory allocated by garbage collector.  */
extern size_t timevar_ggc_mem_total;

extern void timevar_init (void);
extern void timevar_start (timevar_id_t);
extern void timevar_stop (timevar_id_t);
extern bool timevar_cond_start (timevar_id_t);
extern void timevar_cond_stop (timevar_id_t, bool);

/* The public (within GCC) interface for timing.  */

class timer
{
 public:
  timer ();
  ~timer ();

  void start (timevar_id_t tv);
  void stop (timevar_id_t tv);
  void push (timevar_id_t tv);
  void pop (timevar_id_t tv);
  bool cond_start (timevar_id_t tv);
  void cond_stop (timevar_id_t tv);

  void push_client_item (const char *item_name);
  void pop_client_item ();

  void print (FILE *fp);

  const char *get_topmost_item_name () const;

 private:
  /* Private member functions.  */
  void validate_phases (FILE *fp) const;

  struct timevar_def;
  void push_internal (struct timevar_def *tv);
  void pop_internal ();
  static void print_row (FILE *fp,
			 const timevar_time_def *total,
			 const char *name, const timevar_time_def &elapsed);
  static bool all_zero (const timevar_time_def &elapsed);

 private:
  typedef hash_map<timevar_def *, timevar_time_def> child_map_t;

  /* Private type: a timing variable.  */
  struct timevar_def
  {
    /* Elapsed time for this variable.  */
    struct timevar_time_def elapsed;

    /* If this variable is timed independently of the timing stack,
       using timevar_start, this contains the start time.  */
    struct timevar_time_def start_time;

    /* The name of this timing variable.  */
    const char *name;

    /* Nonzero if this timing variable is running as a standalone
       timer.  */
    unsigned standalone : 1;

    /* Nonzero if this timing variable was ever started or pushed onto
       the timing stack.  */
    unsigned used : 1;

    child_map_t *children;
  };

  /* Private type: an element on the timing stack
     Elapsed time is attributed to the topmost timing variable on the
     stack.  */
  struct timevar_stack_def
  {
    /* The timing variable at this stack level.  */
    struct timevar_def *timevar;

    /* The next lower timing variable context in the stack.  */
    struct timevar_stack_def *next;
  };

  /* A class for managing a collection of named timing items, for use
     e.g. by libgccjit for timing client code.  This class is declared
     inside timevar.c to avoid everything using timevar.h
     from needing vec and hash_map.  */
  class named_items;

 private:

  /* Data members (all private).  */

  /* Declared timing variables.  Constructed from the contents of
     timevar.def.  */
  timevar_def m_timevars[TIMEVAR_LAST];

  /* The top of the timing stack.  */
  timevar_stack_def *m_stack;

  /* A list of unused (i.e. allocated and subsequently popped)
     timevar_stack_def instances.  */
  timevar_stack_def *m_unused_stack_instances;

  /* The time at which the topmost element on the timing stack was
     pushed.  Time elapsed since then is attributed to the topmost
     element.  */
  timevar_time_def m_start_time;

  /* If non-NULL, for use when timing libgccjit's client code.  */
  named_items *m_jit_client_items;

  friend class named_items;
};

/* Provided for backward compatibility.  */
static inline void
timevar_push (timevar_id_t tv)
{
  if (g_timer)
    g_timer->push (tv);
}

static inline void
timevar_pop (timevar_id_t tv)
{
  if (g_timer)
    g_timer->pop (tv);
}

// This is a simple timevar wrapper class that pushes a timevar in its
// constructor and pops the timevar in its destructor.
class auto_timevar
{
 public:
  auto_timevar (timer *t, timevar_id_t tv)
    : m_timer (t),
      m_tv (tv)
  {
    if (m_timer)
      m_timer->push (m_tv);
  }

  explicit auto_timevar (timevar_id_t tv)
    : m_timer (g_timer)
    , m_tv (tv)
  {
    if (m_timer)
      m_timer->push (m_tv);
  }

  ~auto_timevar ()
  {
    if (m_timer)
      m_timer->pop (m_tv);
  }

 private:

  // Private to disallow copies.
  auto_timevar (const auto_timevar &);

  timer *m_timer;
  timevar_id_t m_tv;
};

extern void print_time (const char *, long);

#endif /* ! GCC_TIMEVAR_H */
