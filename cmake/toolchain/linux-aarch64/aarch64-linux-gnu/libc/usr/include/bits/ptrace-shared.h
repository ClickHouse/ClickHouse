/* `ptrace' debugger support interface.  Linux version,
   not architecture-specific.
   Copyright (C) 1996-2018 Free Software Foundation, Inc.

   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _SYS_PTRACE_H
# error "Never use <bits/ptrace-shared.h> directly; include <sys/ptrace.h> instead."
#endif

/* Options set using PTRACE_SETOPTIONS.  */
enum __ptrace_setoptions
{
  PTRACE_O_TRACESYSGOOD	= 0x00000001,
  PTRACE_O_TRACEFORK	= 0x00000002,
  PTRACE_O_TRACEVFORK	= 0x00000004,
  PTRACE_O_TRACECLONE	= 0x00000008,
  PTRACE_O_TRACEEXEC	= 0x00000010,
  PTRACE_O_TRACEVFORKDONE = 0x00000020,
  PTRACE_O_TRACEEXIT	= 0x00000040,
  PTRACE_O_TRACESECCOMP	= 0x00000080,
  PTRACE_O_EXITKILL	= 0x00100000,
  PTRACE_O_SUSPEND_SECCOMP = 0x00200000,
  PTRACE_O_MASK		= 0x003000ff
};

enum __ptrace_eventcodes
{
/* Wait extended result codes for the above trace options.  */
  PTRACE_EVENT_FORK	= 1,
  PTRACE_EVENT_VFORK	= 2,
  PTRACE_EVENT_CLONE	= 3,
  PTRACE_EVENT_EXEC	= 4,
  PTRACE_EVENT_VFORK_DONE = 5,
  PTRACE_EVENT_EXIT	= 6,
  PTRACE_EVENT_SECCOMP  = 7,
/* Extended result codes enabled by means other than options.  */
  PTRACE_EVENT_STOP	= 128
};

/* Arguments for PTRACE_PEEKSIGINFO.  */
struct __ptrace_peeksiginfo_args
{
  __uint64_t off;	/* From which siginfo to start.  */
  __uint32_t flags;	/* Flags for peeksiginfo.  */
  __int32_t nr;		/* How many siginfos to take.  */
};

enum __ptrace_peeksiginfo_flags
{
  /* Read signals from a shared (process wide) queue.  */
  PTRACE_PEEKSIGINFO_SHARED = (1 << 0)
};

/* Argument and results of PTRACE_SECCOMP_GET_METADATA.  */
struct __ptrace_seccomp_metadata
{
  __uint64_t filter_off;	/* Input: which filter.  */
  __uint64_t flags;		/* Output: filter's flags.  */
};

/* Perform process tracing functions.  REQUEST is one of the values
   above, and determines the action to be taken.
   For all requests except PTRACE_TRACEME, PID specifies the process to be
   traced.

   PID and the other arguments described above for the various requests should
   appear (those that are used for the particular request) as:
     pid_t PID, void *ADDR, int DATA, void *ADDR2
   after REQUEST.  */
extern long int ptrace (enum __ptrace_request __request, ...) __THROW;
