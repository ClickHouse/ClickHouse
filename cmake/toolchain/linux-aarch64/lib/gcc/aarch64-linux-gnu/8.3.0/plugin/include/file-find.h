/* Prototypes and data structures used for implementing functions for
   finding files relative to GCC binaries.
   Copyright (C) 1992-2018 Free Software Foundation, Inc.

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

#ifndef GCC_FILE_FIND_H
#define GCC_FILE_FIND_H

/* Structure to hold all the directories in which to search for files to
   execute.  */

struct prefix_list
{
  const char *prefix;         /* String to prepend to the path.  */
  struct prefix_list *next;   /* Next in linked list.  */
};

struct path_prefix
{
  struct prefix_list *plist;  /* List of prefixes to try */
  int max_len;                /* Max length of a prefix in PLIST */
  const char *name;           /* Name of this list (used in config stuff) */
};

extern void find_file_set_debug (bool);
extern char *find_a_file (struct path_prefix *, const char *, int);
extern void add_prefix (struct path_prefix *, const char *);
extern void add_prefix_begin (struct path_prefix *, const char *);
extern void prefix_from_env (const char *, struct path_prefix *);
extern void prefix_from_string (const char *, struct path_prefix *);

#endif /* GCC_FILE_FIND_H */
