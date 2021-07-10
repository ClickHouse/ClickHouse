/* Declarations for file prefix remapping support (-f*-prefix-map options).
   Copyright (C) 2017 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 3, or (at your option) any
   later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_FILE_PREFIX_MAP_H
#define GCC_FILE_PREFIX_MAP_H

void add_macro_prefix_map (const char *);
void add_debug_prefix_map (const char *);
void add_file_prefix_map (const char *);

const char *remap_macro_filename (const char *);
const char *remap_debug_filename (const char *);

#endif /* !GCC_FILE_PREFIX_MAP_H  */
