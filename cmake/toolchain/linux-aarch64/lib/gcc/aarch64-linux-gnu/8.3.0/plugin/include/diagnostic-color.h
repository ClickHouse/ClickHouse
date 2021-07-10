/* Copyright (C) 2013-2018 Free Software Foundation, Inc.
   Contributed by Manuel Lopez-Ibanez <manu@gcc.gnu.org>

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

/* Based on code from: */
/* grep.c - main driver file for grep.
   Copyright (C) 1992-2018 Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street - Fifth Floor, Boston, MA
   02110-1301, USA.

   Written July 1992 by Mike Haertel.  */

#ifndef GCC_DIAGNOSTIC_COLOR_H
#define GCC_DIAGNOSTIC_COLOR_H

/* Whether to add color to diagnostics:
   o DIAGNOSTICS_COLOR_NO: never
   o DIAGNOSTICS_COLOR_YES: always
   o DIAGNOSTICS_COLOR_AUTO: depending on the output stream.  */
typedef enum
{
  DIAGNOSTICS_COLOR_NO       = 0,
  DIAGNOSTICS_COLOR_YES      = 1,
  DIAGNOSTICS_COLOR_AUTO     = 2
} diagnostic_color_rule_t;

const char *colorize_start (bool, const char *, size_t);
const char *colorize_stop (bool);
bool colorize_init (diagnostic_color_rule_t);

inline const char *
colorize_start (bool show_color, const char *name)
{
  return colorize_start (show_color, name, strlen (name));
}

#endif /* ! GCC_DIAGNOSTIC_COLOR_H */
