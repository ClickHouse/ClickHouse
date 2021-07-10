/* params-enums.h - Run-time parameter enums.
   Copyright (C) 2015-2018 Free Software Foundation, Inc.

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

#define DEFPARAM(ENUM, OPTION, HELP, DEFAULT, MIN, MAX)
#define DEFPARAMENUMNAME(ENUM) ENUM ## _KIND
#define DEFPARAMENUMVAL(ENUM, V) ENUM ## _KIND_ ## V
#define DEFPARAMENUMTERM(ENUM) ENUM ## _KIND_ ## LAST
#define DEFPARAMENUM5(ENUM, OPTION, HELP, DEFAULT, V0, V1, V2, V3, V4)	\
  enum DEFPARAMENUMNAME (ENUM)					\
  {								\
    DEFPARAMENUMVAL (ENUM, V0),					\
    DEFPARAMENUMVAL (ENUM, V1),					\
    DEFPARAMENUMVAL (ENUM, V2),					\
    DEFPARAMENUMVAL (ENUM, V3),					\
    DEFPARAMENUMVAL (ENUM, V4),					\
    DEFPARAMENUMTERM (ENUM)					\
  };
#include "params.def"
#undef DEFPARAMENUM5
#undef DEFPARAMENUMTERM
#undef DEFPARAMENUMVAL
#undef DEFPARAMENUMNAME
#undef DEFPARAM
