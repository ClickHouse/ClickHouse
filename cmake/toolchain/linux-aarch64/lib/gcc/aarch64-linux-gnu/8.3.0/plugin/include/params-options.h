/* File used to generate params.list
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

#define DEFPARAM(enumerator, option, nocmsgid, default, min, max) \
  option=default,min,max
#define DEFPARAMENUM5(enumerator, option, nocmsgid, default, \
		      v0, v1, v2, v3, v4) \
  option=v0,v1,v2,v3,v4
#include "params.def"
#undef DEFPARAM
#undef DEFPARAMENUM5
