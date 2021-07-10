/* Operation with 128 bit bitmask.
   Copyright (C) 2013-2018 Free Software Foundation, Inc.

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

#ifndef GCC_WIDE_INT_BITMASK_H
#define GCC_WIDE_INT_BITMASK_H

struct wide_int_bitmask
{
  inline wide_int_bitmask ();
  inline wide_int_bitmask (uint64_t l);
  inline wide_int_bitmask (uint64_t l, uint64_t h);
  inline wide_int_bitmask &operator &= (wide_int_bitmask);
  inline wide_int_bitmask &operator |= (wide_int_bitmask);
  inline wide_int_bitmask operator ~ () const;
  inline wide_int_bitmask operator & (wide_int_bitmask) const;
  inline wide_int_bitmask operator | (wide_int_bitmask) const;
  inline wide_int_bitmask operator >> (int);
  inline wide_int_bitmask operator << (int);
  inline bool operator == (wide_int_bitmask) const;
  inline bool operator != (wide_int_bitmask) const;
  uint64_t low, high;
};

inline
wide_int_bitmask::wide_int_bitmask ()
: low (0), high (0)
{
}

inline
wide_int_bitmask::wide_int_bitmask (uint64_t l)
: low (l), high (0)
{
}

inline
wide_int_bitmask::wide_int_bitmask (uint64_t l, uint64_t h)
: low (l), high (h)
{
}

inline wide_int_bitmask &
wide_int_bitmask::operator &= (wide_int_bitmask b)
{
  low &= b.low;
  high &= b.high;
  return *this;
}

inline wide_int_bitmask &
wide_int_bitmask::operator |= (wide_int_bitmask b)
{
  low |= b.low;
  high |= b.high;
  return *this;
}

inline wide_int_bitmask
wide_int_bitmask::operator ~ () const
{
  wide_int_bitmask ret (~low, ~high);
  return ret;
}

inline wide_int_bitmask
wide_int_bitmask::operator | (wide_int_bitmask b) const
{
  wide_int_bitmask ret (low | b.low, high | b.high);
  return ret;
}

inline wide_int_bitmask
wide_int_bitmask::operator & (wide_int_bitmask b) const
{
  wide_int_bitmask ret (low & b.low, high & b.high);
  return ret;
}

inline wide_int_bitmask
wide_int_bitmask::operator << (int amount)
{
  wide_int_bitmask ret;
  if (amount >= 64)
    {
      ret.low = 0;
      ret.high = low << (amount - 64);
    }
  else if (amount == 0)
    ret = *this;
  else
    {
      ret.low = low << amount;
      ret.high = (low >> (64 - amount)) | (high << amount);
    }
  return ret;
}

inline wide_int_bitmask
wide_int_bitmask::operator >> (int amount)
{
  wide_int_bitmask ret;
  if (amount >= 64)
    {
      ret.low = high >> (amount - 64);
      ret.high = 0;
    }
  else if (amount == 0)
    ret = *this;
  else
    {
      ret.low = (high << (64 - amount)) | (low >> amount);
      ret.high = high >> amount;
    }
  return ret;
}

inline bool
wide_int_bitmask::operator == (wide_int_bitmask b) const
{
  return low == b.low && high == b.high;
}

inline bool
wide_int_bitmask::operator != (wide_int_bitmask b) const
{
  return low != b.low || high != b.high;
}

#endif /* ! GCC_WIDE_INT_BITMASK_H */
