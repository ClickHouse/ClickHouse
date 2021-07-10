/* Definitions for simple data type for real numbers.
   Copyright (C) 2002-2018 Free Software Foundation, Inc.

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

#ifndef GCC_SREAL_H
#define GCC_SREAL_H

/* SREAL_PART_BITS has to be an even number.  */
#define SREAL_PART_BITS 32

#define UINT64_BITS	64

#define SREAL_MIN_SIG ((int64_t) 1 << (SREAL_PART_BITS - 2))
#define SREAL_MAX_SIG (((int64_t) 1 << (SREAL_PART_BITS - 1)) - 1)
#define SREAL_MAX_EXP (INT_MAX / 4)

#define SREAL_BITS SREAL_PART_BITS

#define SREAL_SIGN(v) (v < 0 ? -1: 1)
#define SREAL_ABS(v) (v < 0 ? -v: v)

struct output_block;
struct lto_input_block;

/* Structure for holding a simple real number.  */
class sreal
{
public:
  /* Construct an uninitialized sreal.  */
  sreal () : m_sig (-1), m_exp (-1) {}

  /* Construct a sreal.  */
  sreal (int64_t sig, int exp = 0) : m_sig (sig), m_exp (exp)
  {
    normalize ();
  }

  void dump (FILE *) const;
  int64_t to_int () const;
  double to_double () const;
  void stream_out (struct output_block *);
  static sreal stream_in (struct lto_input_block *);
  sreal operator+ (const sreal &other) const;
  sreal operator- (const sreal &other) const;
  sreal operator* (const sreal &other) const;
  sreal operator/ (const sreal &other) const;

  bool operator< (const sreal &other) const
  {
    if (m_exp == other.m_exp)
      return m_sig < other.m_sig;
    else
    {
      bool negative = m_sig < 0;
      bool other_negative = other.m_sig < 0;

      if (negative != other_negative)
        return negative > other_negative;

      bool r = m_exp < other.m_exp;
      return negative ? !r : r;
    }
  }

  bool operator== (const sreal &other) const
  {
    return m_exp == other.m_exp && m_sig == other.m_sig;
  }

  sreal operator- () const
  {
    sreal tmp = *this;
    tmp.m_sig *= -1;

    return tmp;
  }

  sreal shift (int s) const
  {
    /* Zero needs no shifting.  */
    if (!m_sig)
      return *this;
    gcc_checking_assert (s <= SREAL_MAX_EXP);
    gcc_checking_assert (s >= -SREAL_MAX_EXP);

    /* Overflows/drop to 0 could be handled gracefully, but hopefully we do not
       need to do so.  */
    gcc_checking_assert (m_exp + s <= SREAL_MAX_EXP);
    gcc_checking_assert (m_exp + s >= -SREAL_MAX_EXP);

    sreal tmp = *this;
    tmp.m_exp += s;

    return tmp;
  }

  /* Global minimum sreal can hold.  */
  inline static sreal min ()
  {
    sreal min;
    /* This never needs normalization.  */
    min.m_sig = -SREAL_MAX_SIG;
    min.m_exp = SREAL_MAX_EXP;
    return min;
  }

  /* Global minimum sreal can hold.  */
  inline static sreal max ()
  {
    sreal max;
    /* This never needs normalization.  */
    max.m_sig = SREAL_MAX_SIG;
    max.m_exp = SREAL_MAX_EXP;
    return max;
  }

private:
  inline void normalize ();
  inline void normalize_up ();
  inline void normalize_down ();
  void shift_right (int amount);
  static sreal signedless_plus (const sreal &a, const sreal &b, bool negative);
  static sreal signedless_minus (const sreal &a, const sreal &b, bool negative);

  int64_t m_sig;			/* Significant.  */
  signed int m_exp;			/* Exponent.  */
};

extern void debug (const sreal &ref);
extern void debug (const sreal *ptr);

inline sreal &operator+= (sreal &a, const sreal &b)
{
  return a = a + b;
}

inline sreal &operator-= (sreal &a, const sreal &b)
{
  return a = a - b;
}

inline sreal &operator/= (sreal &a, const sreal &b)
{
  return a = a / b;
}

inline sreal &operator*= (sreal &a, const sreal &b)
{
  return a = a  * b;
}

inline bool operator!= (const sreal &a, const sreal &b)
{
  return !(a == b);
}

inline bool operator> (const sreal &a, const sreal &b)
{
  return !(a == b || a < b);
}

inline bool operator<= (const sreal &a, const sreal &b)
{
  return a < b || a == b;
}

inline bool operator>= (const sreal &a, const sreal &b)
{
  return a == b || a > b;
}

inline sreal operator<< (const sreal &a, int exp)
{
  return a.shift (exp);
}

inline sreal operator>> (const sreal &a, int exp)
{
  return a.shift (-exp);
}

/* Make significant to be >= SREAL_MIN_SIG.

   Make this separate method so inliner can handle hot path better.  */

inline void
sreal::normalize_up ()
{
  unsigned HOST_WIDE_INT sig = absu_hwi (m_sig);
  int shift = SREAL_PART_BITS - 2 - floor_log2 (sig);

  gcc_checking_assert (shift > 0);
  sig <<= shift;
  m_exp -= shift;
  gcc_checking_assert (sig <= SREAL_MAX_SIG && sig >= SREAL_MIN_SIG);

  /* Check underflow.  */
  if (m_exp < -SREAL_MAX_EXP)
    {
      m_exp = -SREAL_MAX_EXP;
      sig = 0;
    }
  if (SREAL_SIGN (m_sig) == -1)
    m_sig = -sig;
  else
    m_sig = sig;
}

/* Make significant to be <= SREAL_MAX_SIG.

   Make this separate method so inliner can handle hot path better.  */

inline void
sreal::normalize_down ()
{
  int last_bit;
  unsigned HOST_WIDE_INT sig = absu_hwi (m_sig);
  int shift = floor_log2 (sig) - SREAL_PART_BITS + 2;

  gcc_checking_assert (shift > 0);
  last_bit = (sig >> (shift-1)) & 1;
  sig >>= shift;
  m_exp += shift;
  gcc_checking_assert (sig <= SREAL_MAX_SIG && sig >= SREAL_MIN_SIG);

  /* Round the number.  */
  sig += last_bit;
  if (sig > SREAL_MAX_SIG)
    {
      sig >>= 1;
      m_exp++;
    }

  /* Check overflow.  */
  if (m_exp > SREAL_MAX_EXP)
    {
      m_exp = SREAL_MAX_EXP;
      sig = SREAL_MAX_SIG;
    }
  if (SREAL_SIGN (m_sig) == -1)
    m_sig = -sig;
  else
    m_sig = sig;
}

/* Normalize *this; the hot path.  */

inline void
sreal::normalize ()
{
  unsigned HOST_WIDE_INT sig = absu_hwi (m_sig);

  if (sig == 0)
    m_exp = -SREAL_MAX_EXP;
  else if (sig > SREAL_MAX_SIG)
    normalize_down ();
  else if (sig < SREAL_MIN_SIG)
    normalize_up ();
}

#endif
