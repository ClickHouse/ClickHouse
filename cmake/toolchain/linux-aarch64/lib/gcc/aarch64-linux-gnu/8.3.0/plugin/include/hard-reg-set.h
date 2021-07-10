/* Sets (bit vectors) of hard registers, and operations on them.
   Copyright (C) 1987-2018 Free Software Foundation, Inc.

This file is part of GCC

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

#ifndef GCC_HARD_REG_SET_H
#define GCC_HARD_REG_SET_H

/* Define the type of a set of hard registers.  */

/* HARD_REG_ELT_TYPE is a typedef of the unsigned integral type which
   will be used for hard reg sets, either alone or in an array.

   If HARD_REG_SET is a macro, its definition is HARD_REG_ELT_TYPE,
   and it has enough bits to represent all the target machine's hard
   registers.  Otherwise, it is a typedef for a suitably sized array
   of HARD_REG_ELT_TYPEs.  HARD_REG_SET_LONGS is defined as how many.

   Note that lots of code assumes that the first part of a regset is
   the same format as a HARD_REG_SET.  To help make sure this is true,
   we only try the widest fast integer mode (HOST_WIDEST_FAST_INT)
   instead of all the smaller types.  This approach loses only if
   there are very few registers and then only in the few cases where
   we have an array of HARD_REG_SETs, so it needn't be as complex as
   it used to be.  */

typedef unsigned HOST_WIDEST_FAST_INT HARD_REG_ELT_TYPE;

#if FIRST_PSEUDO_REGISTER <= HOST_BITS_PER_WIDEST_FAST_INT

#define HARD_REG_SET HARD_REG_ELT_TYPE

#else

#define HARD_REG_SET_LONGS \
 ((FIRST_PSEUDO_REGISTER + HOST_BITS_PER_WIDEST_FAST_INT - 1)	\
  / HOST_BITS_PER_WIDEST_FAST_INT)
typedef HARD_REG_ELT_TYPE HARD_REG_SET[HARD_REG_SET_LONGS];

#endif

/* HARD_REG_SET wrapped into a structure, to make it possible to
   use HARD_REG_SET even in APIs that should not include
   hard-reg-set.h.  */
struct hard_reg_set_container
{
  HARD_REG_SET set;
};

/* HARD_CONST is used to cast a constant to the appropriate type
   for use with a HARD_REG_SET.  */

#define HARD_CONST(X) ((HARD_REG_ELT_TYPE) (X))

/* Define macros SET_HARD_REG_BIT, CLEAR_HARD_REG_BIT and TEST_HARD_REG_BIT
   to set, clear or test one bit in a hard reg set of type HARD_REG_SET.
   All three take two arguments: the set and the register number.

   In the case where sets are arrays of longs, the first argument
   is actually a pointer to a long.

   Define two macros for initializing a set:
   CLEAR_HARD_REG_SET and SET_HARD_REG_SET.
   These take just one argument.

   Also define macros for copying hard reg sets:
   COPY_HARD_REG_SET and COMPL_HARD_REG_SET.
   These take two arguments TO and FROM; they read from FROM
   and store into TO.  COMPL_HARD_REG_SET complements each bit.

   Also define macros for combining hard reg sets:
   IOR_HARD_REG_SET and AND_HARD_REG_SET.
   These take two arguments TO and FROM; they read from FROM
   and combine bitwise into TO.  Define also two variants
   IOR_COMPL_HARD_REG_SET and AND_COMPL_HARD_REG_SET
   which use the complement of the set FROM.

   Also define:

   hard_reg_set_subset_p (X, Y), which returns true if X is a subset of Y.
   hard_reg_set_equal_p (X, Y), which returns true if X and Y are equal.
   hard_reg_set_intersect_p (X, Y), which returns true if X and Y intersect.
   hard_reg_set_empty_p (X), which returns true if X is empty.  */

#define UHOST_BITS_PER_WIDE_INT ((unsigned) HOST_BITS_PER_WIDEST_FAST_INT)

#ifdef HARD_REG_SET

#define SET_HARD_REG_BIT(SET, BIT)  \
 ((SET) |= HARD_CONST (1) << (BIT))
#define CLEAR_HARD_REG_BIT(SET, BIT)  \
 ((SET) &= ~(HARD_CONST (1) << (BIT)))
#define TEST_HARD_REG_BIT(SET, BIT)  \
 (!!((SET) & (HARD_CONST (1) << (BIT))))

#define CLEAR_HARD_REG_SET(TO) ((TO) = HARD_CONST (0))
#define SET_HARD_REG_SET(TO) ((TO) = ~ HARD_CONST (0))

#define COPY_HARD_REG_SET(TO, FROM) ((TO) = (FROM))
#define COMPL_HARD_REG_SET(TO, FROM) ((TO) = ~(FROM))

#define IOR_HARD_REG_SET(TO, FROM) ((TO) |= (FROM))
#define IOR_COMPL_HARD_REG_SET(TO, FROM) ((TO) |= ~ (FROM))
#define AND_HARD_REG_SET(TO, FROM) ((TO) &= (FROM))
#define AND_COMPL_HARD_REG_SET(TO, FROM) ((TO) &= ~ (FROM))

static inline bool
hard_reg_set_subset_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return (x & ~y) == HARD_CONST (0);
}

static inline bool
hard_reg_set_equal_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return x == y;
}

static inline bool
hard_reg_set_intersect_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return (x & y) != HARD_CONST (0);
}

static inline bool
hard_reg_set_empty_p (const HARD_REG_SET x)
{
  return x == HARD_CONST (0);
}

#else

#define SET_HARD_REG_BIT(SET, BIT)		\
  ((SET)[(BIT) / UHOST_BITS_PER_WIDE_INT]	\
   |= HARD_CONST (1) << ((BIT) % UHOST_BITS_PER_WIDE_INT))

#define CLEAR_HARD_REG_BIT(SET, BIT)		\
  ((SET)[(BIT) / UHOST_BITS_PER_WIDE_INT]	\
   &= ~(HARD_CONST (1) << ((BIT) % UHOST_BITS_PER_WIDE_INT)))

#define TEST_HARD_REG_BIT(SET, BIT)		\
  (!!((SET)[(BIT) / UHOST_BITS_PER_WIDE_INT]	\
      & (HARD_CONST (1) << ((BIT) % UHOST_BITS_PER_WIDE_INT))))

#if FIRST_PSEUDO_REGISTER <= 2*HOST_BITS_PER_WIDEST_FAST_INT
#define CLEAR_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = 0;						\
     scan_tp_[1] = 0; } while (0)

#define SET_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = -1;						\
     scan_tp_[1] = -1; } while (0)

#define COPY_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = scan_fp_[0];					\
     scan_tp_[1] = scan_fp_[1]; } while (0)

#define COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = ~ scan_fp_[0];				\
     scan_tp_[1] = ~ scan_fp_[1]; } while (0)

#define AND_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= scan_fp_[0];				\
     scan_tp_[1] &= scan_fp_[1]; } while (0)

#define AND_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= ~ scan_fp_[0];				\
     scan_tp_[1] &= ~ scan_fp_[1]; } while (0)

#define IOR_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= scan_fp_[0];				\
     scan_tp_[1] |= scan_fp_[1]; } while (0)

#define IOR_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= ~ scan_fp_[0];				\
     scan_tp_[1] |= ~ scan_fp_[1]; } while (0)

static inline bool
hard_reg_set_subset_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return (x[0] & ~y[0]) == 0 && (x[1] & ~y[1]) == 0;
}

static inline bool
hard_reg_set_equal_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return x[0] == y[0] && x[1] == y[1];
}

static inline bool
hard_reg_set_intersect_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return (x[0] & y[0]) != 0 || (x[1] & y[1]) != 0;
}

static inline bool
hard_reg_set_empty_p (const HARD_REG_SET x)
{
  return x[0] == 0 && x[1] == 0;
}

#else
#if FIRST_PSEUDO_REGISTER <= 3*HOST_BITS_PER_WIDEST_FAST_INT
#define CLEAR_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = 0;						\
     scan_tp_[1] = 0;						\
     scan_tp_[2] = 0; } while (0)

#define SET_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = -1;						\
     scan_tp_[1] = -1;						\
     scan_tp_[2] = -1; } while (0)

#define COPY_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = scan_fp_[0];					\
     scan_tp_[1] = scan_fp_[1];					\
     scan_tp_[2] = scan_fp_[2]; } while (0)

#define COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = ~ scan_fp_[0];				\
     scan_tp_[1] = ~ scan_fp_[1];				\
     scan_tp_[2] = ~ scan_fp_[2]; } while (0)

#define AND_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= scan_fp_[0];				\
     scan_tp_[1] &= scan_fp_[1];				\
     scan_tp_[2] &= scan_fp_[2]; } while (0)

#define AND_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= ~ scan_fp_[0];				\
     scan_tp_[1] &= ~ scan_fp_[1];				\
     scan_tp_[2] &= ~ scan_fp_[2]; } while (0)

#define IOR_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= scan_fp_[0];				\
     scan_tp_[1] |= scan_fp_[1];				\
     scan_tp_[2] |= scan_fp_[2]; } while (0)

#define IOR_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= ~ scan_fp_[0];				\
     scan_tp_[1] |= ~ scan_fp_[1];				\
     scan_tp_[2] |= ~ scan_fp_[2]; } while (0)

static inline bool
hard_reg_set_subset_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return ((x[0] & ~y[0]) == 0
	  && (x[1] & ~y[1]) == 0
	  && (x[2] & ~y[2]) == 0);
}

static inline bool
hard_reg_set_equal_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return x[0] == y[0] && x[1] == y[1] && x[2] == y[2];
}

static inline bool
hard_reg_set_intersect_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return ((x[0] & y[0]) != 0
	  || (x[1] & y[1]) != 0
	  || (x[2] & y[2]) != 0);
}

static inline bool
hard_reg_set_empty_p (const HARD_REG_SET x)
{
  return x[0] == 0 && x[1] == 0 && x[2] == 0;
}

#else
#if FIRST_PSEUDO_REGISTER <= 4*HOST_BITS_PER_WIDEST_FAST_INT
#define CLEAR_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = 0;						\
     scan_tp_[1] = 0;						\
     scan_tp_[2] = 0;						\
     scan_tp_[3] = 0; } while (0)

#define SET_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     scan_tp_[0] = -1;						\
     scan_tp_[1] = -1;						\
     scan_tp_[2] = -1;						\
     scan_tp_[3] = -1; } while (0)

#define COPY_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = scan_fp_[0];					\
     scan_tp_[1] = scan_fp_[1];					\
     scan_tp_[2] = scan_fp_[2];					\
     scan_tp_[3] = scan_fp_[3]; } while (0)

#define COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] = ~ scan_fp_[0];				\
     scan_tp_[1] = ~ scan_fp_[1];				\
     scan_tp_[2] = ~ scan_fp_[2];				\
     scan_tp_[3] = ~ scan_fp_[3]; } while (0)

#define AND_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= scan_fp_[0];				\
     scan_tp_[1] &= scan_fp_[1];				\
     scan_tp_[2] &= scan_fp_[2];				\
     scan_tp_[3] &= scan_fp_[3]; } while (0)

#define AND_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] &= ~ scan_fp_[0];				\
     scan_tp_[1] &= ~ scan_fp_[1];				\
     scan_tp_[2] &= ~ scan_fp_[2];				\
     scan_tp_[3] &= ~ scan_fp_[3]; } while (0)

#define IOR_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= scan_fp_[0];				\
     scan_tp_[1] |= scan_fp_[1];				\
     scan_tp_[2] |= scan_fp_[2];				\
     scan_tp_[3] |= scan_fp_[3]; } while (0)

#define IOR_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     scan_tp_[0] |= ~ scan_fp_[0];				\
     scan_tp_[1] |= ~ scan_fp_[1];				\
     scan_tp_[2] |= ~ scan_fp_[2];				\
     scan_tp_[3] |= ~ scan_fp_[3]; } while (0)

static inline bool
hard_reg_set_subset_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return ((x[0] & ~y[0]) == 0
	  && (x[1] & ~y[1]) == 0
	  && (x[2] & ~y[2]) == 0
	  && (x[3] & ~y[3]) == 0);
}

static inline bool
hard_reg_set_equal_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return x[0] == y[0] && x[1] == y[1] && x[2] == y[2] && x[3] == y[3];
}

static inline bool
hard_reg_set_intersect_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  return ((x[0] & y[0]) != 0
	  || (x[1] & y[1]) != 0
	  || (x[2] & y[2]) != 0
	  || (x[3] & y[3]) != 0);
}

static inline bool
hard_reg_set_empty_p (const HARD_REG_SET x)
{
  return x[0] == 0 && x[1] == 0 && x[2] == 0 && x[3] == 0;
}

#else /* FIRST_PSEUDO_REGISTER > 4*HOST_BITS_PER_WIDEST_FAST_INT */

#define CLEAR_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ = 0; } while (0)

#define SET_HARD_REG_SET(TO)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ = -1; } while (0)

#define COPY_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ = *scan_fp_++; } while (0)

#define COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ = ~ *scan_fp_++; } while (0)

#define AND_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ &= *scan_fp_++; } while (0)

#define AND_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ &= ~ *scan_fp_++; } while (0)

#define IOR_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ |= *scan_fp_++; } while (0)

#define IOR_COMPL_HARD_REG_SET(TO, FROM)  \
do { HARD_REG_ELT_TYPE *scan_tp_ = (TO);			\
     const HARD_REG_ELT_TYPE *scan_fp_ = (FROM);		\
     int i;							\
     for (i = 0; i < HARD_REG_SET_LONGS; i++)			\
       *scan_tp_++ |= ~ *scan_fp_++; } while (0)

static inline bool
hard_reg_set_subset_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  int i;

  for (i = 0; i < HARD_REG_SET_LONGS; i++)
    if ((x[i] & ~y[i]) != 0)
      return false;
  return true;
}

static inline bool
hard_reg_set_equal_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  int i;

  for (i = 0; i < HARD_REG_SET_LONGS; i++)
    if (x[i] != y[i])
      return false;
  return true;
}

static inline bool
hard_reg_set_intersect_p (const HARD_REG_SET x, const HARD_REG_SET y)
{
  int i;

  for (i = 0; i < HARD_REG_SET_LONGS; i++)
    if ((x[i] & y[i]) != 0)
      return true;
  return false;
}

static inline bool
hard_reg_set_empty_p (const HARD_REG_SET x)
{
  int i;

  for (i = 0; i < HARD_REG_SET_LONGS; i++)
    if (x[i] != 0)
      return false;
  return true;
}

#endif
#endif
#endif
#endif

/* Iterator for hard register sets.  */

struct hard_reg_set_iterator
{
  /* Pointer to the current element.  */
  HARD_REG_ELT_TYPE *pelt;

  /* The length of the set.  */
  unsigned short length;

  /* Word within the current element.  */
  unsigned short word_no;

  /* Contents of the actually processed word.  When finding next bit
     it is shifted right, so that the actual bit is always the least
     significant bit of ACTUAL.  */
  HARD_REG_ELT_TYPE bits;
};

#define HARD_REG_ELT_BITS UHOST_BITS_PER_WIDE_INT

/* The implementation of the iterator functions is fully analogous to
   the bitmap iterators.  */
static inline void
hard_reg_set_iter_init (hard_reg_set_iterator *iter, HARD_REG_SET set,
                        unsigned min, unsigned *regno)
{
#ifdef HARD_REG_SET_LONGS
  iter->pelt = set;
  iter->length = HARD_REG_SET_LONGS;
#else
  iter->pelt = &set;
  iter->length = 1;
#endif
  iter->word_no = min / HARD_REG_ELT_BITS;
  if (iter->word_no < iter->length)
    {
      iter->bits = iter->pelt[iter->word_no];
      iter->bits >>= min % HARD_REG_ELT_BITS;

      /* This is required for correct search of the next bit.  */
      min += !iter->bits;
    }
  *regno = min;
}

static inline bool
hard_reg_set_iter_set (hard_reg_set_iterator *iter, unsigned *regno)
{
  while (1)
    {
      /* Return false when we're advanced past the end of the set.  */
      if (iter->word_no >= iter->length)
        return false;

      if (iter->bits)
        {
          /* Find the correct bit and return it.  */
          while (!(iter->bits & 1))
            {
              iter->bits >>= 1;
              *regno += 1;
            }
          return (*regno < FIRST_PSEUDO_REGISTER);
        }

      /* Round to the beginning of the next word.  */
      *regno = (*regno + HARD_REG_ELT_BITS - 1);
      *regno -= *regno % HARD_REG_ELT_BITS;

      /* Find the next non-zero word.  */
      while (++iter->word_no < iter->length)
        {
          iter->bits = iter->pelt[iter->word_no];
          if (iter->bits)
            break;
          *regno += HARD_REG_ELT_BITS;
        }
    }
}

static inline void
hard_reg_set_iter_next (hard_reg_set_iterator *iter, unsigned *regno)
{
  iter->bits >>= 1;
  *regno += 1;
}

#define EXECUTE_IF_SET_IN_HARD_REG_SET(SET, MIN, REGNUM, ITER)          \
  for (hard_reg_set_iter_init (&(ITER), (SET), (MIN), &(REGNUM));       \
       hard_reg_set_iter_set (&(ITER), &(REGNUM));                      \
       hard_reg_set_iter_next (&(ITER), &(REGNUM)))


/* Define some standard sets of registers.  */

/* Indexed by hard register number, contains 1 for registers
   that are being used for global register decls.
   These must be exempt from ordinary flow analysis
   and are also considered fixed.  */

extern char global_regs[FIRST_PSEUDO_REGISTER];

struct simplifiable_subreg;
struct subreg_shape;

struct simplifiable_subregs_hasher : nofree_ptr_hash <simplifiable_subreg>
{
  typedef const subreg_shape *compare_type;

  static inline hashval_t hash (const simplifiable_subreg *);
  static inline bool equal (const simplifiable_subreg *, const subreg_shape *);
};

struct target_hard_regs {
  void finalize ();

  /* The set of registers that actually exist on the current target.  */
  HARD_REG_SET x_accessible_reg_set;

  /* The set of registers that should be considered to be register
     operands.  It is a subset of x_accessible_reg_set.  */
  HARD_REG_SET x_operand_reg_set;

  /* Indexed by hard register number, contains 1 for registers
     that are fixed use (stack pointer, pc, frame pointer, etc.;.
     These are the registers that cannot be used to allocate
     a pseudo reg whose life does not cross calls.  */
  char x_fixed_regs[FIRST_PSEUDO_REGISTER];

  /* The same info as a HARD_REG_SET.  */
  HARD_REG_SET x_fixed_reg_set;

  /* Indexed by hard register number, contains 1 for registers
     that are fixed use or are clobbered by function calls.
     These are the registers that cannot be used to allocate
     a pseudo reg whose life crosses calls.  */
  char x_call_used_regs[FIRST_PSEUDO_REGISTER];

  char x_call_really_used_regs[FIRST_PSEUDO_REGISTER];

  /* The same info as a HARD_REG_SET.  */
  HARD_REG_SET x_call_used_reg_set;

  /* Contains registers that are fixed use -- i.e. in fixed_reg_set -- or
     a function value return register or TARGET_STRUCT_VALUE_RTX or
     STATIC_CHAIN_REGNUM.  These are the registers that cannot hold quantities
     across calls even if we are willing to save and restore them.  */
  HARD_REG_SET x_call_fixed_reg_set;

  /* Contains registers that are fixed use -- i.e. in fixed_reg_set -- but
     only if they are not merely part of that set because they are global
     regs.  Global regs that are not otherwise fixed can still take part
     in register allocation.  */
  HARD_REG_SET x_fixed_nonglobal_reg_set;

  /* Contains 1 for registers that are set or clobbered by calls.  */
  /* ??? Ideally, this would be just call_used_regs plus global_regs, but
     for someone's bright idea to have call_used_regs strictly include
     fixed_regs.  Which leaves us guessing as to the set of fixed_regs
     that are actually preserved.  We know for sure that those associated
     with the local stack frame are safe, but scant others.  */
  HARD_REG_SET x_regs_invalidated_by_call;

  /* Call used hard registers which can not be saved because there is no
     insn for this.  */
  HARD_REG_SET x_no_caller_save_reg_set;

  /* Table of register numbers in the order in which to try to use them.  */
  int x_reg_alloc_order[FIRST_PSEUDO_REGISTER];

  /* The inverse of reg_alloc_order.  */
  int x_inv_reg_alloc_order[FIRST_PSEUDO_REGISTER];

  /* For each reg class, a HARD_REG_SET saying which registers are in it.  */
  HARD_REG_SET x_reg_class_contents[N_REG_CLASSES];

  /* For each reg class, a boolean saying whether the class contains only
     fixed registers.  */
  bool x_class_only_fixed_regs[N_REG_CLASSES];

  /* For each reg class, number of regs it contains.  */
  unsigned int x_reg_class_size[N_REG_CLASSES];

  /* For each reg class, table listing all the classes contained in it.  */
  enum reg_class x_reg_class_subclasses[N_REG_CLASSES][N_REG_CLASSES];

  /* For each pair of reg classes,
     a largest reg class contained in their union.  */
  enum reg_class x_reg_class_subunion[N_REG_CLASSES][N_REG_CLASSES];

  /* For each pair of reg classes,
     the smallest reg class that contains their union.  */
  enum reg_class x_reg_class_superunion[N_REG_CLASSES][N_REG_CLASSES];

  /* Vector indexed by hardware reg giving its name.  */
  const char *x_reg_names[FIRST_PSEUDO_REGISTER];

  /* Records which registers can form a particular subreg, with the subreg
     being identified by its outer mode, inner mode and offset.  */
  hash_table <simplifiable_subregs_hasher> *x_simplifiable_subregs;
};

extern struct target_hard_regs default_target_hard_regs;
#if SWITCHABLE_TARGET
extern struct target_hard_regs *this_target_hard_regs;
#else
#define this_target_hard_regs (&default_target_hard_regs)
#endif

#define accessible_reg_set \
  (this_target_hard_regs->x_accessible_reg_set)
#define operand_reg_set \
  (this_target_hard_regs->x_operand_reg_set)
#define fixed_regs \
  (this_target_hard_regs->x_fixed_regs)
#define fixed_reg_set \
  (this_target_hard_regs->x_fixed_reg_set)
#define fixed_nonglobal_reg_set \
  (this_target_hard_regs->x_fixed_nonglobal_reg_set)
#define call_used_regs \
  (this_target_hard_regs->x_call_used_regs)
#define call_really_used_regs \
  (this_target_hard_regs->x_call_really_used_regs)
#define call_used_reg_set \
  (this_target_hard_regs->x_call_used_reg_set)
#define call_fixed_reg_set \
  (this_target_hard_regs->x_call_fixed_reg_set)
#define regs_invalidated_by_call \
  (this_target_hard_regs->x_regs_invalidated_by_call)
#define no_caller_save_reg_set \
  (this_target_hard_regs->x_no_caller_save_reg_set)
#define reg_alloc_order \
  (this_target_hard_regs->x_reg_alloc_order)
#define inv_reg_alloc_order \
  (this_target_hard_regs->x_inv_reg_alloc_order)
#define reg_class_contents \
  (this_target_hard_regs->x_reg_class_contents)
#define class_only_fixed_regs \
  (this_target_hard_regs->x_class_only_fixed_regs)
#define reg_class_size \
  (this_target_hard_regs->x_reg_class_size)
#define reg_class_subclasses \
  (this_target_hard_regs->x_reg_class_subclasses)
#define reg_class_subunion \
  (this_target_hard_regs->x_reg_class_subunion)
#define reg_class_superunion \
  (this_target_hard_regs->x_reg_class_superunion)
#define reg_names \
  (this_target_hard_regs->x_reg_names)

/* Vector indexed by reg class giving its name.  */

extern const char * reg_class_names[];

/* Given a hard REGN a FROM mode and a TO mode, return true if
   REGN can change from mode FROM to mode TO.  */
#define REG_CAN_CHANGE_MODE_P(REGN, FROM, TO)                          \
  (targetm.can_change_mode_class (FROM, TO, REGNO_REG_CLASS (REGN)))

#endif /* ! GCC_HARD_REG_SET_H */
