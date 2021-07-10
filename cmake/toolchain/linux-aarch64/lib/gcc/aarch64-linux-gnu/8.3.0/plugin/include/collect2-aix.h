/* AIX cross support for collect2.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.

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

#ifndef GCC_COLLECT2_AIX_H
#define GCC_COLLECT2_AIX_H
/* collect2-aix.c requires mmap support.  It should otherwise be
   fairly portable.  */
#if defined(CROSS_DIRECTORY_STRUCTURE) \
    && defined(TARGET_AIX_VERSION) \
    && HAVE_MMAP

#define CROSS_AIX_SUPPORT 1

/* -------------------------------------------------------------------------
   Definitions adapted from bfd.  (Fairly heavily adapted in some cases.)
   ------------------------------------------------------------------------- */

/* Compatibility types for bfd.  */
typedef unsigned HOST_WIDE_INT bfd_vma;

/* The size of an archive's fl_magic field.  */
#define FL_MAGIC_SIZE 8

/* The expected contents of fl_magic for big archives.  */
#define FL_MAGIC_BIG_AR "<bigaf>\012"

/* The size of each offset string in the header of a big archive.  */
#define AR_BIG_OFFSET_SIZE 20

/* The format of the file header in a "big" XCOFF archive.  */
struct external_big_ar_filehdr
{
  /* Magic string.  */
  char fl_magic[FL_MAGIC_SIZE];

  /* Offset of the member table (decimal ASCII string).  */
  char fl_memoff[AR_BIG_OFFSET_SIZE];

  /* Offset of the global symbol table for 32-bit objects (decimal ASCII
     string).  */
  char fl_symoff[AR_BIG_OFFSET_SIZE];

  /* Offset of the global symbol table for 64-bit objects (decimal ASCII
     string).  */
  char fl_symoff64[AR_BIG_OFFSET_SIZE];

  /* Offset of the first member in the archive (decimal ASCII string).  */
  char fl_firstmemoff[AR_BIG_OFFSET_SIZE];

  /* Offset of the last member in the archive (decimal ASCII string).  */
  char fl_lastmemoff[AR_BIG_OFFSET_SIZE];

  /* Offset of the first member on the free list (decimal ASCII
     string).  */
  char fl_freeoff[AR_BIG_OFFSET_SIZE];
};

/* Each archive name is followed by this many bytes of magic string.  */
#define SXCOFFARFMAG 2

/* The format of a member header in a "big" XCOFF archive.  */
struct external_big_ar_member
{
  /* File size not including the header (decimal ASCII string).  */
  char ar_size[AR_BIG_OFFSET_SIZE];

  /* File offset of next archive member (decimal ASCII string).  */
  char ar_nextoff[AR_BIG_OFFSET_SIZE];

  /* File offset of previous archive member (decimal ASCII string).  */
  char ar_prevoff[AR_BIG_OFFSET_SIZE];

  /* File mtime (decimal ASCII string).  */
  char ar_date[12];

  /* File UID (decimal ASCII string).  */
  char ar_uid[12];

  /* File GID (decimal ASCII string).  */
  char ar_gid[12];

  /* File mode (octal ASCII string).  */
  char ar_mode[12];

  /* Length of file name (decimal ASCII string).  */
  char ar_namlen[4];

  /* This structure is followed by the file name.  The length of the
     name is given in the namlen field.  If the length of the name is
     odd, the name is followed by a null byte.  The name and optional
     null byte are followed by XCOFFARFMAG, which is not included in
     namlen.  The contents of the archive member follow; the number of
     bytes is given in the size field.  */
};

/* The known values of f_magic in an XCOFF file header.  */
#define U802WRMAGIC 0730	/* Writeable text segments.  */
#define U802ROMAGIC 0735	/* Readonly sharable text segments.  */
#define U802TOCMAGIC 0737	/* Readonly text segments and TOC.  */
#define U803XTOCMAGIC 0757	/* Aix 4.3 64-bit XCOFF.  */
#define U64_TOCMAGIC 0767	/* AIX 5+ 64-bit XCOFF.  */

/* The number of bytes in an XCOFF file's f_magic field.  */
#define F_MAGIC_SIZE 2

/* The format of a 32-bit XCOFF file header.  */
struct external_filehdr_32
{
  /* The magic number.  */
  char f_magic[F_MAGIC_SIZE];

  /* The number of sections.  */
  char f_nscns[2];

  /* Time & date stamp.  */
  char f_timdat[4];

  /* The offset of the symbol table from the start of the file.  */
  char f_symptr[4];

  /* The number of entries in the symbol table.  */
  char f_nsyms[4];

  /* The size of the auxiliary header.  */
  char f_opthdr[2];

  /* Flags.  */
  char f_flags[2];
};

/* The format of a 64-bit XCOFF file header.  */
struct external_filehdr_64
{
  /* The magic number.  */
  char f_magic[F_MAGIC_SIZE];

  /* The number of sections.  */
  char f_nscns[2];

  /* Time & date stamp.  */
  char f_timdat[4];

  /* The offset of the symbol table from the start of the file.  */
  char f_symptr[8];

  /* The size of the auxiliary header.  */
  char f_opthdr[2];

  /* Flags.  */
  char f_flags[2];

  /* The number of entries in the symbol table.  */
  char f_nsyms[4];
};

/* An internal representation of the XCOFF file header.  */
struct internal_filehdr
{
  unsigned short f_magic;
  unsigned short f_nscns;
  long f_timdat;
  bfd_vma f_symptr;
  long f_nsyms;
  unsigned short f_opthdr;
  unsigned short f_flags;
};

/* Symbol classes have their names in the debug section if this flag
   is set.  */
#define DBXMASK 0x80

/* The format of an XCOFF symbol-table entry.  */
struct external_syment
{
  union {
    struct {
      union {
	/* The name of the symbol.  There is an implicit null character
	   after the end of the array.  */
	char n_name[8];
	struct {
	  /* If n_zeroes is zero, n_offset is the offset the name from
	     the start of the string table.  */
	  char n_zeroes[4];
	  char n_offset[4];
	} u;
      } u;

      /* The symbol's value.  */
      char n_value[4];
    } xcoff32;
    struct {
      /* The symbol's value.  */
      char n_value[8];

      /* The offset of the symbol from the start of the string table.  */
      char n_offset[4];
    } xcoff64;
  } u;

  /* The number of the section to which this symbol belongs.  */
  char n_scnum[2];

  /* The type of symbol.  (It can be interpreted as an n_lang
     and an n_cpu byte, but we don't care about that here.)  */
  char n_type[2];

  /* The class of symbol (a C_* value).  */
  char n_sclass[1];

  /* The number of auxiliary symbols attached to this entry.  */
  char n_numaux[1];
};

/* Definitions required by collect2.  */
#define C_EXT 2

#define F_SHROBJ    0x2000
#define F_LOADONLY  0x4000

#define N_UNDEF ((short) 0)
#define N_TMASK 060
#define N_BTSHFT 4

#define DT_NON 0
#define DT_FCN 2

/* -------------------------------------------------------------------------
   Local code.
   ------------------------------------------------------------------------- */

/* An internal representation of an XCOFF symbol-table entry,
   which is associated with the API-defined SYMENT type.  */
struct internal_syment
{
  char n_name[9];
  unsigned int n_zeroes;
  bfd_vma n_offset;
  bfd_vma n_value;
  short n_scnum;
  unsigned short n_flags;
  unsigned short n_type;
  unsigned char n_sclass;
  unsigned char n_numaux;
};
typedef struct internal_syment SYMENT;

/* The internal representation of the API-defined LDFILE type.  */
struct internal_ldfile
{
  /* The file handle for the associated file, or -1 if it hasn't been
     opened yet.  */
  int fd;

  /* The start of the current XCOFF object, if one has been mapped
     into memory.  Null otherwise.  */
  char *object;

  /* The offset of OBJECT from the start of the containing page.  */
  size_t page_offset;

  /* The size of the file pointed to by OBJECT.  Valid iff OFFSET
     is nonnull.  */
  size_t object_size;

  /* The offset of the next member in an archive after OBJECT,
     or -1 if this isn't an archive.  Valid iff OFFSET is nonnull.  */
  off_t next_member;

  /* The parsed version of the XCOFF file header.  */
  struct internal_filehdr filehdr;
};
typedef struct internal_ldfile LDFILE;

/* The API allows the file header to be directly accessed via this macro.  */
#define HEADER(FILE) ((FILE)->filehdr)

/* API-defined return codes.  SUCCESS must be > 0 and FAILURE must be <= 0.  */
#define SUCCESS 1
#define FAILURE 0

/* API-defined functions.  */
extern LDFILE *ldopen (char *, LDFILE *);
extern char *ldgetname (LDFILE *, SYMENT *);
extern int ldtbread (LDFILE *, long, SYMENT *);
extern int ldclose (LDFILE *);

#endif

#endif /* GCC_COLLECT2_AIX_H */
