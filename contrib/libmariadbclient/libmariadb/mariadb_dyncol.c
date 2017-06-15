/* Copyright (c) 2011,2013  Monty Program Ab;
   Copyright (c) 2011,2012 Oleksandr Byelkin

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:

   1. Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.

   THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND ANY
   EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
   PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
   SUCH DAMAGE.
*/

/*
 Numeric format:
 ===============
  * Fixed header part
    1 byte flags:
      0,1 bits - <offset size> - 1
      2-7 bits - 0
    2 bytes column counter
  * Columns directory sorted by column number, each entry contains of:
    2 bytes column number
    <offset size> bytes (1-4) combined offset from beginning of
      the data segment + 3 bit type
  * Data of above columns size of data and length depend on type

 Columns with names:
 ===================
  * Fixed header part
    1 byte flags:
      0,1 bits - <offset size> - 2
      2 bit    - 1 (means format with names)
      3,4 bits - 00 (means <names offset size> - 2,
                     now 2 is the only supported size)
      5-7 bits - 0
    2 bytes column counter
  * Variable header part (now it is actually fixed part)
    <names offset size> (2) bytes size of stored names pool
  * Column directory sorted by names, each consists of
    <names offset size> (2) bytes offset of name
    <offset size> bytes (2-5)bytes combined offset from beginning of
      the data segment + 4 bit type
  * Names stored one after another
  * Data of above columns size of data and length depend on type
*/

#include <stdio.h>
#include <ma_global.h>
#include <ma_sys.h>
#include <ma_string.h>
#include <ma_hash.h>
#include <mariadb_dyncol.h>
#include <mysql.h>



#ifndef LIBMARIADB
uint32 copy_and_convert(char *to, uint32 to_length, MARIADB_CHARSET_INFO *to_cs,
			const char *from, uint32 from_length,
			MARIADB_CHARSET_INFO *from_cs, uint *errors);
#else

size_t mariadb_time_to_string(const MYSQL_TIME *tm, char *time_str, size_t len,
                           unsigned int digits);
size_t STDCALL mariadb_convert_string(const char *from, size_t *from_len, MARIADB_CHARSET_INFO *from_cs,
                                      char *to, size_t *to_len, MARIADB_CHARSET_INFO *to_cs, int *errorcode);
#endif
/*
  Flag byte bits

  2 bits which determinate size of offset in the header -1
*/
/* mask to get above bits */
#define DYNCOL_FLG_OFFSET   (1|2)
#define DYNCOL_FLG_NAMES    4
#define DYNCOL_FLG_NMOFFSET (8|16)
/**
  All known flags mask that could be set.

  @note DYNCOL_FLG_NMOFFSET should be 0 for now.
*/
#define DYNCOL_FLG_KNOWN  (1|2|4)

/* formats */
enum enum_dyncol_format
{
  dyncol_fmt_num= 0,
  dyncol_fmt_str= 1
};

/* dynamic column size reserve */
#define DYNCOL_SYZERESERVE 80

#define DYNCOL_OFFSET_ERROR 0xffffffff

/* length of fixed string header 1 byte - flags, 2 bytes - columns counter */
#define FIXED_HEADER_SIZE 3
/*
  length of fixed string header with names
  1 byte - flags, 2 bytes - columns counter,  2 bytes - name pool size
*/
#define FIXED_HEADER_SIZE_NM 5

#define COLUMN_NUMBER_SIZE 2
/* 2 bytes offset from the name pool */
#define COLUMN_NAMEPTR_SIZE 2

#define MAX_OFFSET_LENGTH    4
#define MAX_OFFSET_LENGTH_NM 5

#define DYNCOL_NUM_CHAR 6

my_bool mariadb_dyncol_has_names(DYNAMIC_COLUMN *str)
{
  if (str->length < 1)
    return FALSE;
  return test(str->str[0] & DYNCOL_FLG_NAMES);
}

static enum enum_dyncol_func_result
dynamic_column_time_store(DYNAMIC_COLUMN *str,
                          MYSQL_TIME *value, enum enum_dyncol_format format);
static enum enum_dyncol_func_result
dynamic_column_date_store(DYNAMIC_COLUMN *str,
                          MYSQL_TIME *value);
static enum enum_dyncol_func_result
dynamic_column_time_read_internal(DYNAMIC_COLUMN_VALUE *store_it_here,
                                  uchar *data, size_t length);
static enum enum_dyncol_func_result
dynamic_column_date_read_internal(DYNAMIC_COLUMN_VALUE *store_it_here,
                                  uchar *data, size_t length);
static enum enum_dyncol_func_result
dynamic_column_get_internal(DYNAMIC_COLUMN *str,
                                DYNAMIC_COLUMN_VALUE *store_it_here,
                                uint num_key, LEX_STRING *str_key);
static enum enum_dyncol_func_result
dynamic_column_exists_internal(DYNAMIC_COLUMN *str, uint num_key,
                               LEX_STRING *str_key);
static enum enum_dyncol_func_result
dynamic_column_update_many_fmt(DYNAMIC_COLUMN *str,
                               uint add_column_count,
                               void *column_keys,
                               DYNAMIC_COLUMN_VALUE *values,
                               my_bool string_keys);
static int plan_sort_num(const void *a, const void *b);
static int plan_sort_named(const void *a, const void *b);

/*
  Structure to hold information about dynamic columns record and
  iterate through it.
*/

struct st_dyn_header
{
  uchar *header, *nmpool, *dtpool, *data_end;
  size_t offset_size;
  size_t entry_size;
  size_t header_size;
  size_t nmpool_size;
  size_t data_size;
  /* dyncol_fmt_num - numeric columns, dyncol_fmt_str - column names */
  enum enum_dyncol_format format;
  uint column_count;

  uchar *entry, *data, *name;
  size_t offset;
  size_t length;
  enum enum_dynamic_column_type type;
};

typedef struct st_dyn_header DYN_HEADER;

static inline my_bool read_fixed_header(DYN_HEADER *hdr,
                                        DYNAMIC_COLUMN *str);
static void set_fixed_header(DYNAMIC_COLUMN *str,
                             uint offset_size,
                             uint column_count);

/*
  Calculate entry size (E) and header size (H) by offset size (O) and column
  count (C) and fixed part of entry size (F).
*/

#define calc_param(E,H,F,O,C) do { \
  (*(E))= (O) + F;                 \
  (*(H))= (*(E)) * (C);            \
}while(0);


/**
  Name pool size functions, for numeric format it is 0
*/

static size_t name_size_num(void *keys __attribute__((unused)),
                            uint i __attribute__((unused)))
{
  return 0;
}


/**
  Name pool size functions.
*/
static size_t name_size_named(void *keys, uint i)
{
  return ((LEX_STRING *) keys)[i].length;
}


/**
  Comparator function for references on column numbers for qsort
  (numeric format)
*/

static int column_sort_num(const void *a, const void *b)
{
  return **((uint **)a) - **((uint **)b);
}

/**
  Comparator function for references on column numbers for qsort
  (names format)
*/

int mariadb_dyncol_column_cmp_named(const LEX_STRING *s1, const LEX_STRING *s2)
{
  /*
    We compare instead of subtraction to avoid data loss in case of huge
    length difference (more then fit in int).
  */
  int rc= (s1->length > s2->length ? 1 :
           (s1->length < s2->length ? -1 : 0));
  if (rc == 0)
    rc= memcmp((void *)s1->str, (void *)s2->str,
               (size_t) s1->length);
  return rc;
}


/**
  Comparator function for references on column numbers for qsort
  (names format)
*/

static int column_sort_named(const void *a, const void *b)
{
  return mariadb_dyncol_column_cmp_named(*((LEX_STRING **)a),
                                         *((LEX_STRING **)b));
}


/**
  Check limit function (numeric format)
*/

static my_bool check_limit_num(const void *val)
{
  return **((uint **)val) > UINT_MAX16;
}


/**
  Check limit function (names format)
*/

static my_bool check_limit_named(const void *val)
{
  return (*((LEX_STRING **)val))->length > MAX_NAME_LENGTH;
}


/**
  Write numeric format static header part.
*/

static void set_fixed_header_num(DYNAMIC_COLUMN *str, DYN_HEADER *hdr)
{
  set_fixed_header(str, (uint)hdr->offset_size, hdr->column_count);
  hdr->header= (uchar *)str->str + FIXED_HEADER_SIZE;
  hdr->nmpool= hdr->dtpool= hdr->header + hdr->header_size;
}


/**
  Write names format static header part.
*/

static void set_fixed_header_named(DYNAMIC_COLUMN *str, DYN_HEADER *hdr)
{
  DBUG_ASSERT(hdr->column_count <= 0xffff);
  DBUG_ASSERT(hdr->offset_size <= MAX_OFFSET_LENGTH_NM);
  /* size of data offset, named format flag, size of names offset (0 means 2) */
  str->str[0]=
    (char) ((str->str[0] & ~(DYNCOL_FLG_OFFSET | DYNCOL_FLG_NMOFFSET)) |
            (hdr->offset_size - 2) | DYNCOL_FLG_NAMES);
  int2store(str->str + 1, hdr->column_count);        /* columns number */
  int2store(str->str + 3, hdr->nmpool_size);
  hdr->header= (uchar *)str->str + FIXED_HEADER_SIZE_NM;
  hdr->nmpool= hdr->header + hdr->header_size;
  hdr->dtpool= hdr->nmpool + hdr->nmpool_size;
}


/**
  Store offset and type information in the given place

  @param place           Beginning of the index entry
  @param offset_size     Size of offset field in bytes
  @param type            Type to be written
  @param offset          Offset to be written
*/

static my_bool type_and_offset_store_num(uchar *place, size_t offset_size,
                                         DYNAMIC_COLUMN_TYPE type,
                                         size_t offset)
{
  ulong val = (((ulong) offset) << 3) | (type - 1);
  DBUG_ASSERT(type != DYN_COL_NULL);
  DBUG_ASSERT(((type - 1) & (~7)) == 0); /* fit in 3 bits */
  DBUG_ASSERT(offset_size >= 1 && offset_size <= 4);

  /* Index entry starts with column number; jump over it */
  place+= COLUMN_NUMBER_SIZE;

  switch (offset_size) {
  case 1:
    if (offset >= 0x1f)          /* all 1 value is reserved */
      return TRUE;
    place[0]= (uchar)val;
    break;
  case 2:
    if (offset >= 0x1fff)        /* all 1 value is reserved */
      return TRUE;
    int2store(place, val);
    break;
  case 3:
    if (offset >= 0x1fffff)      /* all 1 value is reserved */
      return TRUE;
    int3store(place, val);
    break;
  case 4:
    if (offset >= 0x1fffffff)    /* all 1 value is reserved */
      return TRUE;
    int4store(place, val);
    break;
  default:
      return TRUE;
  }
  return FALSE;
}


static my_bool type_and_offset_store_named(uchar *place, size_t offset_size,
                                           DYNAMIC_COLUMN_TYPE type,
                                           size_t offset)
{
  ulonglong val = (((ulong) offset) << 4) | (type - 1);
  DBUG_ASSERT(type != DYN_COL_NULL);
  DBUG_ASSERT(((type - 1) & (~0xf)) == 0); /* fit in 4 bits */
  DBUG_ASSERT(offset_size >= 2 && offset_size <= 5);

  /* Index entry starts with name offset; jump over it */
  place+= COLUMN_NAMEPTR_SIZE;
  switch (offset_size) {
  case 2:
    if (offset >= 0xfff)          /* all 1 value is reserved */
      return TRUE;
    int2store(place, val);
    break;
  case 3:
    if (offset >= 0xfffff)        /* all 1 value is reserved */
      return TRUE;
    int3store(place, val);
    break;
  case 4:
    if (offset >= 0xfffffff)      /* all 1 value is reserved */
      return TRUE;
    int4store(place, val);
    break;
  case 5:
#if SIZEOF_SIZE_T > 4
    if (offset >= 0xfffffffffull)    /* all 1 value is reserved */
      return TRUE;
#endif
    int5store(place, val);
    break;
  case 1:
  default:
      return TRUE;
  }
  return FALSE;
}

/**
  Write numeric format header entry
   2 bytes - column number
   1-4 bytes - data offset combined with type

  @param hdr             descriptor of dynamic column record
  @param column_key      pointer to uint (column number)
  @param value           value which will be written (only type used)
  @param offset          offset of the data
*/

static my_bool put_header_entry_num(DYN_HEADER *hdr,
                                    void *column_key,
                                    DYNAMIC_COLUMN_VALUE *value,
                                    size_t offset)
{
  uint *column_number= (uint *)column_key;
  int2store(hdr->entry, *column_number);
  DBUG_ASSERT(hdr->nmpool_size == 0);
  if (type_and_offset_store_num(hdr->entry, hdr->offset_size,
                                value->type,
                                offset))
      return TRUE;
  hdr->entry= hdr->entry + hdr->entry_size;
  return FALSE;
}


/**
  Write names format header entry
   1 byte - name length
   2 bytes - name offset in the name pool
   1-4 bytes - data offset combined with type

  @param hdr             descriptor of dynamic column record
  @param column_key      pointer to LEX_STRING (column name)
  @param value           value which will be written (only type used)
  @param offset          offset of the data
*/

static my_bool put_header_entry_named(DYN_HEADER *hdr,
                                      void *column_key,
                                      DYNAMIC_COLUMN_VALUE *value,
                                      size_t offset)
{
  LEX_STRING *column_name= (LEX_STRING *)column_key;
  DBUG_ASSERT(column_name->length <= MAX_NAME_LENGTH);
  DBUG_ASSERT(hdr->name - hdr->nmpool < (long) 0x10000L);
  int2store(hdr->entry, hdr->name - hdr->nmpool);
  memcpy(hdr->name, column_name->str, column_name->length);
  DBUG_ASSERT(hdr->nmpool_size != 0 || column_name->length == 0);
  if (type_and_offset_store_named(hdr->entry, hdr->offset_size,
                                  value->type,
                                  offset))
    return TRUE;
  hdr->entry+= hdr->entry_size;
  hdr->name+= column_name->length;
  return FALSE;
}


/**
  Calculate length of offset field for given data length

  @param data_length     Length of the data segment

  @return number of bytes
*/

static size_t dynamic_column_offset_bytes_num(size_t data_length)
{
  if (data_length < 0x1f)                /* all 1 value is reserved */
    return 1;
  if (data_length < 0x1fff)              /* all 1 value is reserved */
    return 2;
  if (data_length < 0x1fffff)            /* all 1 value is reserved */
    return 3;
  if (data_length < 0x1fffffff)          /* all 1 value is reserved */
    return 4;
  return MAX_OFFSET_LENGTH + 1;          /* For an error generation*/
}

static size_t dynamic_column_offset_bytes_named(size_t data_length)
{
  if (data_length < 0xfff)                /* all 1 value is reserved */
    return 2;
  if (data_length < 0xfffff)              /* all 1 value is reserved */
    return 3;
  if (data_length < 0xfffffff)            /* all 1 value is reserved */
    return 4;
#if SIZEOF_SIZE_T > 4
  if (data_length < 0xfffffffffull)       /* all 1 value is reserved */
#endif
    return 5;
  return MAX_OFFSET_LENGTH_NM + 1;        /* For an error generation */
}

/**
  Read offset and type information from index entry

  @param type            Where to put type info
  @param offset          Where to put offset info
  @param place           beginning of the type and offset
  @param offset_size     Size of offset field in bytes
*/

static my_bool type_and_offset_read_num(DYNAMIC_COLUMN_TYPE *type,
                                        size_t *offset,
                                        uchar *place, size_t offset_size)
{
  ulong UNINIT_VAR(val);
  ulong UNINIT_VAR(lim);

  DBUG_ASSERT(offset_size >= 1 && offset_size <= 4);

  switch (offset_size) {
  case 1:
    val= (ulong)place[0];
    lim= 0x1f;
    break;
  case 2:
    val= uint2korr(place);
    lim= 0x1fff;
    break;
  case 3:
    val= uint3korr(place);
    lim= 0x1fffff;
    break;
  case 4:
    val= uint4korr(place);
    lim= 0x1fffffff;
    break;
  default:
    DBUG_ASSERT(0);                             /* impossible */
    return 1;
  }
  *type= (val & 0x7) + 1;
  *offset= val >> 3;
  return (*offset >= lim);
}

static my_bool type_and_offset_read_named(DYNAMIC_COLUMN_TYPE *type,
                                          size_t *offset,
                                          uchar *place, size_t offset_size)
{
  ulonglong UNINIT_VAR(val);
  ulonglong UNINIT_VAR(lim);
  DBUG_ASSERT(offset_size >= 2 && offset_size <= 5);

  switch (offset_size) {
  case 2:
    val= uint2korr(place);
    lim= 0xfff;
    break;
  case 3:
    val= uint3korr(place);
    lim= 0xfffff;
    break;
  case 4:
    val= uint4korr(place);
    lim= 0xfffffff;
    break;
  case 5:
    val= uint5korr(place);
    lim= 0xfffffffffull;
    break;
  case 1:
  default:
    DBUG_ASSERT(0);                             /* impossible */
    return 1;
  }
  *type= (val & 0xf) + 1;
  *offset= (size_t)(val >> 4);
  return (*offset >= lim);
}

/**
  Format descriptor, contain constants and function references for
  format processing
*/

struct st_service_funcs
{
  /* size of fixed header */
  uint fixed_hdr;
  /* size of fixed part of header entry */
  uint fixed_hdr_entry;

  /*size of array element which stores keys */
  uint key_size_in_array;

  /* Maximum data offset size in bytes */
  size_t max_offset_size;

  size_t (*name_size)
    (void *, uint);
  int (*column_sort)
    (const void *a, const void *b);
  my_bool (*check_limit)
    (const void *val);
  void (*set_fixed_hdr)
    (DYNAMIC_COLUMN *str, DYN_HEADER *hdr);
  my_bool (*put_header_entry)(DYN_HEADER *hdr,
                              void *column_key,
                              DYNAMIC_COLUMN_VALUE *value,
                              size_t offset);
  int (*plan_sort)(const void *a, const void *b);
  size_t (*dynamic_column_offset_bytes)(size_t data_length);
  my_bool (*type_and_offset_read)(DYNAMIC_COLUMN_TYPE *type,
                                  size_t *offset,
                                  uchar *place, size_t offset_size);

};


/**
  Actual our 2 format descriptors
*/

static struct st_service_funcs fmt_data[2]=
{
  {
    FIXED_HEADER_SIZE,
    COLUMN_NUMBER_SIZE,
    sizeof(uint),
    MAX_OFFSET_LENGTH,
    &name_size_num,
    &column_sort_num,
    &check_limit_num,
    &set_fixed_header_num,
    &put_header_entry_num,
    &plan_sort_num,
    &dynamic_column_offset_bytes_num,
    &type_and_offset_read_num
  },
  {
    FIXED_HEADER_SIZE_NM,
    COLUMN_NAMEPTR_SIZE,
    sizeof(LEX_STRING),
    MAX_OFFSET_LENGTH_NM,
    &name_size_named,
    &column_sort_named,
    &check_limit_named,
    &set_fixed_header_named,
    &put_header_entry_named,
    &plan_sort_named,
    &dynamic_column_offset_bytes_named,
    &type_and_offset_read_named
  }
};


/**
  Read dynamic column record header and fill the descriptor

  @param hdr             dynamic columns record descriptor to fill
  @param str             dynamic columns record

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
init_read_hdr(DYN_HEADER *hdr, DYNAMIC_COLUMN *str)
{
  if (read_fixed_header(hdr, str))
    return ER_DYNCOL_FORMAT;
  hdr->header= (uchar*)str->str + fmt_data[hdr->format].fixed_hdr;
  calc_param(&hdr->entry_size, &hdr->header_size,
             fmt_data[hdr->format].fixed_hdr_entry, hdr->offset_size,
             hdr->column_count);
  hdr->nmpool= hdr->header + hdr->header_size;
  hdr->dtpool= hdr->nmpool + hdr->nmpool_size;
  hdr->data_size= str->length - fmt_data[hdr->format].fixed_hdr -
    hdr->header_size - hdr->nmpool_size;
  hdr->data_end= (uchar*)str->str + str->length;
  return ER_DYNCOL_OK;
}


/**
  Initialize dynamic column string with (make it empty but correct format)

  @param str             The string to initialize
  @param size            Amount of preallocated memory for the string.

  @retval FALSE OK
  @retval TRUE  error
*/

static my_bool dynamic_column_init_named(DYNAMIC_COLUMN *str, size_t size)
{
  DBUG_ASSERT(size != 0);

  /*
    Make string with no fields (empty header)
    - First \0 is flags
    - other 2 \0 is number of fields
  */
  if (ma_init_dynamic_string(str, NULL, size, DYNCOL_SYZERESERVE))
    return TRUE;
  return FALSE;
}


/**
  Calculate how many bytes needed to store val as variable length integer
  where first bit indicate continuation of the sequence.

  @param val             The value for which we are calculating length

  @return number of bytes
*/

static size_t dynamic_column_var_uint_bytes(ulonglong val)
{
  size_t len= 0;
  do
  {
    len++;
    val>>= 7;
  } while (val);
  return len;
}


/**
   Stores variable length unsigned integer value to a string

  @param str             The string where to append the value
  @param val             The value to put in the string

  @return ER_DYNCOL_* return code

  @notes
  This is used to store a number together with other data in the same
  object.  (Like decimals, length of string etc)
  (As we don't know the length of this object, we can't store 0 in 0 bytes)
*/

static enum enum_dyncol_func_result
dynamic_column_var_uint_store(DYNAMIC_COLUMN *str, ulonglong val)
{
  if (ma_dynstr_realloc(str, 10))                  /* max what we can use */
    return ER_DYNCOL_RESOURCE;

  do
  {
    ulonglong rest= val >> 7;
    str->str[str->length++]= ((val & 0x7f) | (rest ? 0x80 : 0x00));
    val= rest;
  } while (val);
  return ER_DYNCOL_OK;
}


/**
  Reads variable length unsigned integer value from a string

  @param data            The string from which the int should be read
  @param data_length	 Max length of data
  @param len             Where to put length of the string read in bytes

  @return value of the unsigned integer read from the string

  In case of error, *len is set to 0
*/

static ulonglong
dynamic_column_var_uint_get(uchar *data, size_t data_length,
                            size_t *len)
{
  ulonglong val= 0;
  uint length;
  uchar *end= data + data_length;

  for (length=0; data < end ; data++)
  {
    val+= (((ulonglong)((*data) & 0x7f)) << (length * 7));
    length++;
    if (!((*data) & 0x80))
    {
      /* End of data */
      *len= length;
      return val;
    }
  }
  /* Something was wrong with data */
  *len= 0;                                      /* Mark error */
  return 0;
}


/**
  Calculate how many bytes needed to store val as unsigned.

  @param val             The value for which we are calculating length

  @return number of bytes (0-8)
*/

static size_t dynamic_column_uint_bytes(ulonglong val)
{
  size_t len;

  for (len= 0; val ; val>>= 8, len++)
    ;
  return len;
}


/**
  Append the string with given unsigned int value.

  @param str             The string where to put the value
  @param val             The value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_uint_store(DYNAMIC_COLUMN *str, ulonglong val)
{
  if (ma_dynstr_realloc(str, 8)) /* max what we can use */
    return ER_DYNCOL_RESOURCE;

  for (; val; val>>= 8)
    str->str[str->length++]= (char) (val & 0xff);
  return ER_DYNCOL_OK;
}


/**
  Read unsigned int value of given length from the string

  @param store_it_here   The structure to store the value
  @param data            The string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_uint_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                         uchar *data, size_t length)
{
  ulonglong value= 0;
  size_t i;

  for (i= 0; i < length; i++)
    value+= ((ulonglong)data[i]) << (i*8);

  store_it_here->x.ulong_value= value;
  return ER_DYNCOL_OK;
}

/**
  Calculate how many bytes needed to store val as signed in following encoding:
    0 -> 0
   -1 -> 1
    1 -> 2
   -2 -> 3
    2 -> 4
   ...

  @param val             The value for which we are calculating length

  @return number of bytes
*/

static size_t dynamic_column_sint_bytes(longlong val)
{
  return dynamic_column_uint_bytes((val << 1) ^
                                   (val < 0 ? 0xffffffffffffffffull : 0));
}


/**
  Append the string with given signed int value.

  @param str             the string where to put the value
  @param val             the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_sint_store(DYNAMIC_COLUMN *str, longlong val)
{
  return dynamic_column_uint_store(str,
                                 (val << 1) ^
                                 (val < 0 ? 0xffffffffffffffffULL : 0));
}


/**
  Read signed int value of given length from the string

  @param store_it_here   The structure to store the value
  @param data            The string which should be read
  @param length          The length (in bytes) of the value in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_sint_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                         uchar *data, size_t length)
{
  ulonglong val;
  dynamic_column_uint_read(store_it_here, data, length);
  val= store_it_here->x.ulong_value;
  if (val & 1)
    val= (val >> 1) ^ 0xffffffffffffffffULL;
  else
    val>>= 1;
  store_it_here->x.long_value= (longlong) val;
  return ER_DYNCOL_OK;
}


/**
  Calculate how many bytes needed to store the value.

  @param value          The value for which we are calculating length

  @return
  Error:  (size_t) ~0
  ok      number of bytes
*/

static size_t
dynamic_column_value_len(DYNAMIC_COLUMN_VALUE *value,
                         enum enum_dyncol_format format)
{
  switch (value->type) {
  case DYN_COL_NULL:
    return 0;
  case DYN_COL_INT:
    return dynamic_column_sint_bytes(value->x.long_value);
  case DYN_COL_UINT:
    return dynamic_column_uint_bytes(value->x.ulong_value);
  case DYN_COL_DOUBLE:
    return 8;
  case DYN_COL_STRING:
#ifdef LIBMARIADB
    return (dynamic_column_var_uint_bytes(value->x.string.charset->nr) +
            value->x.string.value.length);
#else
    return (dynamic_column_var_uint_bytes(value->x.string.charset->number) +
            value->x.string.value.length);
#endif
#ifndef LIBMARIADB
  case DYN_COL_DECIMAL:
  {
    int precision= value->x.decimal.value.intg + value->x.decimal.value.frac;
    int scale= value->x.decimal.value.frac;

    if (precision == 0 || decimal_is_zero(&value->x.decimal.value))
    {
      /* This is here to simplify dynamic_column_decimal_store() */
      value->x.decimal.value.intg= value->x.decimal.value.frac= 0;
      return 0;
    }
    /*
      Check if legal decimal;  This is needed to not get an assert in
      decimal_bin_size(). However this should be impossible as all
      decimals entered here should be valid and we have the special check
      above to handle the unlikely but possible case that decimal.value.intg
      and decimal.frac is 0.
    */
    if (scale < 0 || precision <= 0)
    {
      DBUG_ASSERT(0);                           /* Impossible */
      return (size_t) ~0;
    }
    return (dynamic_column_var_uint_bytes(value->x.decimal.value.intg) +
            dynamic_column_var_uint_bytes(value->x.decimal.value.frac) +
            decimal_bin_size(precision, scale));
  }
#endif
  case DYN_COL_DATETIME:
    if (format == dyncol_fmt_num || value->x.time_value.second_part)
      /* date+time in bits: 14 + 4 + 5 + 10 + 6 + 6 + 20 + 1 66bits ~= 9 bytes*/
      return 9;
    else
      return 6;
  case DYN_COL_DATE:
    /* date in dits: 14 + 4 + 5 = 23bits ~= 3bytes*/
    return 3;
  case DYN_COL_TIME:
    if (format == dyncol_fmt_num || value->x.time_value.second_part)
      /* time in bits: 10 + 6 + 6 + 20 + 1 = 43bits ~= 6bytes*/
      return 6;
    else
      return 3;
  case DYN_COL_DYNCOL:
    return value->x.string.value.length;
  default:
    break;  
  }
  DBUG_ASSERT(0);
  return 0;
}


/**
  Append double value to a string

  @param str             the string where to put the value
  @param val             the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_double_store(DYNAMIC_COLUMN *str, double val)
{
   if (ma_dynstr_realloc(str, 8))
     return ER_DYNCOL_RESOURCE;
   float8store(str->str + str->length, val);
   str->length+= 8;
   return ER_DYNCOL_OK;
}


/**
  Read double value of given length from the string

  @param store_it_here   The structure to store the value
  @param data            The string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_double_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                               uchar *data, size_t length)
{
  if (length != 8)
    return ER_DYNCOL_FORMAT;
  float8get(store_it_here->x.double_value, data);
  return ER_DYNCOL_OK;
}


/**
  Append the string with given string value.

  @param str             the string where to put the value
  @param val             the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_string_store(DYNAMIC_COLUMN *str, LEX_STRING *string,
                            MARIADB_CHARSET_INFO *charset)
{
  enum enum_dyncol_func_result rc;
#ifdef LIBMARIADB
  if ((rc= dynamic_column_var_uint_store(str, charset->nr)))
#else
  if ((rc= dynamic_column_var_uint_store(str, charset->number)))
#endif
    return rc;
  if (ma_dynstr_append_mem(str, string->str, string->length))
    return ER_DYNCOL_RESOURCE;
  return ER_DYNCOL_OK;
}

/**
  Append the string with given string value.

  @param str             the string where to put the value
  @param val             the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_dyncol_store(DYNAMIC_COLUMN *str, LEX_STRING *string)
{
  if (ma_dynstr_append_mem(str, string->str, string->length))
    return ER_DYNCOL_RESOURCE;
  return ER_DYNCOL_OK;
}

/**
  Read string value of given length from the packed string

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_string_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                           uchar *data, size_t length)
{
  size_t len;
  uint charset_nr= (uint)dynamic_column_var_uint_get(data, length, &len);
  if (len == 0)                                /* Wrong packed number */
    return ER_DYNCOL_FORMAT;
#ifndef LIBMARIADB
  store_it_here->x.string.charset= get_charset_by_nr(charset_nr);
#else
  store_it_here->x.string.charset= mariadb_get_charset_by_nr(charset_nr);
#endif
  if (store_it_here->x.string.charset == NULL)
    return ER_DYNCOL_UNKNOWN_CHARSET;
  data+= len;
  store_it_here->x.string.value.length= (length-= len);
  store_it_here->x.string.value.str= (char*) data;
  return ER_DYNCOL_OK;
}

/**
  Read Dynamic columns packet string value of given length
  from the packed string

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_dyncol_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                           uchar *data, size_t length)
{
  store_it_here->x.string.charset= ma_charset_bin;
  store_it_here->x.string.value.length= length;
  store_it_here->x.string.value.str= (char*) data;
  return ER_DYNCOL_OK;
}

/**
  Append the string with given decimal value.

  @param str             the string where to put the value
  @param val             the value to put in the string

  @return ER_DYNCOL_* return code
*/
#ifndef LIBMARIADB
static enum enum_dyncol_func_result
dynamic_column_decimal_store(DYNAMIC_COLUMN *str,
                             decimal_t *value)
{
  uint bin_size;
  int precision= value->intg + value->frac;
  
  /* Store decimal zero as empty string */
  if (precision == 0)
    return ER_DYNCOL_OK;

  bin_size= decimal_bin_size(precision, value->frac);
  if (ma_dynstr_realloc(str, bin_size + 20))
    return ER_DYNCOL_RESOURCE;

  /* The following can't fail as memory is already allocated */
  (void) dynamic_column_var_uint_store(str, value->intg);
  (void) dynamic_column_var_uint_store(str, value->frac);

  decimal2bin(value, (uchar *) str->str + str->length,
              precision, value->frac);
  str->length+= bin_size;
  return ER_DYNCOL_OK;
}


/**
  Prepare the value to be used as decimal.

  @param value           The value structure which sould be setup.
*/

void mariadb_dyncol_prepare_decimal(DYNAMIC_COLUMN_VALUE *value)
{
  value->x.decimal.value.buf= value->x.decimal.buffer;
  value->x.decimal.value.len= DECIMAL_BUFF_LENGTH;
  /* just to be safe */
  value->type= DYN_COL_DECIMAL;
  decimal_make_zero(&value->x.decimal.value);
}

void dynamic_column_prepare_decimal(DYNAMIC_COLUMN_VALUE *value)
{
  mariadb_dyncol_prepare_decimal(value);
}



/**
  Read decimal value of given length from the string

  @param store_it_here   The structure to store the value
  @param data            The string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_decimal_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                            uchar *data, size_t length)
{
  size_t intg_len, frac_len;
  int intg, frac, precision, scale;

  dynamic_column_prepare_decimal(store_it_here);
  /* Decimals 0.0 is stored as a zero length string */
  if (length == 0)
    return ER_DYNCOL_OK;                        /* value contains zero */

  intg= (int)dynamic_column_var_uint_get(data, length, &intg_len);
  data+= intg_len;
  frac= (int)dynamic_column_var_uint_get(data, length - intg_len, &frac_len);
  data+= frac_len;

  /* Check the size of data is correct */
  precision= intg + frac;
  scale=     frac;
  if (scale < 0 || precision <= 0 || scale > precision ||
      (length - intg_len - frac_len) >
      (size_t) (DECIMAL_BUFF_LENGTH*sizeof(decimal_digit_t)) ||
      decimal_bin_size(intg + frac, frac) !=
      (int) (length - intg_len - frac_len))
    return ER_DYNCOL_FORMAT;

  if (bin2decimal(data, &store_it_here->x.decimal.value, precision, scale) !=
      E_DEC_OK)
    return ER_DYNCOL_FORMAT;
  return ER_DYNCOL_OK;
}
#endif

/**
  Append the string with given datetime value.

  @param str             the string where to put the value
  @param value           the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_date_time_store(DYNAMIC_COLUMN *str, MYSQL_TIME *value,
                               enum enum_dyncol_format format)
{
  enum enum_dyncol_func_result rc;
  /*
    0<----year----><mn><day>00000!<-hours--><min-><sec-><---microseconds--->
     12345678901234123412345     1123456789012345612345612345678901234567890
    <123456><123456><123456><123456><123456><123456><123456><123456><123456>
  */
  if ((rc= dynamic_column_date_store(str, value)) ||
      (rc= dynamic_column_time_store(str, value, format)))
    return rc;
  return ER_DYNCOL_OK;
}


/**
  Read datetime value of given length from the packed string

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_date_time_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                              uchar *data, size_t length)
{
  enum enum_dyncol_func_result rc= ER_DYNCOL_FORMAT;
  /*
    0<----year----><mn><day>00000!<-hours--><min-><sec-><---microseconds--->
     12345678901234123412345     1123456789012345612345612345678901234567890
    <123456><123456><123456><123456><123456><123456><123456><123456><123456>
  */
  if (length != 9 && length != 6)
    goto err;
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_DATETIME;
  if ((rc= dynamic_column_date_read_internal(store_it_here, data, 3)) ||
      (rc= dynamic_column_time_read_internal(store_it_here, data + 3,
                                             length - 3)))
    goto err;
  return ER_DYNCOL_OK;

err:
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_ERROR;
  return rc;
}


/**
  Append the string with given time value.

  @param str             the string where to put the value
  @param value           the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_time_store(DYNAMIC_COLUMN *str, MYSQL_TIME *value,
                          enum enum_dyncol_format format)
{
  uchar *buf;
  if (ma_dynstr_realloc(str, 6))
    return ER_DYNCOL_RESOURCE;

  buf= ((uchar *)str->str) + str->length;

  if (value->time_type == MYSQL_TIMESTAMP_NONE ||
      value->time_type == MYSQL_TIMESTAMP_ERROR ||
      value->time_type == MYSQL_TIMESTAMP_DATE)
  {
    value->neg= 0;
    value->second_part= 0;
    value->hour= 0;
    value->minute= 0;
    value->second= 0;
  }
  DBUG_ASSERT(value->hour <= 838);
  DBUG_ASSERT(value->minute <= 59);
  DBUG_ASSERT(value->second <= 59);
  DBUG_ASSERT(value->second_part <= 999999);
  if (format == dyncol_fmt_num || value->second_part)
  {
  /*
    00000!<-hours--><min-><sec-><---microseconds--->
         1123456789012345612345612345678901234567890
    <123456><123456><123456><123456><123456><123456>
  */
    buf[0]= (value->second_part & 0xff);
    buf[1]= ((value->second_part & 0xff00) >> 8);
    buf[2]= (uchar)(((value->second & 0xf) << 4) |
                    ((value->second_part & 0xf0000) >> 16));
    buf[3]= ((value->minute << 2) | ((value->second & 0x30) >> 4));
    buf[4]= (value->hour & 0xff);
    buf[5]= ((value->neg ? 0x4 : 0) | (value->hour >> 8));
    str->length+= 6;
  }
  else
  {
  /*
     !<-hours--><min-><sec->
     11234567890123456123456
    <123456><123456><123456>
  */
    buf[0]= (value->second) | ((value->minute & 0x3) << 6);
    buf[1]= (value->minute >> 2) | ((value->hour & 0xf) << 4);
    buf[2]= (value->hour >> 4) | (value->neg ? 0x80 : 0);
    str->length+= 3;
  }

  return ER_DYNCOL_OK;
}


/**
  Read time value of given length from the packed string

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_time_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                         uchar *data, size_t length)
{
  store_it_here->x.time_value.year= store_it_here->x.time_value.month=
    store_it_here->x.time_value.day= 0;
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_TIME;
  return dynamic_column_time_read_internal(store_it_here, data, length);
}

/**
  Internal function for reading time part from the string.

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_time_read_internal(DYNAMIC_COLUMN_VALUE *store_it_here,
                                  uchar *data, size_t length)
{
  if (length != 6 && length != 3)
    goto err;
  if (length == 6)
  {
    /*
      00000!<-hours--><min-><sec-><---microseconds--->
      1123456789012345612345612345678901234567890
      <123456><123456><123456><123456><123456><123456>
    */
    store_it_here->x.time_value.second_part= (data[0] |
                                              (data[1] << 8) |
                                              ((data[2] & 0xf) << 16));
    store_it_here->x.time_value.second= ((data[2] >> 4) |
                                         ((data[3] & 0x3) << 4));
    store_it_here->x.time_value.minute= (data[3] >> 2);
    store_it_here->x.time_value.hour= (((((uint)data[5]) & 0x3 ) << 8) | data[4]);
    store_it_here->x.time_value.neg= ((data[5] & 0x4) ? 1 : 0);
  }
  else
  {
    /*
     !<-hours--><min-><sec->
     11234567890123456123456
    <123456><123456><123456>
   */
    store_it_here->x.time_value.second_part= 0;
    store_it_here->x.time_value.second= (data[0] & 0x3f);
    store_it_here->x.time_value.minute= (data[0] >> 6) | ((data[1] & 0xf) << 2);
    store_it_here->x.time_value.hour= (data[1] >> 4) | ((data[2] & 0x3f) << 4);
    store_it_here->x.time_value.neg= ((data[2] & 0x80) ? 1 : 0);
  }
  if (store_it_here->x.time_value.second > 59 ||
      store_it_here->x.time_value.minute > 59 ||
      store_it_here->x.time_value.hour > 838 ||
      store_it_here->x.time_value.second_part > 999999)
    goto err;
  return ER_DYNCOL_OK;

err:
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_ERROR;
  return ER_DYNCOL_FORMAT;
}


/**
  Append the string with given date value.

  @param str             the string where to put the value
  @param value           the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_date_store(DYNAMIC_COLUMN *str, MYSQL_TIME *value)
{
  uchar *buf;
  if (ma_dynstr_realloc(str, 3))
    return ER_DYNCOL_RESOURCE;

  buf= ((uchar *)str->str) + str->length;
  if (value->time_type == MYSQL_TIMESTAMP_NONE ||
      value->time_type == MYSQL_TIMESTAMP_ERROR ||
      value->time_type == MYSQL_TIMESTAMP_TIME)
    value->year= value->month= value->day = 0;
  DBUG_ASSERT(value->year <= 9999);
  DBUG_ASSERT(value->month <= 12);
  DBUG_ASSERT(value->day <= 31);
  /*
    0<----year----><mn><day>
    012345678901234123412345
    <123456><123456><123456>
  */
  buf[0]= (value->day |
           ((value->month & 0x7) << 5));
  buf[1]= ((value->month >> 3) | ((value->year & 0x7F) << 1));
  buf[2]= (value->year >> 7);
  str->length+= 3;
  return ER_DYNCOL_OK;
}



/**
  Read date value of given length from the packed string

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_date_read(DYNAMIC_COLUMN_VALUE *store_it_here,
                         uchar *data, size_t length)
{
  store_it_here->x.time_value.neg= 0;
  store_it_here->x.time_value.second_part= 0;
  store_it_here->x.time_value.hour= 0;
  store_it_here->x.time_value.minute= 0;
  store_it_here->x.time_value.second= 0;
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_DATE;
  return dynamic_column_date_read_internal(store_it_here, data, length);
}

/**
  Internal function for reading date part from the string.

  @param store_it_here   The structure to store the value
  @param data            The packed string which should be read
  @param length          The length (in bytes) of the value in nthe string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_date_read_internal(DYNAMIC_COLUMN_VALUE *store_it_here,
                                  uchar *data,
                                  size_t length)
{
  if (length != 3)
    goto err;
  /*
    0<----year----><mn><day>
     12345678901234123412345
    <123456><123456><123456>
  */
  store_it_here->x.time_value.day= (data[0] & 0x1f);
  store_it_here->x.time_value.month= (((data[1] & 0x1) << 3) |
                                    (data[0] >> 5));
  store_it_here->x.time_value.year= ((((uint)data[2]) << 7) |
                                    (data[1] >> 1));
  if (store_it_here->x.time_value.day > 31 ||
      store_it_here->x.time_value.month > 12 ||
      store_it_here->x.time_value.year > 9999)
    goto err;
  return ER_DYNCOL_OK;

err:
  store_it_here->x.time_value.time_type= MYSQL_TIMESTAMP_ERROR;
  return ER_DYNCOL_FORMAT;
}


/**
  Append the string with given value.

  @param str             the string where to put the value
  @param value           the value to put in the string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
data_store(DYNAMIC_COLUMN *str, DYNAMIC_COLUMN_VALUE *value,
           enum enum_dyncol_format format)
{
  switch (value->type) {
  case DYN_COL_INT:
    return dynamic_column_sint_store(str, value->x.long_value);
  case DYN_COL_UINT:
    return dynamic_column_uint_store(str, value->x.ulong_value);
  case DYN_COL_DOUBLE:
    return dynamic_column_double_store(str, value->x.double_value);
  case DYN_COL_STRING:
    return dynamic_column_string_store(str, &value->x.string.value,
                                     value->x.string.charset);
#ifndef LIBMARIADB
  case DYN_COL_DECIMAL:
    return dynamic_column_decimal_store(str, &value->x.decimal.value);
#endif
  case DYN_COL_DATETIME:
    /* date+time in bits: 14 + 4 + 5 + 5 + 6 + 6 40bits = 5 bytes */
    return dynamic_column_date_time_store(str, &value->x.time_value, format);
  case DYN_COL_DATE:
    /* date in dits: 14 + 4 + 5 = 23bits ~= 3bytes*/
    return dynamic_column_date_store(str, &value->x.time_value);
  case DYN_COL_TIME:
    /* time in bits: 5 + 6 + 6 = 17bits ~= 3bytes*/
    return dynamic_column_time_store(str, &value->x.time_value, format);
  case DYN_COL_DYNCOL:
    return dynamic_column_dyncol_store(str, &value->x.string.value);
  case DYN_COL_NULL:
    break;                                      /* Impossible */
  default:
    break;
  }
  DBUG_ASSERT(0);
  return ER_DYNCOL_OK;                          /* Impossible */
}


/**
  Write information to the fixed header

  @param str             String where to write the header
  @param offset_size     Size of offset field in bytes
  @param column_count    Number of columns
*/

static void set_fixed_header(DYNAMIC_COLUMN *str,
                             uint offset_size,
                             uint column_count)
{
  DBUG_ASSERT(column_count <= 0xffff);
  DBUG_ASSERT(offset_size <= MAX_OFFSET_LENGTH);
  str->str[0]= ((str->str[0] & ~DYNCOL_FLG_OFFSET) |
                (offset_size - 1));             /* size of offset */
  int2store(str->str + 1, column_count);        /* columns number */
  DBUG_ASSERT((str->str[0] & (~DYNCOL_FLG_KNOWN)) == 0);
}

/**
  Adds columns into the empty string

  @param str             String where to write the data (the record)
  @param hdr             Dynamic columns record descriptor
  @param column_count    Number of columns in the arrays
  @param column_keys     Array of columns keys (uint or LEX_STRING)
  @param values          Array of columns values
  @param new_str         True if we need to allocate new string

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_new_column_store(DYNAMIC_COLUMN *str,
                         DYN_HEADER *hdr,
                         uint column_count,
                         void *column_keys,
                         DYNAMIC_COLUMN_VALUE *values,
                         my_bool new_str)
{
  struct st_service_funcs *fmt= fmt_data + hdr->format;
  void **columns_order;
  uchar *element;
  uint i;
  enum enum_dyncol_func_result rc= ER_DYNCOL_RESOURCE;
  size_t all_headers_size;

  if (!(columns_order= malloc(sizeof(void*)*column_count)))
    return ER_DYNCOL_RESOURCE;
  if (new_str || str->str == 0)
  {
    if (column_count)
    {
      if (dynamic_column_init_named(str,
                                    fmt->fixed_hdr +
                                    hdr->header_size +
                                    hdr->nmpool_size +
                                    hdr->data_size +
                                    DYNCOL_SYZERESERVE))
        goto err;
    }
    else
    {
      dynamic_column_initialize(str);
    }
  }
  else
  {
    str->length= 0;
    if (ma_dynstr_realloc(str,
                       fmt->fixed_hdr +
                       hdr->header_size +
                       hdr->nmpool_size +
                       hdr->data_size +
                       DYNCOL_SYZERESERVE))
      goto err;
  }
  if (!column_count)
    return ER_DYNCOL_OK;

  memset(str->str, 0, fmt->fixed_hdr);
  str->length= fmt->fixed_hdr;

  /* sort columns for the header */
  for (i= 0, element= (uchar *) column_keys;
       i < column_count;
       i++, element+= fmt->key_size_in_array)
    columns_order[i]= (void *)element;
  qsort(columns_order, (size_t)column_count, sizeof(void*), fmt->column_sort);

  /*
    For now we don't allow creating two columns with the same number
    at the time of create.  This can be fixed later to just use the later
    by comparing the pointers.
  */
  for (i= 0; i < column_count - 1; i++)
  {
    if ((*fmt->check_limit)(&columns_order[i]) ||
        (*fmt->column_sort)(&columns_order[i], &columns_order[i + 1]) == 0)
    {
      rc= ER_DYNCOL_DATA;
      goto err;
    }
  }
  if ((*fmt->check_limit)(&columns_order[i]))
  {
    rc= ER_DYNCOL_DATA;
    goto err;
  }

  (*fmt->set_fixed_hdr)(str, hdr);
  /* reserve place for header and name pool */
  str->length+= hdr->header_size + hdr->nmpool_size;

  hdr->entry= hdr->header;
  hdr->name= hdr->nmpool;
  all_headers_size= fmt->fixed_hdr + hdr->header_size + hdr->nmpool_size;
  for (i= 0; i < column_count; i++)
  {
    uint ord= (uint)(((uchar*)columns_order[i] - (uchar*)column_keys) /
                     fmt->key_size_in_array);
    if (values[ord].type != DYN_COL_NULL)
    {
      /* Store header first in the str */
      if ((*fmt->put_header_entry)(hdr, columns_order[i], values + ord,
                                   str->length - all_headers_size))
      {
        rc= ER_DYNCOL_FORMAT;
        goto err;
      }

      /* Store value in 'str + str->length' and increase str->length */
      if ((rc= data_store(str, values + ord, hdr->format)))
        goto err;
    }
  }
  rc= ER_DYNCOL_OK;
err:
  free(columns_order);
  return rc;
}

/**
  Calculate size of header, name pool and data pool

  @param hdr             descriptor of dynamic column record
  @param column_count    number of elements in arrays
  @param column_count    Number of columns in the arrays
  @param column_keys     Array of columns keys (uint or LEX_STRING)
  @param values          Array of columns values

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
calc_var_sizes(DYN_HEADER *hdr,
               uint column_count,
               void *column_keys,
               DYNAMIC_COLUMN_VALUE *values)
{
  struct st_service_funcs *fmt= fmt_data + hdr->format;
  uint i;
  hdr->nmpool_size= hdr->data_size= 0;
  hdr->column_count= 0;
  for (i= 0; i < column_count; i++)
  {
    if (values[i].type != DYN_COL_NULL)
    {
      size_t tmp;
      hdr->column_count++;
      hdr->data_size+= (tmp= dynamic_column_value_len(values + i,
                        hdr->format));
      if (tmp == (size_t) ~0)
        return ER_DYNCOL_DATA;
      hdr->nmpool_size+= (*fmt->name_size)(column_keys, i);
    }
  }
  /*
    We can handle data up to 0x1fffffff (old format) and
    0xfffffffff (new format) bytes now.
  */
  if ((hdr->offset_size= fmt->dynamic_column_offset_bytes(hdr->data_size)) >=
      fmt->max_offset_size)
    return ER_DYNCOL_LIMIT;

  /* header entry is column number or string pointer + offset & type */
  hdr->entry_size= fmt->fixed_hdr_entry + hdr->offset_size;
  hdr->header_size= hdr->column_count * hdr->entry_size;
  return ER_DYNCOL_OK;
}

/**
  Create packed string which contains given columns (internal multi format)

  @param str             String where to write the data
  @param column_count    Number of columns in the arrays
  @param column_keys     Array of columns keys (format dependent)
  @param values          Array of columns values
  @param new_str         True if we need allocate new string
  @param string_keys     keys are strings

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_create_many_internal_fmt(DYNAMIC_COLUMN *str,
                                        uint column_count,
                                        void *column_keys,
                                        DYNAMIC_COLUMN_VALUE *values,
                                        my_bool new_str,
                                        my_bool string_keys)
{
  DYN_HEADER header;
  enum enum_dyncol_func_result rc;
  memset(&header, 0, sizeof(header));
  header.format= (string_keys ? 1 : 0);

  if (new_str)
  {
    /* to make dynstr_free() working in case of errors */
    memset(str, 0, sizeof(DYNAMIC_COLUMN));
  }

  if ((rc= calc_var_sizes(&header, column_count, column_keys, values)) < 0)
    return rc;

  return dynamic_new_column_store(str, &header,
                                  column_count,
                                  column_keys, values,
                                  new_str);
}


/**
  Create packed string which contains given columns

  @param str             String where to write the data
  @param column_count    Number of columns in the arrays
  @param column_numbers  Array of columns numbers
  @param values          Array of columns values

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
dynamic_column_create_many(DYNAMIC_COLUMN *str,
                           uint column_count,
                           uint *column_numbers,
                           DYNAMIC_COLUMN_VALUE *values)
{
  return(dynamic_column_create_many_internal_fmt(str, column_count,
                                                      column_numbers, values,
                                                      TRUE, FALSE));
}

/**
  Create packed string which contains given columns

  @param str             String where to write the data
  @param column_count    Number of columns in the arrays
  @param column_numbers  Array of columns numbers
  @param values          Array of columns values
  @param new_string      True if we need allocate new string

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_create_many_num(DYNAMIC_COLUMN *str,
                               uint column_count,
                               uint *column_numbers,
                               DYNAMIC_COLUMN_VALUE *values,
                               my_bool new_string)
{
  return(dynamic_column_create_many_internal_fmt(str, column_count,
                                                      column_numbers, values,
                                                      new_string, FALSE));
}

/**
  Create packed string which contains given columns

  @param str             String where to write the data
  @param column_count    Number of columns in the arrays
  @param column_keys     Array of columns keys
  @param values          Array of columns value
  @param new_string      True if we need allocate new string

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_create_many_named(DYNAMIC_COLUMN *str,
                                 uint column_count,
                                 LEX_STRING *column_keys,
                                 DYNAMIC_COLUMN_VALUE *values,
                                 my_bool new_string)
{
  return(dynamic_column_create_many_internal_fmt(str, column_count,
                                                      column_keys, values,
                                                      new_string, TRUE));
}

/**
  Create packed string which contains given column

  @param str             String where to write the data
  @param column_number   Column number
  @param value           The columns value

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
dynamic_column_create(DYNAMIC_COLUMN *str, uint column_nr,
                      DYNAMIC_COLUMN_VALUE *value)
{
  return(dynamic_column_create_many(str, 1, &column_nr, value));
}


/**
  Calculate length of data between given two header entries

  @param entry           Pointer to the first entry
  @param entry_next      Pointer to the last entry
  @param header_end      Pointer to the header end
  @param offset_size     Size of offset field in bytes
  @param last_offset     Size of the data segment

  @return number of bytes
*/

static size_t get_length_interval(uchar *entry, uchar *entry_next,
                                  uchar *header_end, size_t offset_size,
                                  size_t last_offset)
{
  size_t offset, offset_next;
  DYNAMIC_COLUMN_TYPE type, type_next;
  DBUG_ASSERT(entry < entry_next);

  if (type_and_offset_read_num(&type, &offset, entry + COLUMN_NUMBER_SIZE,
                               offset_size))
      return DYNCOL_OFFSET_ERROR;
  if (entry_next >= header_end)
    return (last_offset - offset);
  if (type_and_offset_read_num(&type_next, &offset_next,
                               entry_next + COLUMN_NUMBER_SIZE, offset_size))
    return DYNCOL_OFFSET_ERROR;
  return (offset_next - offset);
}


/**
  Calculate length of data between given hdr->entry and next_entry

  @param hdr             descriptor of dynamic column record
  @param next_entry      next header entry (can point just after last header
                         entry)

  @return number of bytes
*/

static size_t hdr_interval_length(DYN_HEADER *hdr, uchar *next_entry)
{
  struct st_service_funcs *fmt= fmt_data + hdr->format;
  size_t next_entry_offset;
  DYNAMIC_COLUMN_TYPE next_entry_type;
  DBUG_ASSERT(hdr->entry < next_entry);
  DBUG_ASSERT(hdr->entry >= hdr->header);
  DBUG_ASSERT(next_entry <= hdr->header + hdr->header_size);

  if ((*fmt->type_and_offset_read)(&hdr->type, &hdr->offset,
                                   hdr->entry + fmt->fixed_hdr_entry,
                                   hdr->offset_size))
    return DYNCOL_OFFSET_ERROR;
  if (next_entry == hdr->header + hdr->header_size)
    return hdr->data_size - hdr->offset;
  if ((*fmt->type_and_offset_read)(&next_entry_type, &next_entry_offset,
                                   next_entry + fmt->fixed_hdr_entry,
                                   hdr->offset_size))
    return DYNCOL_OFFSET_ERROR;
  return (next_entry_offset - hdr->offset);
}


/**
  Comparator function for references to header entries for qsort
*/

static int header_compar_num(const void *a, const void *b)
{
  uint va= uint2korr((uchar*)a), vb= uint2korr((uchar*)b);
  return (va > vb ? 1 : (va < vb ? -1 : 0));
}


/**
  Find entry in the numeric format header by the column number

  @param hdr             descriptor of dynamic column record
  @param key             number to find

  @return pointer to the entry or NULL
*/

static uchar *find_entry_num(DYN_HEADER *hdr, uint key)
{
  uchar header_entry[2+4];
  DBUG_ASSERT(hdr->format == dyncol_fmt_num);
  int2store(header_entry, key);
  return hdr->entry= bsearch(header_entry, hdr->header,
                             (size_t)hdr->column_count,
                             hdr->entry_size, &header_compar_num);
}


/**
  Read name from header entry

  @param hdr             descriptor of dynamic column record
  @param entry           pointer to the header entry
  @param name            where to put name

  @return 0 ok
  @return 1 error in data
*/

static my_bool read_name(DYN_HEADER *hdr, uchar *entry, LEX_STRING *name)
{
  size_t nmoffset= uint2korr(entry);
  uchar *next_entry= entry + hdr->entry_size;

  if (nmoffset > hdr->nmpool_size)
    return 1;

  name->str= (char *)hdr->nmpool + nmoffset;
  if (next_entry == hdr->header + hdr->header_size)
    name->length= hdr->nmpool_size - nmoffset;
  else
  {
    size_t next_nmoffset= uint2korr(next_entry);
    if (next_nmoffset > hdr->nmpool_size)
      return 1;
    name->length= next_nmoffset - nmoffset;
  }
  return 0;
}


/**
  Find entry in the names format header by the column number

  @param hdr             descriptor of dynamic column record
  @param key             name to find

  @return pointer to the entry or NULL
*/
static uchar *find_entry_named(DYN_HEADER *hdr, LEX_STRING *key)
{
  uchar *min= hdr->header;
  uchar *max= hdr->header + (hdr->column_count - 1) * hdr->entry_size;
  uchar *mid;
  DBUG_ASSERT(hdr->format == dyncol_fmt_str);
  DBUG_ASSERT(hdr->nmpool != NULL);
  while (max >= min)
  {
    LEX_STRING name;
    int cmp;
    mid= hdr->header + ((min - hdr->header) +
                        (max - hdr->header)) /
      2 /
      hdr->entry_size * hdr->entry_size;
    if (read_name(hdr, mid, &name))
      return NULL;
    cmp= mariadb_dyncol_column_cmp_named(&name, key);
    if (cmp < 0)
      min= mid + hdr->entry_size;
    else if (cmp > 0)
      max= mid - hdr->entry_size;
    else
      return mid;
  }
  return NULL;
}


/**
  Write number in the buffer (backward direction - starts from the buffer end)

  @return pointer on the number begining
*/

static char *backwritenum(char *chr, uint numkey)
{
  if (numkey == 0)
    *(--chr)= '0';
  else
    while (numkey > 0)
    {
      *(--chr)= '0' + numkey % 10;
      numkey/= 10;
    }
  return chr;
}


/**
  Find column and fill information about it

  @param hdr             descriptor of dynamic column record
  @param numkey          Number of the column to fetch (if strkey is NULL)
  @param strkey          Name of the column to fetch (or NULL)

  @return 0 ok
  @return 1 error in data
*/

static my_bool
find_column(DYN_HEADER *hdr, uint numkey, LEX_STRING *strkey)
{
  LEX_STRING nmkey;
  char nmkeybuff[DYNCOL_NUM_CHAR]; /* to fit max 2 bytes number */
  DBUG_ASSERT(hdr->header != NULL);

  if (hdr->header + hdr->header_size > hdr->data_end)
    return TRUE;

  /* fix key */
  if (hdr->format == dyncol_fmt_num && strkey != NULL)
  {
    char *end;
    numkey= (uint) strtoul(strkey->str, &end, 10);
    if (end != strkey->str + strkey->length)
    {
      /* we can't find non-numeric key among numeric ones */
      hdr->type= DYN_COL_NULL;
      return 0;
    }
  }
  else if (hdr->format == dyncol_fmt_str && strkey == NULL)
  {
    nmkey.str= backwritenum(nmkeybuff + sizeof(nmkeybuff), numkey);
    nmkey.length= (nmkeybuff + sizeof(nmkeybuff)) - nmkey.str;
    strkey= &nmkey;
  }
  if (hdr->format == dyncol_fmt_num)
    hdr->entry= find_entry_num(hdr, numkey);
  else
    hdr->entry= find_entry_named(hdr, strkey);

  if (!hdr->entry)
  {
    /* Column not found */
    hdr->type= DYN_COL_NULL;
    return 0;
  }
  hdr->length= hdr_interval_length(hdr, hdr->entry + hdr->entry_size);
  hdr->data= hdr->dtpool + hdr->offset;
  /*
    Check that the found data is withing the ranges. This can happen if
    we get data with wrong offsets.
  */
  if (hdr->length == DYNCOL_OFFSET_ERROR ||
      hdr->length > INT_MAX || hdr->offset > hdr->data_size)
    return 1;

  return 0;
}


/**
  Read and check the header of the dynamic string

  @param hdr             descriptor of dynamic column record
  @param str             Dynamic string

  @retval FALSE OK
  @retval TRUE  error

  Note
    We don't check for str->length == 0 as all code that calls this
    already have handled this case.
*/

static inline my_bool read_fixed_header(DYN_HEADER *hdr,
                                        DYNAMIC_COLUMN *str)
{
  DBUG_ASSERT(str != NULL && str->length != 0);
  if ((str->length < 1)  ||
      (str->str[0] & (~DYNCOL_FLG_KNOWN)))
    return 1;
  hdr->format= ((str->str[0] & DYNCOL_FLG_NAMES) ?
                dyncol_fmt_str:
                dyncol_fmt_num);
  if ((str->length < fmt_data[hdr->format].fixed_hdr))
    return 1;                                   /* Wrong header */
  hdr->offset_size= (str->str[0] & DYNCOL_FLG_OFFSET) + 1 +
    (hdr->format == dyncol_fmt_str ? 1 : 0);
  hdr->column_count= uint2korr(str->str + 1);
  if (hdr->format == dyncol_fmt_str)
    hdr->nmpool_size= uint2korr(str->str + 3); // only 2 bytes supported for now
  else
    hdr->nmpool_size= 0;
  return 0;
}


/**
  Get dynamic column value by column number

  @param str             The packed string to extract the column
  @param column_nr       Number of column to fetch
  @param store_it_here   Where to store the extracted value

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
dynamic_column_get(DYNAMIC_COLUMN *str, uint column_nr,
                       DYNAMIC_COLUMN_VALUE *store_it_here)
{
  return dynamic_column_get_internal(str, store_it_here, column_nr, NULL);
}

enum enum_dyncol_func_result
mariadb_dyncol_get_num(DYNAMIC_COLUMN *str, uint column_nr,
                       DYNAMIC_COLUMN_VALUE *store_it_here)
{
  return dynamic_column_get_internal(str, store_it_here, column_nr, NULL);
}


/**
  Get dynamic column value by name

  @param str             The packed string to extract the column
  @param name            Name of column to fetch
  @param store_it_here   Where to store the extracted value

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_get_named(DYNAMIC_COLUMN *str, LEX_STRING *name,
                         DYNAMIC_COLUMN_VALUE *store_it_here)
{
  DBUG_ASSERT(name != NULL);
  return dynamic_column_get_internal(str, store_it_here, 0, name);
}


static enum enum_dyncol_func_result
dynamic_column_get_value(DYN_HEADER *hdr, DYNAMIC_COLUMN_VALUE *store_it_here)
{
  static enum enum_dyncol_func_result rc;
  switch ((store_it_here->type= hdr->type)) {
  case DYN_COL_INT:
    rc= dynamic_column_sint_read(store_it_here, hdr->data, hdr->length);
    break;
  case DYN_COL_UINT:
    rc= dynamic_column_uint_read(store_it_here, hdr->data, hdr->length);
    break;
  case DYN_COL_DOUBLE:
    rc= dynamic_column_double_read(store_it_here, hdr->data, hdr->length);
    break;
  case DYN_COL_STRING:
    rc= dynamic_column_string_read(store_it_here, hdr->data, hdr->length);
    break;
#ifndef LIBMARIADB
  case DYN_COL_DECIMAL:
    rc= dynamic_column_decimal_read(store_it_here, hdr->data, hdr->length);
    break;
#endif
  case DYN_COL_DATETIME:
    rc= dynamic_column_date_time_read(store_it_here, hdr->data,
                                      hdr->length);
    break;
  case DYN_COL_DATE:
    rc= dynamic_column_date_read(store_it_here, hdr->data, hdr->length);
    break;
  case DYN_COL_TIME:
    rc= dynamic_column_time_read(store_it_here, hdr->data, hdr->length);
    break;
  case DYN_COL_NULL:
    rc= ER_DYNCOL_OK;
    break;
  case DYN_COL_DYNCOL:
    rc= dynamic_column_dyncol_read(store_it_here, hdr->data, hdr->length);
    break;
  default:
    rc= ER_DYNCOL_FORMAT;
    store_it_here->type= DYN_COL_NULL;
    break;
  }
  return rc;
}

/**
  Get dynamic column value by number or name

  @param str             The packed string to extract the column
  @param store_it_here   Where to store the extracted value
  @param numkey          Number of the column to fetch (if strkey is NULL)
  @param strkey          Name of the column to fetch (or NULL)

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_get_internal(DYNAMIC_COLUMN *str,
                            DYNAMIC_COLUMN_VALUE *store_it_here,
                            uint num_key, LEX_STRING *str_key)
{
  DYN_HEADER header;
  enum enum_dyncol_func_result rc= ER_DYNCOL_FORMAT;
  memset(&header, 0, sizeof(header));

  if (str->length == 0)
    goto null;

  if ((rc= init_read_hdr(&header, str)) < 0)
    goto err;

  if (header.column_count == 0)
    goto null;

  if (find_column(&header, num_key, str_key))
    goto err;

  rc= dynamic_column_get_value(&header, store_it_here);
  return rc;

null:
    rc= ER_DYNCOL_OK;
err:
    store_it_here->type= DYN_COL_NULL;
    return rc;
}


/**
  Check existence of the column in the packed string (by number)

  @param str             The packed string to check the column
  @param column_nr       Number of column to check

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_exists_num(DYNAMIC_COLUMN *str, uint column_nr)
{
  return dynamic_column_exists_internal(str, column_nr, NULL);
}

/**
  Check existence of the column in the packed string (by name)

  @param str             The packed string to check the column
  @param name            Name of column to check

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_exists_named(DYNAMIC_COLUMN *str, LEX_STRING *name)
{
  DBUG_ASSERT(name != NULL);
  return dynamic_column_exists_internal(str, 0, name);
}


/**
  Check existence of the column in the packed string (by name of number)

  @param str             The packed string to check the column
  @param num_key         Number of the column to fetch (if strkey is NULL)
  @param str_key         Name of the column to fetch (or NULL)

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_exists_internal(DYNAMIC_COLUMN *str, uint num_key,
                               LEX_STRING *str_key)
{
  DYN_HEADER header;
  enum enum_dyncol_func_result rc;
  memset(&header, 0, sizeof(header));

  if (str->length == 0)
    return ER_DYNCOL_NO;                        /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;

  if (header.column_count == 0)
    return ER_DYNCOL_NO;                        /* no columns */

  if (find_column(&header, num_key, str_key))
    return ER_DYNCOL_FORMAT;

  return (header.type != DYN_COL_NULL ? ER_DYNCOL_YES : ER_DYNCOL_NO);
}


/**
  List not-null columns in the packed string (only numeric format)

  @param str             The packed string
  @param array_of_uint   Where to put reference on created array

  @return ER_DYNCOL_* return code
*/
enum enum_dyncol_func_result
dynamic_column_list(DYNAMIC_COLUMN *str, DYNAMIC_ARRAY *array_of_uint)
{
  DYN_HEADER header;
  uchar *read;
  uint i;
  enum enum_dyncol_func_result rc;

  memset(array_of_uint, 0, sizeof(*array_of_uint)); /* In case of errors */
  if (str->length == 0)
    return ER_DYNCOL_OK;                        /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;

  if (header.format != dyncol_fmt_num)
    return ER_DYNCOL_FORMAT;

  if (header.entry_size * header.column_count + FIXED_HEADER_SIZE >
      str->length)
    return ER_DYNCOL_FORMAT;

  if (ma_init_dynamic_array(array_of_uint, sizeof(uint), header.column_count, 0))
    return ER_DYNCOL_RESOURCE;

  for (i= 0, read= header.header;
       i < header.column_count;
       i++, read+= header.entry_size)
  {
    uint nm= uint2korr(read);
    /* Insert can't never fail as it's pre-allocated above */
    (void) ma_insert_dynamic(array_of_uint, (uchar *)&nm);
  }
  return ER_DYNCOL_OK;
}

/**
  List not-null columns in the packed string (only numeric format)

  @param str             The packed string
  @param array_of_uint   Where to put reference on created array

  @return ER_DYNCOL_* return code
*/
enum enum_dyncol_func_result
mariadb_dyncol_list_num(DYNAMIC_COLUMN *str, uint *count, uint **nums)
{
  DYN_HEADER header;
  uchar *read;
  uint i;
  enum enum_dyncol_func_result rc;

  (*nums)= 0; (*count)= 0;                      /* In case of errors */
  if (str->length == 0)
    return ER_DYNCOL_OK;                        /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;

  if (header.format != dyncol_fmt_num)
    return ER_DYNCOL_FORMAT;

  if (header.entry_size * header.column_count + FIXED_HEADER_SIZE >
      str->length)
    return ER_DYNCOL_FORMAT;

  if (!((*nums)= (uint *)malloc(sizeof(uint) * header.column_count)))
    return ER_DYNCOL_RESOURCE;

  for (i= 0, read= header.header;
       i < header.column_count;
       i++, read+= header.entry_size)
  {
    (*nums)[i]= uint2korr(read);
  }
  (*count)= header.column_count;
  return ER_DYNCOL_OK;
}

/**
  List not-null columns in the packed string (any format)

  @param str             The packed string
  @param count           Number of names in the list
  @param names           Where to put names list (should be freed)

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_list_named(DYNAMIC_COLUMN *str, uint *count, LEX_STRING **names)
{
  DYN_HEADER header;
  uchar *read;
  char *pool;
  struct st_service_funcs *fmt;
  uint i;
  enum enum_dyncol_func_result rc;

  (*names)= 0; (*count)= 0;

  if (str->length == 0)
    return ER_DYNCOL_OK;                        /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;

  fmt= fmt_data + header.format;

  if (header.entry_size * header.column_count + fmt->fixed_hdr >
      str->length)
    return ER_DYNCOL_FORMAT;

  if (header.format == dyncol_fmt_num)
    *names= (LEX_STRING *)malloc(sizeof(LEX_STRING) * header.column_count +
                      DYNCOL_NUM_CHAR * header.column_count);
  else
    *names= (LEX_STRING *)malloc(sizeof(LEX_STRING) * header.column_count +
                      header.nmpool_size + header.column_count);
  if (!(*names))
    return ER_DYNCOL_RESOURCE;
  pool= ((char *)(*names)) + sizeof(LEX_STRING) * header.column_count;

  for (i= 0, read= header.header;
       i < header.column_count;
       i++, read+= header.entry_size)
  {
    if (header.format == dyncol_fmt_num)
    {
      uint nm= uint2korr(read);
      (*names)[i].str= pool;
      pool+= DYNCOL_NUM_CHAR;
      (*names)[i].length=
        ma_ll2str(nm, (*names)[i].str, 10) - (*names)[i].str;
    }
    else
    {
      LEX_STRING tmp;
      if (read_name(&header, read, &tmp))
        return ER_DYNCOL_FORMAT;
      (*names)[i].length= tmp.length;
      (*names)[i].str= pool;
      pool+= tmp.length + 1;
      memcpy((*names)[i].str, (const void *)tmp.str, tmp.length);
      (*names)[i].str[tmp.length]= '\0'; // just for safety
    }
  }
  (*count)= header.column_count;
  return ER_DYNCOL_OK;
}

/**
  Find the place of the column in the header or place where it should be put

  @param hdr             descriptor of dynamic column record
  @param key             Name or number of column to fetch
                         (depends on string_key)
  @param string_key      True if we gave pointer to LEX_STRING.

  @retval TRUE found
  @retval FALSE pointer set to the next row
*/

static my_bool
find_place(DYN_HEADER *hdr, void *key, my_bool string_keys)
{
  uint mid, start, end, val;
  int UNINIT_VAR(flag);
  LEX_STRING str;
  char buff[DYNCOL_NUM_CHAR];
  my_bool need_conversion= ((string_keys ? dyncol_fmt_str : dyncol_fmt_num) !=
                            hdr->format);
  /* new format can't be numeric if the old one is names */
  DBUG_ASSERT(string_keys ||
              hdr->format == dyncol_fmt_num);

  start= 0;
  end= hdr->column_count -1;
  mid= 1;
  while (start != end)
  {
    uint val;
    mid= (start + end) / 2;
    hdr->entry= hdr->header + mid * hdr->entry_size;
    if (!string_keys)
    {
      val= uint2korr(hdr->entry);
      flag= CMP_NUM(*((uint *)key), val);
    }
    else
    {
      if (need_conversion)
      {
        str.str= backwritenum(buff + sizeof(buff), uint2korr(hdr->entry));
        str.length= (buff + sizeof(buff)) - str.str;
      }
      else
      {
        DBUG_ASSERT(hdr->format == dyncol_fmt_str);
        if (read_name(hdr, hdr->entry, &str))
          return 0;
      }
      flag= mariadb_dyncol_column_cmp_named((LEX_STRING *)key, &str);
    }
    if (flag <= 0)
      end= mid;
    else
      start= mid + 1;
  }
  hdr->entry= hdr->header + start * hdr->entry_size;
  if (start != mid)
  {
    if (!string_keys)
    {
      val= uint2korr(hdr->entry);
      flag= CMP_NUM(*((uint *)key), val);
    }
    else
    {
      if (need_conversion)
      {
        str.str= backwritenum(buff + sizeof(buff), uint2korr(hdr->entry));
        str.length= (buff + sizeof(buff)) - str.str;
      }
      else
      {
        DBUG_ASSERT(hdr->format == dyncol_fmt_str);
        if (read_name(hdr, hdr->entry, &str))
          return 0;
      }
      flag= mariadb_dyncol_column_cmp_named((LEX_STRING *)key, &str);
    }
  }
  if (flag > 0)
    hdr->entry+= hdr->entry_size; /* Point at next bigger key */
  return flag == 0;
}


/*
  It is internal structure which describes a plan of changing the record
  of dynamic columns
*/

typedef enum {PLAN_REPLACE, PLAN_ADD, PLAN_DELETE, PLAN_NOP} PLAN_ACT;

struct st_plan {
  DYNAMIC_COLUMN_VALUE *val;
  void *key;
  uchar *place;
  size_t length;
  long long hdelta, ddelta, ndelta;
  long long mv_offset, mv_length;
  uint mv_end;
  PLAN_ACT act;
};
typedef struct st_plan PLAN;


/**
  Sort function for plan by column number
*/

static int plan_sort_num(const void *a, const void *b)
{
  return *((uint *)((PLAN *)a)->key) - *((uint *)((PLAN *)b)->key);
}


/**
  Sort function for plan by column name
*/

static int plan_sort_named(const void *a, const void *b)
{
  return mariadb_dyncol_column_cmp_named((LEX_STRING *)((PLAN *)a)->key,
                                         (LEX_STRING *)((PLAN *)b)->key);
}

#define DELTA_CHECK(S, D, C)        \
  if ((S) == 0)                     \
    (S)= (D);                       \
  else if (((S) > 0 && (D) < 0) ||  \
            ((S) < 0 && (D) > 0))   \
  {                                 \
    (C)= TRUE;                      \
  }

/**
  Update dynamic column by copying in a new record (string).

  @param str             Dynamic column record to change
  @param plan            Plan of changing the record
  @param add_column_count number of records in the plan array.
  @param hdr             descriptor of old dynamic column record
  @param new_hdr         descriptor of new dynamic column record
  @param convert         need conversion from numeric to names format

  @return ER_DYNCOL_* return code
*/

static enum enum_dyncol_func_result
dynamic_column_update_copy(DYNAMIC_COLUMN *str, PLAN *plan,
                           uint add_column_count,
                           DYN_HEADER *hdr, DYN_HEADER *new_hdr,
                           my_bool convert)
{
  DYNAMIC_COLUMN tmp;
  struct st_service_funcs *fmt= fmt_data + hdr->format,
                          *new_fmt= fmt_data + new_hdr->format;
  uint i, j, k;
  size_t all_headers_size;

  if (dynamic_column_init_named(&tmp,
                              (new_fmt->fixed_hdr + new_hdr->header_size +
                               new_hdr->nmpool_size +
                               new_hdr->data_size + DYNCOL_SYZERESERVE)))
  {
    return ER_DYNCOL_RESOURCE;
  }
  memset(tmp.str, 0, new_fmt->fixed_hdr);
  (*new_fmt->set_fixed_hdr)(&tmp, new_hdr);
  /* Adjust tmp to contain whole the future header */
  tmp.length= new_fmt->fixed_hdr + new_hdr->header_size + new_hdr->nmpool_size;


  /*
    Copy data to the new string
    i= index in array of changes
    j= index in packed string header index
  */
  new_hdr->entry= new_hdr->header;
  new_hdr->name= new_hdr->nmpool;
  all_headers_size= new_fmt->fixed_hdr +
    new_hdr->header_size + new_hdr->nmpool_size;
  for (i= 0, j= 0; i < add_column_count || j < hdr->column_count; i++)
  {
    size_t UNINIT_VAR(first_offset);
    uint start= j, end;

    /*
      Search in i and j for the next column to add from i and where to
      add.
    */

    while (i < add_column_count && plan[i].act == PLAN_NOP)
      i++;                                    /* skip NOP */

    if (i == add_column_count)
      j= end= hdr->column_count;
    else
    {
      /*
        old data portion. We don't need to check that j < column_count
        as plan[i].place is guaranteed to have a pointer inside the
        data.
      */
      while (hdr->header + j * hdr->entry_size < plan[i].place)
        j++;
      end= j;
      if ((plan[i].act == PLAN_REPLACE || plan[i].act == PLAN_DELETE))
        j++;                              /* data at 'j' will be removed */
    }

    /*
      Adjust all headers since last loop.
      We have to do this as the offset for data has moved
    */
    for (k= start; k < end; k++)
    {
      uchar *read= hdr->header + k * hdr->entry_size;
      void *key;
      LEX_STRING name;
      size_t offs;
      uint nm;
      DYNAMIC_COLUMN_TYPE tp;
      char buff[DYNCOL_NUM_CHAR];

      if (hdr->format == dyncol_fmt_num)
      {
        if (convert)
        {
          name.str= backwritenum(buff + sizeof(buff), uint2korr(read));
          name.length= (buff + sizeof(buff)) - name.str;
          key= &name;
        }
        else
        {
          nm= uint2korr(read);                    /* Column nummber */
          key= &nm;
        }
      }
      else
      {
        if (read_name(hdr, read, &name))
          goto err;
        key= &name;
      }
      if (fmt->type_and_offset_read(&tp, &offs,
                                    read + fmt->fixed_hdr_entry,
                                    hdr->offset_size))
          goto err;
      if (k == start)
        first_offset= offs;
      else if (offs < first_offset)
        goto err;

      offs+= (size_t)plan[i].ddelta;
      {
        DYNAMIC_COLUMN_VALUE val;
        val.type= tp; // only the type used in the header
        if ((*new_fmt->put_header_entry)(new_hdr, key, &val, offs))
          goto err;
      }
    }

    /* copy first the data that was not replaced in original packed data */
    if (start < end)
    {
      size_t data_size;
      /* Add old data last in 'tmp' */
      hdr->entry= hdr->header + start * hdr->entry_size;
      data_size=
        hdr_interval_length(hdr, hdr->header + end * hdr->entry_size);
      if (data_size == DYNCOL_OFFSET_ERROR ||
          (long) data_size < 0 ||
          data_size > hdr->data_size - first_offset)
        goto err;

      memcpy(tmp.str + tmp.length, (char *)hdr->dtpool + first_offset,
             data_size);
      tmp.length+= data_size;
    }

    /* new data adding */
    if (i < add_column_count)
    {
      if( plan[i].act == PLAN_ADD || plan[i].act == PLAN_REPLACE)
      {
        if ((*new_fmt->put_header_entry)(new_hdr, plan[i].key,
                                         plan[i].val,
                                         tmp.length - all_headers_size))
          goto err;
        data_store(&tmp, plan[i].val, new_hdr->format); /* Append new data */
      }
    }
  }
  dynamic_column_column_free(str);
  *str= tmp;
  return ER_DYNCOL_OK;
err:
  dynamic_column_column_free(&tmp);
  return ER_DYNCOL_FORMAT;
}

static enum enum_dyncol_func_result
dynamic_column_update_move_left(DYNAMIC_COLUMN *str, PLAN *plan,
                                size_t offset_size,
                                size_t entry_size,
                                size_t header_size,
                                size_t new_offset_size,
                                size_t new_entry_size,
                                size_t new_header_size,
                                uint column_count,
                                uint new_column_count,
                                uint add_column_count,
                                uchar *header_end,
                                size_t max_offset)
{
  uchar *write;
  uchar *header_base= (uchar *)str->str + FIXED_HEADER_SIZE;
  uint i, j, k;
  size_t curr_offset;

  write= (uchar *)str->str + FIXED_HEADER_SIZE;
  set_fixed_header(str, (uint)new_offset_size, new_column_count);

  /*
    Move headers first.
    i= index in array of changes
    j= index in packed string header index
  */
  for (curr_offset= 0, i= 0, j= 0;
       i < add_column_count || j < column_count;
       i++)
  {
    size_t UNINIT_VAR(first_offset);
    uint start= j, end;

    /*
      Search in i and j for the next column to add from i and where to
      add.
    */

    while (i < add_column_count && plan[i].act == PLAN_NOP)
      i++;                                    /* skip NOP */

    if (i == add_column_count)
      j= end= column_count;
    else
    {
      /*
        old data portion. We don't need to check that j < column_count
        as plan[i].place is guaranteed to have a pointer inside the
        data.
      */
      while (header_base + j * entry_size < plan[i].place)
        j++;
      end= j;
      if ((plan[i].act == PLAN_REPLACE || plan[i].act == PLAN_DELETE))
        j++;                              /* data at 'j' will be removed */
    }
    plan[i].mv_end= end;

    {
      DYNAMIC_COLUMN_TYPE tp;
      if (type_and_offset_read_num(&tp, &first_offset,
                                   header_base + start * entry_size +
                                   COLUMN_NUMBER_SIZE, offset_size))
        return ER_DYNCOL_FORMAT;
    }
    /* find data to be moved */
    if (start < end)
    {
      size_t data_size=
        get_length_interval(header_base + start * entry_size,
                            header_base + end * entry_size,
                            header_end, offset_size, max_offset);
      if (data_size == DYNCOL_OFFSET_ERROR ||
          (long) data_size < 0 ||
          data_size > max_offset - first_offset)
      {
        str->length= 0; // just something valid
        return ER_DYNCOL_FORMAT;
      }
      DBUG_ASSERT(curr_offset == first_offset + plan[i].ddelta);
      plan[i].mv_offset= first_offset;
      plan[i].mv_length= data_size;
      curr_offset+= data_size;
    }
    else
    {
      plan[i].mv_length= 0;
      plan[i].mv_offset= curr_offset;
    }

    if (plan[i].ddelta == 0 && offset_size == new_offset_size &&
        plan[i].act != PLAN_DELETE)
      write+= entry_size * (end - start);
    else
    {
      /*
        Adjust all headers since last loop.
        We have to do this as the offset for data has moved
      */
      for (k= start; k < end; k++)
      {
        uchar *read= header_base + k * entry_size;
        size_t offs;
        uint nm;
        DYNAMIC_COLUMN_TYPE tp;

        nm= uint2korr(read);                    /* Column nummber */
        if (type_and_offset_read_num(&tp, &offs, read + COLUMN_NUMBER_SIZE,
                                     offset_size))
          return ER_DYNCOL_FORMAT;

        if (k > start && offs < first_offset)
        {
          str->length= 0; // just something valid
          return ER_DYNCOL_FORMAT;
        }

        offs+= (size_t)plan[i].ddelta;
        int2store(write, nm);
        /* write rest of data at write + COLUMN_NUMBER_SIZE */
        type_and_offset_store_num(write, new_offset_size, tp, offs);
        write+= new_entry_size;
      }
    }

    /* new data adding */
    if (i < add_column_count)
    {
      if( plan[i].act == PLAN_ADD || plan[i].act == PLAN_REPLACE)
      {
        int2store(write, *((uint *)plan[i].key));
        type_and_offset_store_num(write, new_offset_size,
                                  plan[i].val[0].type,
                                  curr_offset);
        write+= new_entry_size;
        curr_offset+= plan[i].length;
      }
    }
  }

  /*
    Move data.
    i= index in array of changes
    j= index in packed string header index
  */
  str->length= (FIXED_HEADER_SIZE + new_header_size);
  for (i= 0, j= 0;
       i < add_column_count || j < column_count;
       i++)
  {
    uint start= j, end;

    /*
      Search in i and j for the next column to add from i and where to
      add.
    */

    while (i < add_column_count && plan[i].act == PLAN_NOP)
      i++;                                    /* skip NOP */

    j= end= plan[i].mv_end;
    if (i != add_column_count &&
        (plan[i].act == PLAN_REPLACE || plan[i].act == PLAN_DELETE))
      j++;

    /* copy first the data that was not replaced in original packed data */
    if (start < end && plan[i].mv_length)
    {
      memmove((header_base + new_header_size +
               (size_t)plan[i].mv_offset + (size_t)plan[i].ddelta),
              header_base + header_size + (size_t)plan[i].mv_offset,
              (size_t)plan[i].mv_length);
    }
    str->length+= (size_t)plan[i].mv_length;

    /* new data adding */
    if (i < add_column_count)
    {
      if( plan[i].act == PLAN_ADD || plan[i].act == PLAN_REPLACE)
      {
        data_store(str, plan[i].val, dyncol_fmt_num);/* Append new data */
      }
    }
  }
  return ER_DYNCOL_OK;
}

#ifdef UNUSED
static enum enum_dyncol_func_result
dynamic_column_update_move_right(DYNAMIC_COLUMN *str, PLAN *plan,
                                 size_t offset_size,
                                 size_t entry_size,
                                 size_t header_size,
                                 size_t new_offset_size,
                                 size_t new_entry_size,
                                 size_t new_header_size,
                                 uint column_count,
                                 uint new_column_count,
                                 uint add_column_count,
                                 uchar *header_end,
                                 size_t max_offset)
{
  uchar *write;
  uchar *header_base= (uchar *)str->str + FIXED_HEADER_SIZE;
  uint i, j, k;
  size_t curr_offset;

  write= (uchar *)str->str + FIXED_HEADER_SIZE;
  set_fixed_header(str, new_offset_size, new_column_count);

  /*
    Move data first.
    i= index in array of changes
    j= index in packed string header index
  */
  for (curr_offset= 0, i= 0, j= 0;
       i < add_column_count || j < column_count;
       i++)
  {
    size_t UNINIT_VAR(first_offset);
    uint start= j, end;

    /*
      Search in i and j for the next column to add from i and where to
      add.
    */

    while (i < add_column_count && plan[i].act == PLAN_NOP)
      i++;                                    /* skip NOP */

    if (i == add_column_count)
      j= end= column_count;
    else
    {
      /*
        old data portion. We don't need to check that j < column_count
        as plan[i].place is guaranteed to have a pointer inside the
        data.
      */
      while (header_base + j * entry_size < plan[i].place)
        j++;
      end= j;
      if ((plan[i].act == PLAN_REPLACE || plan[i].act == PLAN_DELETE))
        j++;                              /* data at 'j' will be removed */
    }
    plan[i].mv_end= end;

    {
      DYNAMIC_COLUMN_TYPE tp;
      type_and_offset_read_num(&tp, &first_offset,
                               header_base +
                               start * entry_size + COLUMN_NUMBER_SIZE,
                               offset_size);
    }
    /* find data to be moved */
    if (start < end)
    {
      size_t data_size=
        get_length_interval(header_base + start * entry_size,
                            header_base + end * entry_size,
                            header_end, offset_size, max_offset);
      if (data_size == DYNCOL_OFFSET_ERROR ||
          (long) data_size < 0 ||
          data_size > max_offset - first_offset)
      {
        str->length= 0; // just something valid
        return ER_DYNCOL_FORMAT;
      }
      DBUG_ASSERT(curr_offset == first_offset + plan[i].ddelta);
      plan[i].mv_offset= first_offset;
      plan[i].mv_length= data_size;
      curr_offset+= data_size;
    }
    else
    {
      plan[i].mv_length= 0;
      plan[i].mv_offset= curr_offset;
    }

    if (plan[i].ddelta == 0 && offset_size == new_offset_size &&
        plan[i].act != PLAN_DELETE)
      write+= entry_size * (end - start);
    else
    {
      /*
        Adjust all headers since last loop.
        We have to do this as the offset for data has moved
      */
      for (k= start; k < end; k++)
      {
        uchar *read= header_base + k * entry_size;
        size_t offs;
        uint nm;
        DYNAMIC_COLUMN_TYPE tp;

        nm= uint2korr(read);                    /* Column nummber */
        type_and_offset_read_num(&tp, &offs, read + COLUMN_NUMBER_SIZE,
                                 offset_size);
        if (k > start && offs < first_offset)
        {
          str->length= 0; // just something valid
          return ER_DYNCOL_FORMAT;
        }

        offs+= plan[i].ddelta;
        int2store(write, nm);
        /* write rest of data at write + COLUMN_NUMBER_SIZE */
        if (type_and_offset_store_num(write, new_offset_size, tp, offs))
        {
          str->length= 0; // just something valid
          return ER_DYNCOL_FORMAT;
        }
        write+= new_entry_size;
      }
    }

    /* new data adding */
    if (i < add_column_count)
    {
      if( plan[i].act == PLAN_ADD || plan[i].act == PLAN_REPLACE)
      {
        int2store(write, *((uint *)plan[i].key));
        if (type_and_offset_store_num(write, new_offset_size,
                                      plan[i].val[0].type,
                                      curr_offset))
        {
          str->length= 0; // just something valid
          return ER_DYNCOL_FORMAT;
        }
        write+= new_entry_size;
        curr_offset+= plan[i].length;
      }
    }
  }

  /*
    Move headers.
    i= index in array of changes
    j= index in packed string header index
  */
  str->length= (FIXED_HEADER_SIZE + new_header_size);
  for (i= 0, j= 0;
       i < add_column_count || j < column_count;
       i++)
  {
    uint start= j, end;

    /*
      Search in i and j for the next column to add from i and where to
      add.
    */

    while (i < add_column_count && plan[i].act == PLAN_NOP)
      i++;                                    /* skip NOP */

    j= end= plan[i].mv_end;
    if (i != add_column_count &&
        (plan[i].act == PLAN_REPLACE || plan[i].act == PLAN_DELETE))
      j++;

    /* copy first the data that was not replaced in original packed data */
    if (start < end && plan[i].mv_length)
    {
      memmove((header_base + new_header_size +
               plan[i].mv_offset + plan[i].ddelta),
              header_base + header_size + plan[i].mv_offset,
              plan[i].mv_length);
    }
    str->length+= plan[i].mv_length;

    /* new data adding */
    if (i < add_column_count)
    {
      if( plan[i].act == PLAN_ADD || plan[i].act == PLAN_REPLACE)
      {
        data_store(str, plan[i].val, dyncol_fmt_num); /* Append new data */
      }
    }
  }
  return ER_DYNCOL_OK;
}
#endif

/**
  Update the packed string with the given columns

  @param str             String where to write the data
  @param add_column_count Number of columns in the arrays
  @param column_numbers  Array of columns numbers
  @param values          Array of columns values

  @return ER_DYNCOL_* return code
*/
/* plan allocated on the stack */
#define IN_PLACE_PLAN 4

enum enum_dyncol_func_result
dynamic_column_update_many(DYNAMIC_COLUMN *str,
                           uint add_column_count,
                           uint *column_numbers,
                           DYNAMIC_COLUMN_VALUE *values)
{
  return dynamic_column_update_many_fmt(str, add_column_count, column_numbers,
                                        values, FALSE);
}

enum enum_dyncol_func_result
mariadb_dyncol_update_many_num(DYNAMIC_COLUMN *str,
                               uint add_column_count,
                               uint *column_numbers,
                               DYNAMIC_COLUMN_VALUE *values)
{
  return dynamic_column_update_many_fmt(str, add_column_count, column_numbers,
                                        values, FALSE);
}

enum enum_dyncol_func_result
mariadb_dyncol_update_many_named(DYNAMIC_COLUMN *str,
                                 uint add_column_count,
                                 LEX_STRING *column_names,
                                 DYNAMIC_COLUMN_VALUE *values)
{
  return dynamic_column_update_many_fmt(str, add_column_count, column_names,
                                        values, TRUE);
}

static uint numlen(uint val)
{
  uint res;
  if (val == 0)
    return 1;
  res= 0;
  while(val)
  {
    res++;
    val/=10;
  }
  return res;
}

static enum enum_dyncol_func_result
dynamic_column_update_many_fmt(DYNAMIC_COLUMN *str,
                               uint add_column_count,
                               void *column_keys,
                               DYNAMIC_COLUMN_VALUE *values,
                               my_bool string_keys)
{
  PLAN *plan, *alloc_plan= NULL, in_place_plan[IN_PLACE_PLAN];
  uchar *element;
  DYN_HEADER header, new_header;
  struct st_service_funcs *fmt, *new_fmt;
  long long data_delta= 0, name_delta= 0;
  uint i;
  uint not_null;
  long long header_delta= 0;
  long long header_delta_sign, data_delta_sign;
  int copy= FALSE;
  enum enum_dyncol_func_result rc;
  my_bool convert;

  if (add_column_count == 0)
    return ER_DYNCOL_OK;

  memset(&header, 0, sizeof(header));
  memset(&new_header, 0, sizeof(new_header));
  new_header.format= (string_keys ? dyncol_fmt_str : dyncol_fmt_num);
  new_fmt= fmt_data + new_header.format;

  /*
    Get columns in column order. As the data in 'str' is already
    in column order this allows to replace all columns in one loop.
  */
  if (IN_PLACE_PLAN > add_column_count)
    plan= in_place_plan;
  else if (!(alloc_plan= plan=
             (PLAN *)malloc(sizeof(PLAN) * (add_column_count + 1))))
    return ER_DYNCOL_RESOURCE;

  not_null= add_column_count;
  for (i= 0, element= (uchar *) column_keys;
       i < add_column_count;
       i++, element+= new_fmt->key_size_in_array)
  {
    if ((*new_fmt->check_limit)(&element))
    {
      rc= ER_DYNCOL_DATA;
      goto end;
    }

    plan[i].val= values + i;
    plan[i].key= element;
    if (values[i].type == DYN_COL_NULL)
      not_null--;

  }

  if (str->length == 0)
  {
    /*
      Just add new columns. If there was no columns to add we return
      an empty string.
     */
    goto create_new_string;
  }

  /* Check that header is ok */
  if ((rc= init_read_hdr(&header, str)) < 0)
    goto end;
  fmt= fmt_data + header.format;
  /* new format can't be numeric if the old one is names */
  DBUG_ASSERT(new_header.format == dyncol_fmt_str ||
              header.format == dyncol_fmt_num);
  if (header.column_count == 0)
    goto create_new_string;

  qsort(plan, (size_t)add_column_count, sizeof(PLAN), new_fmt->plan_sort);

  new_header.column_count= header.column_count;
  new_header.nmpool_size= header.nmpool_size;
  if ((convert= (new_header.format == dyncol_fmt_str &&
                 header.format == dyncol_fmt_num)))
  {
    DBUG_ASSERT(new_header.nmpool_size == 0);
    for(i= 0, header.entry= header.header;
        i < header.column_count;
        i++, header.entry+= header.entry_size)
    {
      new_header.nmpool_size+= numlen(uint2korr(header.entry));
    }
  }

  if (fmt->fixed_hdr + header.header_size + header.nmpool_size > str->length)
  {
    rc= ER_DYNCOL_FORMAT;
    goto end;
  }

  /*
    Calculate how many columns and data is added/deleted and make a 'plan'
    for each of them.
  */
  for (i= 0; i < add_column_count; i++)
  {
    /*
      For now we don't allow creating two columns with the same number
      at the time of create.  This can be fixed later to just use the later
      by comparing the pointers.
    */
    if (i < add_column_count - 1 &&
        new_fmt->column_sort(&plan[i].key, &plan[i + 1].key) == 0)
    {
      rc= ER_DYNCOL_DATA;
      goto end;
    }

    /* Set common variables for all plans */
    plan[i].ddelta= data_delta;
    plan[i].ndelta= name_delta;
    /* get header delta in entries */
    plan[i].hdelta= header_delta;
    plan[i].length= 0;                          /* Length if NULL */

    if (find_place(&header, plan[i].key, string_keys))
    {
      size_t entry_data_size, entry_name_size= 0;

      /* Data existed; We have to replace or delete it */

      entry_data_size= hdr_interval_length(&header, header.entry +
                                           header.entry_size);
      if (entry_data_size == DYNCOL_OFFSET_ERROR ||
          (long) entry_data_size < 0)
      {
        rc= ER_DYNCOL_FORMAT;
        goto end;
      }

      if (new_header.format == dyncol_fmt_str)
      {
        if (header.format == dyncol_fmt_str)
        {
          LEX_STRING name;
          if (read_name(&header, header.entry, &name))
          {
            rc= ER_DYNCOL_FORMAT;
            goto end;
          }
          entry_name_size= name.length;
        }
        else
          entry_name_size= numlen(uint2korr(header.entry));
      }

      if (plan[i].val->type == DYN_COL_NULL)
      {
        /* Inserting a NULL means delete the old data */

        plan[i].act= PLAN_DELETE;	        /* Remove old value */
        header_delta--;                         /* One row less in header */
        data_delta-= entry_data_size;           /* Less data to store */
        name_delta-= entry_name_size;
      }
      else
      {
        /* Replace the value */

        plan[i].act= PLAN_REPLACE;
        /* get data delta in bytes */
        if ((plan[i].length= dynamic_column_value_len(plan[i].val,
                                                      new_header.format)) ==
            (size_t) ~0)
        {
          rc= ER_DYNCOL_DATA;
          goto end;
        }
        data_delta+= plan[i].length - entry_data_size;
        if (new_header.format == dyncol_fmt_str)
        {
          name_delta+= ((LEX_STRING *)(plan[i].key))->length - entry_name_size;
        }
      }
    }
    else
    {
      /* Data did not exists. Add if it it's not NULL */

      if (plan[i].val->type == DYN_COL_NULL)
      {
        plan[i].act= PLAN_NOP;                  /* Mark entry to be skiped */
      }
      else
      {
        /* Add new value */

        plan[i].act= PLAN_ADD;
        header_delta++;                         /* One more row in header */
        /* get data delta in bytes */
        if ((plan[i].length= dynamic_column_value_len(plan[i].val,
                                                      new_header.format)) ==
            (size_t) ~0)
        {
          rc= ER_DYNCOL_DATA;
          goto end;
        }
        data_delta+= plan[i].length;
        if (new_header.format == dyncol_fmt_str)
          name_delta+= ((LEX_STRING *)plan[i].key)->length;
      }
    }
    plan[i].place= header.entry;
  }
  plan[add_column_count].hdelta= header_delta;
  plan[add_column_count].ddelta= data_delta;
  plan[add_column_count].act= PLAN_NOP;
  plan[add_column_count].place= header.dtpool;

  new_header.column_count= (uint)(header.column_count + header_delta);

  /*
    Check if it is only "increasing" or only "decreasing" plan for (header
    and data separately).
  */
  new_header.data_size= header.data_size + (size_t)data_delta;
  new_header.nmpool_size= new_header.nmpool_size + (size_t)name_delta;
  DBUG_ASSERT(new_header.format != dyncol_fmt_num ||
              new_header.nmpool_size == 0);
  if ((new_header.offset_size=
       new_fmt->dynamic_column_offset_bytes(new_header.data_size)) >=
      new_fmt->max_offset_size)
  {
    rc= ER_DYNCOL_LIMIT;
    goto end;
  }

  copy= ((header.format != new_header.format) ||
         (new_header.format == dyncol_fmt_str));
  /* if (new_header.offset_size!=offset_size) then we have to rewrite header */
  header_delta_sign=
    ((int)new_header.offset_size + new_fmt->fixed_hdr_entry) -
    ((int)header.offset_size + fmt->fixed_hdr_entry);
  data_delta_sign= 0;
  // plan[add_column_count] contains last deltas.
  for (i= 0; i <= add_column_count && !copy; i++)
  {
    /* This is the check for increasing/decreasing */
    DELTA_CHECK(header_delta_sign, plan[i].hdelta, copy);
    DELTA_CHECK(data_delta_sign, plan[i].ddelta, copy);
  }
  calc_param(&new_header.entry_size, &new_header.header_size,
             new_fmt->fixed_hdr_entry,
             new_header.offset_size, new_header.column_count);

  /*
    Need copy because:
    1, Header/data parts moved in different directions.
    2. There is no enough allocated space in the string.
    3. Header and data moved in different directions.
  */
  if (copy || /*1.*/
      str->max_length < str->length + header_delta + data_delta || /*2.*/
      ((header_delta_sign < 0 && data_delta_sign > 0) ||
       (header_delta_sign > 0 && data_delta_sign < 0))) /*3.*/
    rc= dynamic_column_update_copy(str, plan, add_column_count,
                                   &header, &new_header,
                                   convert);
  else
    if (header_delta_sign < 0)
      rc= dynamic_column_update_move_left(str, plan, header.offset_size,
                                          header.entry_size,
                                          header.header_size,
                                          new_header.offset_size,
                                          new_header.entry_size,
                                          new_header.header_size,
                                          header.column_count,
                                          new_header.column_count,
                                          add_column_count, header.dtpool,
                                          header.data_size);
    else
      /*
      rc= dynamic_column_update_move_right(str, plan, offset_size,
                                           entry_size,  header_size,
                                           new_header.offset_size,
                                           new_header.entry_size,
                                           new_heder.header_size, column_count,
                                           new_header.column_count,
                                           add_column_count, header_end,
                                           header.data_size);
                                         */
      rc= dynamic_column_update_copy(str, plan, add_column_count,
                                     &header, &new_header,
                                     convert);
end:
  free(alloc_plan);
  return rc;

create_new_string:
  /* There is no columns from before, so let's just add the new ones */
  rc= ER_DYNCOL_OK;
  free(alloc_plan);
  if (not_null != 0)
    rc= dynamic_column_create_many_internal_fmt(str, add_column_count,
                                                (uint*)column_keys, values,
                                                str->str == NULL,
                                                string_keys);
  goto end;
}


/**
  Update the packed string with the given column

  @param str             String where to write the data
  @param column_number   Array of columns number
  @param values          Array of columns values

  @return ER_DYNCOL_* return code
*/


int dynamic_column_update(DYNAMIC_COLUMN *str, uint column_nr,
                          DYNAMIC_COLUMN_VALUE *value)
{
  return dynamic_column_update_many(str, 1, &column_nr, value);
}


enum enum_dyncol_func_result
mariadb_dyncol_check(DYNAMIC_COLUMN *str)
{
  struct st_service_funcs *fmt;
  enum enum_dyncol_func_result rc= ER_DYNCOL_FORMAT;
  DYN_HEADER header;
  uint i;
  size_t data_offset= 0, name_offset= 0;
  size_t prev_data_offset= 0, prev_name_offset= 0;
  LEX_STRING name= {0,0}, prev_name= {0,0};
  uint num= 0, prev_num= 0;
  void *key, *prev_key;
  enum enum_dynamic_column_type type= DYN_COL_NULL, prev_type= DYN_COL_NULL;

  if (str->length == 0)
  {
    return(ER_DYNCOL_OK);
  }

  memset(&header, 0, sizeof(header));

  /* Check that header is OK */
  if (read_fixed_header(&header, str))
  {
    goto end;
  }
  fmt= fmt_data + header.format;
  calc_param(&header.entry_size, &header.header_size,
             fmt->fixed_hdr_entry, header.offset_size,
             header.column_count);
  /* headers are out of string length (no space for data and part of headers) */
  if (fmt->fixed_hdr + header.header_size + header.nmpool_size > str->length)
  {
    goto end;
  }
  header.header= (uchar*)str->str + fmt->fixed_hdr;
  header.nmpool= header.header + header.header_size;
  header.dtpool= header.nmpool + header.nmpool_size;
  header.data_size= str->length - fmt->fixed_hdr -
    header.header_size - header.nmpool_size;

  /* read and check headers */
  if (header.format == dyncol_fmt_num)
  {
    key= &num;
    prev_key= &prev_num;
  }
  else
  {
    key= &name;
    prev_key= &prev_name;
  }
  for (i= 0, header.entry= header.header;
       i < header.column_count;
       i++, header.entry+= header.entry_size)
  {

    if (header.format == dyncol_fmt_num)
    {
       num= uint2korr(header.entry);
    }
    else
    {
      DBUG_ASSERT(header.format == dyncol_fmt_str);
      if (read_name(&header, header.entry, &name))
      {
        goto end;
      }
      name_offset= name.str - (char *)header.nmpool;
    }
    if ((*fmt->type_and_offset_read)(&type, &data_offset,
                                     header.entry + fmt->fixed_hdr_entry,
                                     header.offset_size))
      goto end;

    DBUG_ASSERT(type != DYN_COL_NULL);
    if (data_offset > header.data_size)
    {
      goto end;
    }
    if (prev_type != DYN_COL_NULL)
    {
      /* It is not first entry */
      if (prev_data_offset >= data_offset)
      {
        goto end;
      }
      if (prev_name_offset > name_offset)
      {
        goto end;
      }
      if ((*fmt->column_sort)(&prev_key, &key) >= 0)
      {
        goto end;
      }
    }
    prev_num= num;
    prev_name= name;
    prev_data_offset= data_offset;
    prev_name_offset= name_offset;
    prev_type= type;
  }

  /* check data, which we can */
  for (i= 0, header.entry= header.header;
       i < header.column_count;
       i++, header.entry+= header.entry_size)
  {
    DYNAMIC_COLUMN_VALUE store;
    // already checked by previouse pass
    (*fmt->type_and_offset_read)(&header.type, &header.offset,
                                 header.entry + fmt->fixed_hdr_entry,
                                 header.offset_size);
    header.length=
      hdr_interval_length(&header, header.entry + header.entry_size);
    header.data= header.dtpool + header.offset;
    switch ((header.type)) {
    case DYN_COL_INT:
      rc= dynamic_column_sint_read(&store, header.data, header.length);
      break;
    case DYN_COL_UINT:
      rc= dynamic_column_uint_read(&store, header.data, header.length);
      break;
    case DYN_COL_DOUBLE:
      rc= dynamic_column_double_read(&store, header.data, header.length);
      break;
    case DYN_COL_STRING:
      rc= dynamic_column_string_read(&store, header.data, header.length);
      break;
#ifndef LIBMARIADB
    case DYN_COL_DECIMAL:
      rc= dynamic_column_decimal_read(&store, header.data, header.length);
      break;
#endif
    case DYN_COL_DATETIME:
      rc= dynamic_column_date_time_read(&store, header.data,
                                        header.length);
      break;
    case DYN_COL_DATE:
      rc= dynamic_column_date_read(&store, header.data, header.length);
      break;
    case DYN_COL_TIME:
      rc= dynamic_column_time_read(&store, header.data, header.length);
      break;
    case DYN_COL_DYNCOL:
      rc= dynamic_column_dyncol_read(&store, header.data, header.length);
      break;
    case DYN_COL_NULL:
    default:
      rc= ER_DYNCOL_FORMAT;
      goto end;
    }
    if (rc != ER_DYNCOL_OK)
    {
      DBUG_ASSERT(rc < 0);
      goto end;
    }
  }

  rc= ER_DYNCOL_OK;
end:
  return(rc);
}

enum enum_dyncol_func_result
mariadb_dyncol_val_str(DYNAMIC_STRING *str, DYNAMIC_COLUMN_VALUE *val,
                       MARIADB_CHARSET_INFO *cs, char quote)
{
  char buff[40];
  size_t len;
  switch (val->type) {
    case DYN_COL_INT:
      len= snprintf(buff, sizeof(buff), "%lld", val->x.long_value);
      if (ma_dynstr_append_mem(str, buff, len))
        return ER_DYNCOL_RESOURCE;
      break;
    case DYN_COL_UINT:
      len= snprintf(buff, sizeof(buff), "%llu", val->x.ulong_value);
      if (ma_dynstr_append_mem(str, buff, len))
        return ER_DYNCOL_RESOURCE;
      break;
    case DYN_COL_DOUBLE:
      len= snprintf(buff, sizeof(buff), "%g", val->x.double_value);
      if (ma_dynstr_realloc(str, len + (quote ? 2 : 0)))
        return ER_DYNCOL_RESOURCE;
      if (quote)
        str->str[str->length++]= quote;
      ma_dynstr_append_mem(str, buff, len);
      if (quote)
        str->str[str->length++]= quote;
      break;
    case DYN_COL_DYNCOL:
    case DYN_COL_STRING:
      {
        char *alloc= NULL;
        char *from= val->x.string.value.str;
        ulong bufflen;
        my_bool conv= ((val->x.string.charset == cs) || 
                       !strcmp(val->x.string.charset->name, cs->name));
        my_bool rc;
        len= val->x.string.value.length;
        bufflen= (ulong)(len * (conv ? cs->char_maxlen : 1));
        if (ma_dynstr_realloc(str, bufflen))
            return ER_DYNCOL_RESOURCE;

        // guaranty UTF-8 string for value
        if (!conv)
        {
#ifndef LIBMARIADB
          uint dumma_errors;
#else
          int dumma_errors;
#endif
          if (!quote)
          {
            /* convert to the destination */
            str->length+=
#ifndef LIBMARIADB
              copy_and_convert_extended(str->str, bufflen,
                                                    cs,
                                                    from, (uint32)len,
                                                    val->x.string.charset,
                                                    &dumma_errors);
#else
              mariadb_convert_string(from, &len, val->x.string.charset,
                                     str->str, (size_t *)&bufflen, cs, &dumma_errors);
#endif
            return ER_DYNCOL_OK;
          }
          if ((alloc= (char *)malloc(bufflen)))
          {
            len=
#ifndef LIBMARIADB
              copy_and_convert_extended(alloc, bufflen, cs,
                                        from, (uint32)len,
                                        val->x.string.charset,
                                        &dumma_errors);
#else
              mariadb_convert_string(from, &len, val->x.string.charset,
                                     alloc, (size_t *)&bufflen, cs, &dumma_errors);
#endif
            from= alloc;
          }
          else
            return ER_DYNCOL_RESOURCE;
        }
        if (quote)
          rc= ma_dynstr_append_mem(str, &quote, 1);
        rc= ma_dynstr_append_mem(str, from, len);
        if (quote)
          rc= ma_dynstr_append_mem(str, &quote, 1);
        if (alloc)
          free(alloc);
        if (rc)
          return ER_DYNCOL_RESOURCE;
        break;
      }
#ifndef LIBMARIADB
    case DYN_COL_DECIMAL:
      {
        int len= sizeof(buff);
        decimal2string(&val->x.decimal.value, buff, &len,
                       0, val->x.decimal.value.frac,
                       '0');
        if (ma_dynstr_append_mem(str, buff, len))
          return ER_DYNCOL_RESOURCE;
        break;
      }
#endif
    case DYN_COL_DATETIME:
    case DYN_COL_DATE:
    case DYN_COL_TIME:
#ifndef LIBMARIADB
      len= my_TIME_to_str(&val->x.time_value, buff, AUTO_SEC_PART_DIGITS);
#else
      len= mariadb_time_to_string(&val->x.time_value, buff, 39, AUTO_SEC_PART_DIGITS);
#endif
      if (ma_dynstr_realloc(str, len + (quote ? 2 : 0)))
        return ER_DYNCOL_RESOURCE;
      if (quote)
        str->str[str->length++]= '"';
      ma_dynstr_append_mem(str, buff, len);
      if (quote)
        str->str[str->length++]= '"';
      break;
    case DYN_COL_NULL:
      if (ma_dynstr_append_mem(str, "null", 4))
        return ER_DYNCOL_RESOURCE;
      break;
    default:
      return(ER_DYNCOL_FORMAT);
  }
  return(ER_DYNCOL_OK);
}

enum enum_dyncol_func_result
mariadb_dyncol_val_long(longlong *ll, DYNAMIC_COLUMN_VALUE *val)
{
  enum enum_dyncol_func_result rc= ER_DYNCOL_OK;
  *ll= 0;
  switch (val->type) {
  case DYN_COL_INT:
      *ll= val->x.long_value;
      break;
    case DYN_COL_UINT:
      *ll= (longlong)val->x.ulong_value;
      if (val->x.ulong_value > ULONGLONG_MAX)
         rc= ER_DYNCOL_TRUNCATED;
      break;
    case DYN_COL_DOUBLE:
      *ll= (longlong)val->x.double_value;
      if (((double) *ll) != val->x.double_value)
        rc= ER_DYNCOL_TRUNCATED;
      break;
    case DYN_COL_STRING:
      {
        char *src= val->x.string.value.str;
        size_t len= val->x.string.value.length;
        longlong i= 0, sign= 1;

        while (len && isspace(*src)) src++,len--;

        if (len)
        {
          if (*src == '-')
          {
            sign= -1;
            src++;
          } else if (*src == '-')
            src++;
          while(len && isdigit(*src))
          {
            i= i * 10 + (*src - '0');
            src++;
          }
        }
        else
          rc= ER_DYNCOL_TRUNCATED;
        if (len)
          rc= ER_DYNCOL_TRUNCATED;
        *ll= i * sign;
        break;
      }
#ifndef LIBMARIADB
    case DYN_COL_DECIMAL:
      if (decimal2longlong(&val->x.decimal.value, ll) != E_DEC_OK)
        rc= ER_DYNCOL_TRUNCATED;
      break;
#endif
    case DYN_COL_DATETIME:
      *ll= (val->x.time_value.year * 10000000000ull +
            val->x.time_value.month * 100000000L +
            val->x.time_value.day * 1000000 +
            val->x.time_value.hour * 10000 +
            val->x.time_value.minute * 100 +
            val->x.time_value.second) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_DATE:
      *ll= (val->x.time_value.year * 10000 +
            val->x.time_value.month * 100 +
            val->x.time_value.day) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_TIME:
      *ll= (val->x.time_value.hour * 10000 +
            val->x.time_value.minute * 100 +
            val->x.time_value.second) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_DYNCOL:
    case DYN_COL_NULL:
      rc= ER_DYNCOL_TRUNCATED;
      break;
    default:
      return(ER_DYNCOL_FORMAT);
  }
  return(rc);
}


enum enum_dyncol_func_result
mariadb_dyncol_val_double(double *dbl, DYNAMIC_COLUMN_VALUE *val)
{
  enum enum_dyncol_func_result rc= ER_DYNCOL_OK;
  *dbl= 0;
  switch (val->type) {
  case DYN_COL_INT:
      *dbl= (double)val->x.long_value;
      if (((longlong) *dbl) != val->x.long_value)
        rc= ER_DYNCOL_TRUNCATED;
      break;
    case DYN_COL_UINT:
      *dbl= (double)val->x.ulong_value;
      if (((ulonglong) *dbl) != val->x.ulong_value)
        rc= ER_DYNCOL_TRUNCATED;
      break;
    case DYN_COL_DOUBLE:
      *dbl= val->x.double_value;
      break;
    case DYN_COL_STRING:
      {
        char *str, *end;
        if ((str= malloc(val->x.string.value.length + 1)))
          return ER_DYNCOL_RESOURCE;
        memcpy(str, val->x.string.value.str, val->x.string.value.length);
        str[val->x.string.value.length]= '\0';
        *dbl= strtod(str, &end);
        if (*end != '\0')
          rc= ER_DYNCOL_TRUNCATED;
      }
#ifndef LIBMARIADB
    case DYN_COL_DECIMAL:
      if (decimal2double(&val->x.decimal.value, dbl) != E_DEC_OK)
        rc= ER_DYNCOL_TRUNCATED;
      break;
#endif
    case DYN_COL_DATETIME:
      *dbl= (double)(val->x.time_value.year * 10000000000ull +
                     val->x.time_value.month * 100000000L +
                     val->x.time_value.day * 1000000 +
                     val->x.time_value.hour * 10000 +
                     val->x.time_value.minute * 100 +
                     val->x.time_value.second) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_DATE:
      *dbl= (double)(val->x.time_value.year * 10000 +
                     val->x.time_value.month * 100 +
                     val->x.time_value.day) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_TIME:
      *dbl= (double)(val->x.time_value.hour * 10000 +
                     val->x.time_value.minute * 100 +
                     val->x.time_value.second) *
        (val->x.time_value.neg ? -1 : 1);
      break;
    case DYN_COL_DYNCOL:
    case DYN_COL_NULL:
      rc= ER_DYNCOL_TRUNCATED;
      break;
    default:
      return(ER_DYNCOL_FORMAT);
  }
  return(rc);
}


/**
  Convert to JSON

  @param str             The packed string
  @param json            Where to put json result

  @return ER_DYNCOL_* return code
*/

#define JSON_STACK_PROTECTION 10

static enum enum_dyncol_func_result
mariadb_dyncol_json_internal(DYNAMIC_COLUMN *str, DYNAMIC_STRING *json,
                             uint lvl)
{
  DYN_HEADER header;
  uint i;
  enum enum_dyncol_func_result rc;

  if (lvl >= JSON_STACK_PROTECTION)
  {
    rc= ER_DYNCOL_RESOURCE;
    goto err;
  }


  if (str->length == 0)
    return ER_DYNCOL_OK;                        /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    goto err;

  if (header.entry_size * header.column_count + FIXED_HEADER_SIZE >
      str->length)
  {
    rc= ER_DYNCOL_FORMAT;
    goto err;
  }

  rc= ER_DYNCOL_RESOURCE;

  if (ma_dynstr_append_mem(json, "{", 1))
    goto err;
  for (i= 0, header.entry= header.header;
       i < header.column_count;
       i++, header.entry+= header.entry_size)
  {
    DYNAMIC_COLUMN_VALUE val;
    if (i != 0 && ma_dynstr_append_mem(json, ",", 1))
      goto err;
    header.length=
      hdr_interval_length(&header, header.entry + header.entry_size);
    header.data= header.dtpool + header.offset;
    /*
      Check that the found data is withing the ranges. This can happen if
      we get data with wrong offsets.
    */
    if (header.length == DYNCOL_OFFSET_ERROR ||
        header.length > INT_MAX || header.offset > header.data_size)
    {
      rc= ER_DYNCOL_FORMAT;
      goto err;
    }
    if ((rc= dynamic_column_get_value(&header, &val)) < 0)
      goto err;
    if (header.format == dyncol_fmt_num)
    {
      uint nm= uint2korr(header.entry);
      if (ma_dynstr_realloc(json, DYNCOL_NUM_CHAR + 3))
        goto err;
      json->str[json->length++]= '"';
      json->length+= snprintf(json->str + json->length,
                               DYNCOL_NUM_CHAR, "%u", nm);
    }
    else
    {
      LEX_STRING name;
      if (read_name(&header, header.entry, &name))
      {
        rc= ER_DYNCOL_FORMAT;
        goto err;
      }
      if (ma_dynstr_realloc(json, name.length + 3))
        goto err;
      json->str[json->length++]= '"';
      memcpy(json->str + json->length, name.str, name.length);
      json->length+= name.length;
    }
    json->str[json->length++]= '"';
    json->str[json->length++]= ':';
    if (val.type == DYN_COL_DYNCOL)
    {
      /* here we use it only for read so can cheat a bit */
      DYNAMIC_COLUMN dc;
      memset(&dc, 0, sizeof(dc));
      dc.str= val.x.string.value.str;
      dc.length= val.x.string.value.length;
      if (mariadb_dyncol_json_internal(&dc, json, lvl + 1) < 0)
      {
        dc.str= NULL; dc.length= 0;
        goto err;
      }
      dc.str= NULL; dc.length= 0;
    }
    else
    {
      if ((rc= mariadb_dyncol_val_str(json, &val,
                                      ma_charset_utf8_general_ci, '"')) < 0)
        goto err;
    }
  }
  if (ma_dynstr_append_mem(json, "}", 1))
  {
    rc= ER_DYNCOL_RESOURCE;
    goto err;
  }
  return ER_DYNCOL_OK;

err:
  json->length= 0;
  return rc;
}

enum enum_dyncol_func_result
mariadb_dyncol_json(DYNAMIC_COLUMN *str, DYNAMIC_STRING *json)
{

  if (ma_init_dynamic_string(json, NULL, str->length * 2, 100))
    return ER_DYNCOL_RESOURCE;

  return mariadb_dyncol_json_internal(str, json, 1);
}

/**
  Convert to DYNAMIC_COLUMN_VALUE values and names (LEX_STING) dynamic array

  @param str             The packed string
  @param count           number of elements in the arrays
  @param names           Where to put names (should be free by user)
  @param vals            Where to put values (should be free by user)

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_unpack(DYNAMIC_COLUMN *str,
                      uint *count,
                      LEX_STRING **names, DYNAMIC_COLUMN_VALUE **vals)
{
  DYN_HEADER header;
  char *nm;
  uint i;
  enum enum_dyncol_func_result rc;

  *count= 0; *names= 0; *vals= 0;

  if (str->length == 0)
    return ER_DYNCOL_OK;                      /* no columns */

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;


  if (header.entry_size * header.column_count + FIXED_HEADER_SIZE >
      str->length)
    return ER_DYNCOL_FORMAT;

  *vals= (DYNAMIC_COLUMN_VALUE *)malloc(sizeof(DYNAMIC_COLUMN_VALUE)* header.column_count);
  if (header.format == dyncol_fmt_num)
  {
    *names= (LEX_STRING *)malloc(sizeof(LEX_STRING) * header.column_count +
                      DYNCOL_NUM_CHAR * header.column_count);
    nm= (char *)((*names) + header.column_count);
  }
  else
  {
    *names= (LEX_STRING *)malloc(sizeof(LEX_STRING) * header.column_count);
    nm= 0;
  }
  if (!(*vals) || !(*names))
  {
    rc= ER_DYNCOL_RESOURCE;
    goto err;
  }

  for (i= 0, header.entry= header.header;
       i < header.column_count;
       i++, header.entry+= header.entry_size)
  {
    header.length=
      hdr_interval_length(&header, header.entry + header.entry_size);
    header.data= header.dtpool + header.offset;
    /*
      Check that the found data is withing the ranges. This can happen if
      we get data with wrong offsets.
    */
    if (header.length == DYNCOL_OFFSET_ERROR ||
        header.length > INT_MAX || header.offset > header.data_size)
    {
      rc= ER_DYNCOL_FORMAT;
      goto err;
    }
    if ((rc= dynamic_column_get_value(&header, (*vals) + i)) < 0)
      goto err;

    if (header.format == dyncol_fmt_num)
    {
      uint num= uint2korr(header.entry);
      (*names)[i].str= nm;
      (*names)[i].length= snprintf(nm, DYNCOL_NUM_CHAR, "%u", num);
      nm+= (*names)[i].length + 1;
    }
    else
    {
      if (read_name(&header, header.entry, (*names) + i))
      {
        rc= ER_DYNCOL_FORMAT;
        goto err;
      }
    }
  }

  *count= header.column_count;
  return ER_DYNCOL_OK;

err:
  if (*vals)
  {
    free(*vals);
    *vals= 0;
  }
  if (*names)
  {
    free(*names);
    *names= 0;
  }
  return rc;
}


/**
  Get not NULL column count

  @param str             The packed string
  @param column_count    Where to put column count

  @return ER_DYNCOL_* return code
*/

enum enum_dyncol_func_result
mariadb_dyncol_column_count(DYNAMIC_COLUMN *str, uint *column_count)
{
  DYN_HEADER header;
  enum enum_dyncol_func_result rc;

  (*column_count)= 0;
  if (str->length == 0)
    return ER_DYNCOL_OK;

  if ((rc= init_read_hdr(&header, str)) < 0)
    return rc;
  *column_count= header.column_count;
  return rc;
}

/**
  Release dynamic column memory

  @param str             dynamic column
  @return                void
*/
void mariadb_dyncol_free(DYNAMIC_COLUMN *str)
{
  ma_dynstr_free(str);
}
