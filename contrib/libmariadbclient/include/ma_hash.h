/************************************************************************************
    Copyright (C) 2000, 2012 MySQL AB & MySQL Finland AB & TCX DataKonsult AB,
                 Monty Program AB
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

   Part of this code includes code from the PHP project which
   is freely available from http://www.php.net
*************************************************************************************/

#ifndef _ma_hash_h
#define _ma_hash_h
#ifdef	__cplusplus
extern "C" {
#endif

typedef uchar *(*hash_get_key)(const uchar *,uint*,my_bool);
typedef void (*hash_free_key)(void *);

  /* flags for hash_init */
#define HASH_CASE_INSENSITIVE	1

typedef struct st_hash_info {
  uint next;					/* index to next key */
  uchar *data;					/* data for current entry */
} HASH_LINK;

typedef struct st_hash {
  uint key_offset,key_length;		/* Length of key if const length */
  uint records,blength,current_record;
  uint flags;
  DYNAMIC_ARRAY array;				/* Place for hash_keys */
  hash_get_key get_key;
  void (*free)(void *);
  uint (*calc_hashnr)(const uchar *key,uint length);
} HASH;

#define hash_init(A,B,C,D,E,F,G) _hash_init(A,B,C,D,E,F,G CALLER_INFO)
my_bool _hash_init(HASH *hash,uint default_array_elements, uint key_offset,
		  uint key_length, hash_get_key get_key,
		  void (*free_element)(void*), uint flags CALLER_INFO_PROTO);
void hash_free(HASH *tree);
uchar *hash_element(HASH *hash,uint idx);
void * hash_search(HASH *info,const uchar *key,uint length);
void * hash_next(HASH *info,const uchar *key,uint length);
my_bool hash_insert(HASH *info,const uchar *data);
my_bool hash_delete(HASH *hash,uchar *record);
my_bool hash_update(HASH *hash,uchar *record,uchar *old_key,uint old_key_length);
my_bool hash_check(HASH *hash);			/* Only in debug library */

#define hash_clear(H) memset((char*) (H), 0,sizeof(*(H)))
#define hash_inited(H) ((H)->array.buffer != 0)

#ifdef	__cplusplus
}
#endif
#endif
