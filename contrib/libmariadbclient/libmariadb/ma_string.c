/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
                 2016 MariaDB Corporation AB

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not, write to the Free
   Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02111-1301, USA */

/*
  Code for handling strings with can grow dynamicly.
  Copyright Monty Program KB.
  By monty.
*/

#include <ma_global.h>
#include <ma_sys.h>
#include <ma_string.h>

my_bool ma_init_dynamic_string(DYNAMIC_STRING *str, const char *init_str,
			    size_t init_alloc, size_t alloc_increment)
{
  uint length;

  if (!alloc_increment)
    alloc_increment=128;
  length=1;
  if (init_str && (length= (uint) strlen(init_str)+1) < init_alloc)
    init_alloc=((length+alloc_increment-1)/alloc_increment)*alloc_increment;
  if (!init_alloc)
    init_alloc=alloc_increment;

  if (!(str->str=(char*) malloc(init_alloc)))
    return(TRUE);
  str->length=length-1;
  if (init_str)
    memcpy(str->str,init_str,length);
  str->max_length=init_alloc;
  str->alloc_increment=alloc_increment;
  return(FALSE);
}

my_bool ma_dynstr_set(DYNAMIC_STRING *str, const char *init_str)
{
  uint length;

  if (init_str && (length= (uint) strlen(init_str)+1) > str->max_length)
  {
    str->max_length=((length+str->alloc_increment-1)/str->alloc_increment)*
      str->alloc_increment;
    if (!str->max_length)
      str->max_length=str->alloc_increment;
    if (!(str->str=(char*) realloc(str->str,str->max_length)))
      return(TRUE);
  }
  if (init_str)
  {
    str->length=length-1;
    memcpy(str->str,init_str,length);
  }
  else
    str->length=0;
  return(FALSE);
}


my_bool ma_dynstr_realloc(DYNAMIC_STRING *str, size_t additional_size)
{
  if (!additional_size) return(FALSE);
  if (str->length + additional_size > str->max_length)
  {
    str->max_length=((str->length + additional_size+str->alloc_increment-1)/
		     str->alloc_increment)*str->alloc_increment;
    if (!(str->str=(char*) realloc(str->str,str->max_length)))
      return(TRUE);
  }
  return(FALSE);
}


my_bool ma_dynstr_append(DYNAMIC_STRING *str, const char *append)
{
  return ma_dynstr_append_mem(str,append,strlen(append));
}


my_bool ma_dynstr_append_mem(DYNAMIC_STRING *str, const char *append,
			  size_t length)
{
  char *new_ptr;
  if (str->length+length >= str->max_length)
  {
    size_t new_length=(str->length+length+str->alloc_increment)/
      str->alloc_increment;
    new_length*=str->alloc_increment;
    if (!(new_ptr=(char*) realloc(str->str,new_length)))
      return TRUE;
    str->str=new_ptr;
    str->max_length=new_length;
  }
  memcpy(str->str + str->length,append,length);
  str->length+=length;
  str->str[str->length]=0;			/* Safety for C programs */
  return FALSE;
}


void ma_dynstr_free(DYNAMIC_STRING *str)
{
  if (str->str)
  {
    free(str->str);
    str->str=0;
  }
}

char *ma_strmake(register char *dst, register const char *src, size_t length)
{
  while (length--)
    if (! (*dst++ = *src++))
      return dst-1;
  *dst=0;
  return dst;
}
