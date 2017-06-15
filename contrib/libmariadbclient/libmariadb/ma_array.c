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

/* Handling of arrays that can grow dynamicly. */

#undef SAFEMALLOC /* Problems with threads */

#include <ma_global.h>
#include <ma_sys.h>
#include "ma_string.h"
#include <memory.h>

/*
  Initiate array and alloc space for init_alloc elements. Array is usable
  even if space allocation failed
*/

my_bool ma_init_dynamic_array(DYNAMIC_ARRAY *array, uint element_size,
                              uint init_alloc, uint alloc_increment CALLER_INFO_PROTO)
{
  if (!alloc_increment)
  {
    alloc_increment=max((8192-MALLOC_OVERHEAD)/element_size,16);
    if (init_alloc > 8 && alloc_increment > init_alloc * 2)
      alloc_increment=init_alloc*2;
  }

  if (!init_alloc)
    init_alloc=alloc_increment;
  array->elements=0;
  array->max_element=init_alloc;
  array->alloc_increment=alloc_increment;
  array->size_of_element=element_size;
  if (!(array->buffer=(char*) malloc(element_size*init_alloc)))
  {
    array->max_element=0;
    return(TRUE);
  }
  return(FALSE);
}


my_bool ma_insert_dynamic(DYNAMIC_ARRAY *array, void *element)
{
  void *buffer;
  if (array->elements == array->max_element)
  {						/* Call only when nessesary */
    if (!(buffer=ma_alloc_dynamic(array)))
      return TRUE;
  }
  else
  {
    buffer=array->buffer+(array->elements * array->size_of_element);
    array->elements++;
  }
  memcpy(buffer,element,(size_t) array->size_of_element);
  return FALSE;
}


	/* Alloc room for one element */

unsigned char *ma_alloc_dynamic(DYNAMIC_ARRAY *array)
{
  if (array->elements == array->max_element)
  {
    char *new_ptr;
    if (!(new_ptr=(char*) realloc(array->buffer,(array->max_element+
			          array->alloc_increment)*
				   array->size_of_element)))
      return 0;
    array->buffer=new_ptr;
    array->max_element+=array->alloc_increment;
  }
  return (unsigned char *)array->buffer+(array->elements++ * array->size_of_element);
}


	/* remove last element from array and return it */

unsigned char *ma_pop_dynamic(DYNAMIC_ARRAY *array)
{
  if (array->elements)
    return (unsigned char *)array->buffer+(--array->elements * array->size_of_element);
  return 0;
}


my_bool ma_set_dynamic(DYNAMIC_ARRAY *array, void * element, uint idx)
{
  if (idx >= array->elements)
  {
    if (idx >= array->max_element)
    {
      uint size;
      char *new_ptr;
      size=(idx+array->alloc_increment)/array->alloc_increment;
      size*= array->alloc_increment;
      if (!(new_ptr=(char*) realloc(array->buffer,size*
			            array->size_of_element)))
	return TRUE;
      array->buffer=new_ptr;
      array->max_element=size;
    }
    memset((array->buffer+array->elements*array->size_of_element), 0,
	  (idx - array->elements)*array->size_of_element);
    array->elements=idx+1;
  }
  memcpy(array->buffer+(idx * array->size_of_element),element,
	 (size_t) array->size_of_element);
  return FALSE;
}


void ma_get_dynamic(DYNAMIC_ARRAY *array, void * element, uint idx)
{
  if (idx >= array->elements)
  {
    memset(element, 0, array->size_of_element);
    return;
  }
  memcpy(element,array->buffer+idx*array->size_of_element,
	 (size_t) array->size_of_element);
}


void ma_delete_dynamic(DYNAMIC_ARRAY *array)
{
  if (array->buffer)
  {
    free(array->buffer);
    array->buffer=0;
    array->elements=array->max_element=0;
  }
}


void ma_delete_dynamic_element(DYNAMIC_ARRAY *array, uint idx)
{
  char *ptr=array->buffer+array->size_of_element*idx;
  array->elements--;
  memmove(ptr,ptr+array->size_of_element,
	  (array->elements-idx)*array->size_of_element);
}


void ma_freeze_size(DYNAMIC_ARRAY *array)
{
  uint elements=max(array->elements,1);

  if (array->buffer && array->max_element != elements)
  {
    array->buffer=(char*) realloc(array->buffer,
			          elements*array->size_of_element);
    array->max_element=elements;
  }
}
