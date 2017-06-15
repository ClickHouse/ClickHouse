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
  Code for handling dubble-linked lists in C
*/

#include <ma_global.h>
#include <ma_sys.h>
#include <ma_list.h>

	/* Add a element to start of list */

LIST *list_add(LIST *root, LIST *element)
{
  if (root)
  {
    if (root->prev)			/* If add in mid of list */
      root->prev->next= element;
    element->prev=root->prev;
    root->prev=element;
  }
  else
    element->prev=0;
  element->next=root;
  return(element);			/* New root */
}


LIST *list_delete(LIST *root, LIST *element)
{
  if (element->prev)
    element->prev->next=element->next;
  else
    root=element->next;
  if (element->next)
    element->next->prev=element->prev;
  return root;
}


void list_free(LIST *root, unsigned int free_data)
{
  LIST *next;
  while (root)
  {
    next=root->next;
    if (free_data)
      free(root->data);
    free(root);
    root=next;
  }
}


LIST *list_cons(void *data, LIST *list)
{
  LIST *new_charset=(LIST*) malloc(sizeof(LIST));
  if (!new_charset)
    return 0;
  new_charset->data=data;
  return list_add(list,new_charset);
}


LIST *list_reverse(LIST *root)
{
  LIST *last;

  last=root;
  while (root)
  {
    last=root;
    root=root->next;
    last->next=last->prev;
    last->prev=root;
  }
  return last;
}

uint list_length(LIST *list)
{
  uint count;
  for (count=0 ; list ; list=list->next, count++) ;
  return count;
}


int list_walk(LIST *list, list_walk_action action, gptr argument)
{
  int error=0;
  while (list)
  {
    if ((error = (*action)(list->data,argument)))
      return error;
    list=rest(list);
  }
  return 0;
}
