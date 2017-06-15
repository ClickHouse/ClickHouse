/* Copyright (C) 2000 MySQL AB & MySQL Finland AB & TCX DataKonsult AB
   
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

#ifndef _list_h_
#define _list_h_

#ifdef	__cplusplus
extern "C" {
#endif

typedef struct st_list {
  struct st_list *prev,*next;
  void *data;
} LIST;

typedef int (*list_walk_action)(void *,void *);

extern LIST *list_add(LIST *root,LIST *element);
extern LIST *list_delete(LIST *root,LIST *element);
extern LIST *list_cons(void *data,LIST *root);
extern LIST *list_reverse(LIST *root);
extern void list_free(LIST *root,unsigned int free_data);
extern unsigned int list_length(LIST *list);
extern int list_walk(LIST *list,list_walk_action action,char * argument);

#define rest(a) ((a)->next)
#define list_push(a,b) (a)=list_cons((b),(a))
#define list_pop(A) {LIST *old=(A); (A)=list_delete(old,old) ; ma_free((char *) old,MYF(MY_FAE)); }

#ifdef	__cplusplus
}
#endif
#endif
