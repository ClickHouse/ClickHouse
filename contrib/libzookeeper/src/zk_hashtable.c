/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "zk_hashtable.h"
#include "zk_adaptor.h"
#include "hashtable/hashtable.h"
#include "hashtable/hashtable_itr.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>

typedef struct _watcher_object {
    watcher_fn watcher;
    void* context;
    struct _watcher_object* next;
} watcher_object_t;


struct _zk_hashtable {
    struct hashtable* ht;
};

struct watcher_object_list {
    watcher_object_t* head;
};

/* the following functions are for testing only */
typedef struct hashtable hashtable_impl;

hashtable_impl* getImpl(zk_hashtable* ht){
    return ht->ht;
}

watcher_object_t* getFirstWatcher(zk_hashtable* ht,const char* path)
{
    watcher_object_list_t* wl=hashtable_search(ht->ht,(void*)path);
    if(wl!=0)
        return wl->head;
    return 0;
}
/* end of testing functions */

watcher_object_t* clone_watcher_object(watcher_object_t* wo)
{
    watcher_object_t* res=calloc(1,sizeof(watcher_object_t));
    assert(res);
    res->watcher=wo->watcher;
    res->context=wo->context;
    return res;
}

static unsigned int string_hash_djb2(void *str) 
{
    unsigned int hash = 5381;
    int c;
    const char* cstr = (const char*)str;
    while ((c = *cstr++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

    return hash;
}

static int string_equal(void *key1,void *key2)
{
    return strcmp((const char*)key1,(const char*)key2)==0;
}

static watcher_object_t* create_watcher_object(watcher_fn watcher,void* ctx)
{
    watcher_object_t* wo=calloc(1,sizeof(watcher_object_t));
    assert(wo);
    wo->watcher=watcher;
    wo->context=ctx;
    return wo;
}

static watcher_object_list_t* create_watcher_object_list(watcher_object_t* head) 
{
    watcher_object_list_t* wl=calloc(1,sizeof(watcher_object_list_t));
    assert(wl);
    wl->head=head;
    return wl;
}

static void destroy_watcher_object_list(watcher_object_list_t* list)
{
    watcher_object_t* e = NULL;

    if(list==0)
        return;
    e=list->head;
    while(e!=0){
        watcher_object_t* this=e;
        e=e->next;
        free(this);
    }
    free(list);
}

zk_hashtable* create_zk_hashtable()
{
    struct _zk_hashtable *ht=calloc(1,sizeof(struct _zk_hashtable));
    assert(ht);
    ht->ht=create_hashtable(32,string_hash_djb2,string_equal);
    return ht;
}

static void do_clean_hashtable(zk_hashtable* ht)
{
    struct hashtable_itr *it;
    int hasMore;
    if(hashtable_count(ht->ht)==0)
        return;
    it=hashtable_iterator(ht->ht);
    do {
        watcher_object_list_t* w=hashtable_iterator_value(it);
        destroy_watcher_object_list(w);
        hasMore=hashtable_iterator_remove(it);
    } while(hasMore);
    free(it);
}

void destroy_zk_hashtable(zk_hashtable* ht)
{
    if(ht!=0){
        do_clean_hashtable(ht);
        hashtable_destroy(ht->ht,0);
        free(ht);
    }
}

// searches for a watcher object instance in a watcher object list;
// two watcher objects are equal if their watcher function and context pointers
// are equal
static watcher_object_t* search_watcher(watcher_object_list_t** wl,watcher_object_t* wo)
{
    watcher_object_t* wobj=(*wl)->head;
    while(wobj!=0){
        if(wobj->watcher==wo->watcher && wobj->context==wo->context)
            return wobj;
        wobj=wobj->next;
    }
    return 0;
}

static int add_to_list(watcher_object_list_t **wl, watcher_object_t *wo,
                       int clone)
{
    if (search_watcher(wl, wo)==0) {
        watcher_object_t* cloned=wo;
        if (clone) {
            cloned = clone_watcher_object(wo);
            assert(cloned);
        }
        cloned->next = (*wl)->head;
        (*wl)->head = cloned;
        return 1;
    } else if (!clone) {
        // If it's here and we aren't supposed to clone, we must destroy
        free(wo);
    }
    return 0;
}

static int do_insert_watcher_object(zk_hashtable *ht, const char *path, watcher_object_t* wo)
{
    int res=1;
    watcher_object_list_t* wl;

    wl=hashtable_search(ht->ht,(void*)path);
    if(wl==0){
        int res;
        /* inserting a new path element */
        res=hashtable_insert(ht->ht,strdup(path),create_watcher_object_list(wo));
        assert(res);
    }else{
        /*
         * Path already exists; check if the watcher already exists.
         * Don't clone the watcher since it's allocated on the heap --- avoids
         * a memory leak and saves a clone operation (calloc + copy).
         */
        res = add_to_list(&wl, wo, 0);
    }
    return res;    
}


char **collect_keys(zk_hashtable *ht, int *count)
{
    char **list;
    struct hashtable_itr *it;
    int i;

    *count = hashtable_count(ht->ht);
    list = calloc(*count, sizeof(char*));
    it=hashtable_iterator(ht->ht);
    for(i = 0; i < *count; i++) {
        list[i] = strdup(hashtable_iterator_key(it));
        hashtable_iterator_advance(it);
    }
    free(it);
    return list;
}

static int insert_watcher_object(zk_hashtable *ht, const char *path,
                                 watcher_object_t* wo)
{
    int res;
    res=do_insert_watcher_object(ht,path,wo);
    return res;
}

static void copy_watchers(watcher_object_list_t *from, watcher_object_list_t *to, int clone)
{
    watcher_object_t* wo=from->head;
    while(wo){
        watcher_object_t *next = wo->next;
        add_to_list(&to, wo, clone);
        wo=next;
    }
}

static void copy_table(zk_hashtable *from, watcher_object_list_t *to) {
    struct hashtable_itr *it;
    int hasMore;
    if(hashtable_count(from->ht)==0)
        return;
    it=hashtable_iterator(from->ht);
    do {
        watcher_object_list_t *w = hashtable_iterator_value(it);
        copy_watchers(w, to, 1);
        hasMore=hashtable_iterator_advance(it);
    } while(hasMore);
    free(it);
}

static void collect_session_watchers(zhandle_t *zh,
                                     watcher_object_list_t **list)
{
    copy_table(zh->active_node_watchers, *list);
    copy_table(zh->active_exist_watchers, *list);
    copy_table(zh->active_child_watchers, *list);
}

static void add_for_event(zk_hashtable *ht, char *path, watcher_object_list_t **list)
{
    watcher_object_list_t* wl;
    wl = (watcher_object_list_t*)hashtable_remove(ht->ht, path);
    if (wl) {
        copy_watchers(wl, *list, 0);
        // Since we move, not clone the watch_objects, we just need to free the
        // head pointer
        free(wl);
    }
}

static void do_foreach_watcher(watcher_object_t* wo,zhandle_t* zh,
        const char* path,int type,int state)
{
    // session event's don't have paths
    const char *client_path =
        (type != ZOO_SESSION_EVENT ? sub_string(zh, path) : path);
    while(wo!=0){
        wo->watcher(zh,type,state,client_path,wo->context);
        wo=wo->next;
    }    
    free_duplicate_path(client_path, path);
}

watcher_object_list_t *collectWatchers(zhandle_t *zh,int type, char *path)
{
    struct watcher_object_list *list = create_watcher_object_list(0); 

    if(type==ZOO_SESSION_EVENT){
        watcher_object_t defWatcher;
        defWatcher.watcher=zh->watcher;
        defWatcher.context=zh->context;
        add_to_list(&list, &defWatcher, 1);
        collect_session_watchers(zh, &list);
        return list;
    }
    switch(type){
    case CREATED_EVENT_DEF:
    case CHANGED_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(zh->active_node_watchers,path,&list);
        add_for_event(zh->active_exist_watchers,path,&list);
        break;
    case CHILD_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(zh->active_child_watchers,path,&list);
        break;
    case DELETED_EVENT_DEF:
        // look up the watchers for the path and move them to a delivery list
        add_for_event(zh->active_node_watchers,path,&list);
        add_for_event(zh->active_exist_watchers,path,&list);
        add_for_event(zh->active_child_watchers,path,&list);
        break;
    }
    return list;
}

void deliverWatchers(zhandle_t *zh, int type,int state, char *path, watcher_object_list_t **list)
{
    if (!list || !(*list)) return;
    do_foreach_watcher((*list)->head, zh, path, type, state);
    destroy_watcher_object_list(*list);
    *list = 0;
}

void activateWatcher(zhandle_t *zh, watcher_registration_t* reg, int rc)
{
    if(reg){
        /* in multithreaded lib, this code is executed 
         * by the IO thread */
        zk_hashtable *ht = reg->checker(zh, rc);
        if(ht){
            insert_watcher_object(ht,reg->path,
                    create_watcher_object(reg->watcher, reg->context));
        }
    }    
}
