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

#ifndef ZK_HASHTABLE_H_
#define ZK_HASHTABLE_H_

#include <zookeeper.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct watcher_object_list watcher_object_list_t;
typedef struct _zk_hashtable zk_hashtable;

/**
 * The function must return a non-zero value if the watcher object can be activated
 * as a result of the server response. Normally, a watch can only be activated
 * if the server returns a success code (ZOK). However in the case when zoo_exists() 
 * returns a ZNONODE code the watcher should be activated nevertheless.
 */
typedef zk_hashtable *(*result_checker_fn)(zhandle_t *, int rc);

/**
 * A watcher object gets temporarily stored with the completion entry until 
 * the server response comes back at which moment the watcher object is moved
 * to the active watchers map.
 */
typedef struct _watcher_registration {
    watcher_fn watcher;
    void* context;
    result_checker_fn checker;
    const char* path;
} watcher_registration_t;

zk_hashtable* create_zk_hashtable();
void destroy_zk_hashtable(zk_hashtable* ht);

char **collect_keys(zk_hashtable *ht, int *count);

/**
 * check if the completion has a watcher object associated
 * with it. If it does, move the watcher object to the map of
 * active watchers (only if the checker allows to do so)
 */
    void activateWatcher(zhandle_t *zh, watcher_registration_t* reg, int rc);
    watcher_object_list_t *collectWatchers(zhandle_t *zh,int type, char *path);
    void deliverWatchers(zhandle_t *zh, int type, int state, char *path, struct watcher_object_list **list);

#ifdef __cplusplus
}
#endif

#endif /*ZK_HASHTABLE_H_*/
