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

#ifndef DLL_EXPORT
#  define USE_STATIC_LIB
#endif

#include "zk_adaptor.h"
#include <stdlib.h>
#include <time.h>

int zoo_lock_auth(zhandle_t *zh)
{
	return 0;
}
int zoo_unlock_auth(zhandle_t *zh)
{
	return 0;
}
int lock_buffer_list(buffer_head_t *l)
{
	return 0;
}
int unlock_buffer_list(buffer_head_t *l)
{
	return 0;
}
int lock_completion_list(completion_head_t *l)
{
	return 0;
}
int unlock_completion_list(completion_head_t *l)
{
	return 0;
}
struct sync_completion *alloc_sync_completion(void)
{
    return (struct sync_completion*)calloc(1, sizeof(struct sync_completion));
}
int wait_sync_completion(struct sync_completion *sc)
{
    return 0;
}

void free_sync_completion(struct sync_completion *sc)
{
    free(sc);
}

void notify_sync_completion(struct sync_completion *sc)
{
}

int process_async(int outstanding_sync)
{
    return outstanding_sync == 0;
}

int adaptor_init(zhandle_t *zh)
{
    return 0;
}

void adaptor_finish(zhandle_t *zh){}

void adaptor_destroy(zhandle_t *zh){}

int flush_send_queue(zhandle_t *, int);

int adaptor_send_queue(zhandle_t *zh, int timeout)
{
    return flush_send_queue(zh, timeout);
}

int32_t inc_ref_counter(zhandle_t* zh,int i)
{
    zh->ref_counter+=(i<0?-1:(i>0?1:0));
    return zh->ref_counter;
}

int32_t get_xid()
{
    static int32_t xid = -1;
    if (xid == -1) {
        xid = time(0);
    }
    return xid++;
}

int enter_critical(zhandle_t* zh)
{
	return 0;
}

int leave_critical(zhandle_t* zh)
{
	return 0;
}
