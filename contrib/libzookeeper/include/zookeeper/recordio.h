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
#ifndef __RECORDIO_H__
#define __RECORDIO_H__

#include <sys/types.h>
#ifndef WIN32
#define STRUCT_INITIALIZER(l,r) .l = r
#else
#define STRUCT_INITIALIZER(l,r)  r
#include "winconfig.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct buffer {
    int32_t len;
    char *buff;
};

void deallocate_String(char **s);
void deallocate_Buffer(struct buffer *b);
void deallocate_vector(void *d);
struct iarchive {
    int (*start_record)(struct iarchive *ia, const char *tag);
    int (*end_record)(struct iarchive *ia, const char *tag);
    int (*start_vector)(struct iarchive *ia, const char *tag, int32_t *count);
    int (*end_vector)(struct iarchive *ia, const char *tag);
    int (*deserialize_Bool)(struct iarchive *ia, const char *name, int32_t *);
    int (*deserialize_Int)(struct iarchive *ia, const char *name, int32_t *);
    int (*deserialize_Long)(struct iarchive *ia, const char *name, int64_t *);
    int (*deserialize_Buffer)(struct iarchive *ia, const char *name,
            struct buffer *);
    int (*deserialize_String)(struct iarchive *ia, const char *name, char **);
    void *priv;
};
struct oarchive {
    int (*start_record)(struct oarchive *oa, const char *tag);
    int (*end_record)(struct oarchive *oa, const char *tag);
    int (*start_vector)(struct oarchive *oa, const char *tag, const int32_t *count);
    int (*end_vector)(struct oarchive *oa, const char *tag);
    int (*serialize_Bool)(struct oarchive *oa, const char *name, const int32_t *);
    int (*serialize_Int)(struct oarchive *oa, const char *name, const int32_t *);
    int (*serialize_Long)(struct oarchive *oa, const char *name,
            const int64_t *);
    int (*serialize_Buffer)(struct oarchive *oa, const char *name,
            const struct buffer *);
    int (*serialize_String)(struct oarchive *oa, const char *name, char **);
    void *priv;
};

struct oarchive *create_buffer_oarchive(void);
void close_buffer_oarchive(struct oarchive **oa, int free_buffer);
struct iarchive *create_buffer_iarchive(char *buffer, int len);
void close_buffer_iarchive(struct iarchive **ia);
char *get_buffer(struct oarchive *);
int get_buffer_len(struct oarchive *);

int64_t zoo_htonll(int64_t v);

#ifdef __cplusplus
}
#endif

#endif
