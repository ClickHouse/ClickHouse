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

#include <recordio.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#ifndef WIN32
#include <netinet/in.h>
#endif

void deallocate_String(char **s)
{
    if (*s)
        free(*s);
    *s = 0;
}

void deallocate_Buffer(struct buffer *b)
{
    if (b->buff)
        free(b->buff);
    b->buff = 0;
}

struct buff_struct {
    int32_t len;
    int32_t off;
    char *buffer;
};

static int resize_buffer(struct buff_struct *s, int newlen)
{
    char *buffer= NULL;
    while (s->len < newlen) {
        s->len *= 2;
    }
    buffer = (char*)realloc(s->buffer, s->len);
    if (!buffer) {
        s->buffer = 0;
        return -ENOMEM;
    }
    s->buffer = buffer;
    return 0;
}

int oa_start_record(struct oarchive *oa, const char *tag)
{
    return 0;
}
int oa_end_record(struct oarchive *oa, const char *tag)
{
    return 0;
}
int oa_serialize_int(struct oarchive *oa, const char *tag, const int32_t *d)
{
    struct buff_struct *priv = oa->priv;
    int32_t i = htonl(*d);
    if ((priv->len - priv->off) < sizeof(i)) {
        int rc = resize_buffer(priv, priv->len + sizeof(i));
        if (rc < 0) return rc;
    }
    memcpy(priv->buffer+priv->off, &i, sizeof(i));
    priv->off+=sizeof(i);
    return 0;
}
int64_t zoo_htonll(int64_t v)
{
    int i = 0;
    char *s = (char *)&v;
    if (htonl(1) == 1) {
        return v;
    }
    for (i = 0; i < 4; i++) {
        int tmp = s[i];
        s[i] = s[8-i-1];
        s[8-i-1] = tmp;
    }

    return v;
}

int oa_serialize_long(struct oarchive *oa, const char *tag, const int64_t *d)
{
    const int64_t i = zoo_htonll(*d);
    struct buff_struct *priv = oa->priv;
    if ((priv->len - priv->off) < sizeof(i)) {
        int rc = resize_buffer(priv, priv->len + sizeof(i));
        if (rc < 0) return rc;
    }
    memcpy(priv->buffer+priv->off, &i, sizeof(i));
    priv->off+=sizeof(i);
    return 0;
}
int oa_start_vector(struct oarchive *oa, const char *tag, const int32_t *count)
{
    return oa_serialize_int(oa, tag, count);
}
int oa_end_vector(struct oarchive *oa, const char *tag)
{
    return 0;
}
int oa_serialize_bool(struct oarchive *oa, const char *name, const int32_t *i)
{
    //return oa_serialize_int(oa, name, i);
    struct buff_struct *priv = oa->priv;
    if ((priv->len - priv->off) < 1) {
        int rc = resize_buffer(priv, priv->len + 1);
        if (rc < 0)
            return rc;
    }
    priv->buffer[priv->off] = (*i == 0 ? '\0' : '\1');
    priv->off++;
    return 0;
}
static const int32_t negone = -1;
int oa_serialize_buffer(struct oarchive *oa, const char *name,
        const struct buffer *b)
{
    struct buff_struct *priv = oa->priv;
    int rc;
    if (!b) {
        return oa_serialize_int(oa, "len", &negone);
    }
    rc = oa_serialize_int(oa, "len", &b->len);
    if (rc < 0)
        return rc;
    // this means a buffer of NUll 
    // with size of -1. This is 
    // waht we use in java serialization for NULL
    if (b->len == -1) {
      return rc;
    }
    if ((priv->len - priv->off) < b->len) {
        rc = resize_buffer(priv, priv->len + b->len);
        if (rc < 0)
            return rc;
    }
    memcpy(priv->buffer+priv->off, b->buff, b->len);
    priv->off += b->len;
    return 0;
}
int oa_serialize_string(struct oarchive *oa, const char *name, char **s)
{
    struct buff_struct *priv = oa->priv;
    int32_t len;
    int rc;
    if (!*s) {
        oa_serialize_int(oa, "len", &negone);
        return 0;
    }
    len = strlen(*s);
    rc = oa_serialize_int(oa, "len", &len);
    if (rc < 0)
        return rc;
    if ((priv->len - priv->off) < len) {
        rc = resize_buffer(priv, priv->len + len);
        if (rc < 0)
            return rc;
    }
    memcpy(priv->buffer+priv->off, *s, len);
    priv->off += len;
    return 0;
}
int ia_start_record(struct iarchive *ia, const char *tag)
{
    return 0;
}
int ia_end_record(struct iarchive *ia, const char *tag)
{
    return 0;
}
int ia_deserialize_int(struct iarchive *ia, const char *tag, int32_t *count)
{
    struct buff_struct *priv = ia->priv;
    if ((priv->len - priv->off) < sizeof(*count)) {
        return -E2BIG;
    }
    memcpy(count, priv->buffer+priv->off, sizeof(*count));
    priv->off+=sizeof(*count);
    *count = ntohl(*count);
    return 0;
}

int ia_deserialize_long(struct iarchive *ia, const char *tag, int64_t *count)
{
    struct buff_struct *priv = ia->priv;
    int64_t v = 0;
    if ((priv->len - priv->off) < sizeof(*count)) {
        return -E2BIG;
    }
    memcpy(count, priv->buffer+priv->off, sizeof(*count));
    priv->off+=sizeof(*count);
    v = zoo_htonll(*count); // htonll and  ntohll do the same
    *count = v;
    return 0;
}
int ia_start_vector(struct iarchive *ia, const char *tag, int32_t *count)
{
    return ia_deserialize_int(ia, tag, count);
}
int ia_end_vector(struct iarchive *ia, const char *tag)
{
    return 0;
}
int ia_deserialize_bool(struct iarchive *ia, const char *name, int32_t *v)
{
    struct buff_struct *priv = ia->priv;
    //fprintf(stderr, "Deserializing bool %d\n", priv->off);
    //return ia_deserialize_int(ia, name, v);
    if ((priv->len - priv->off) < 1) {
        return -E2BIG;
    }
    *v = priv->buffer[priv->off];
    priv->off+=1;
    //fprintf(stderr, "Deserializing bool end %d\n", priv->off);
    return 0;
}
int ia_deserialize_buffer(struct iarchive *ia, const char *name,
        struct buffer *b)
{
    struct buff_struct *priv = ia->priv;
    int rc = ia_deserialize_int(ia, "len", &b->len);
    if (rc < 0)
        return rc;
    if ((priv->len - priv->off) < b->len) {
        return -E2BIG;
    }
    // set the buffer to null
    if (b->len == -1) {
       b->buff = NULL;
       return rc;
    }
    b->buff = malloc(b->len);
    if (!b->buff) {
        return -ENOMEM;
    }
    memcpy(b->buff, priv->buffer+priv->off, b->len);
    priv->off += b->len;
    return 0;
}
int ia_deserialize_string(struct iarchive *ia, const char *name, char **s)
{
    struct buff_struct *priv = ia->priv;
    int32_t len;
    int rc = ia_deserialize_int(ia, "len", &len);
    if (rc < 0)
        return rc;
    if ((priv->len - priv->off) < len) {
        return -E2BIG;
    }
    if (len < 0) {
        return -EINVAL;
    }
    *s = malloc(len+1);
    if (!*s) {
        return -ENOMEM;
    }
    memcpy(*s, priv->buffer+priv->off, len);
    (*s)[len] = '\0';
    priv->off += len;
    return 0;
}

static struct iarchive ia_default = { STRUCT_INITIALIZER (start_record ,ia_start_record),
        STRUCT_INITIALIZER (end_record ,ia_end_record), STRUCT_INITIALIZER (start_vector , ia_start_vector),
        STRUCT_INITIALIZER (end_vector ,ia_end_vector), STRUCT_INITIALIZER (deserialize_Bool , ia_deserialize_bool),
        STRUCT_INITIALIZER (deserialize_Int ,ia_deserialize_int),
        STRUCT_INITIALIZER (deserialize_Long , ia_deserialize_long) ,
        STRUCT_INITIALIZER (deserialize_Buffer, ia_deserialize_buffer),
        STRUCT_INITIALIZER (deserialize_String, ia_deserialize_string)   };

static struct oarchive oa_default = { STRUCT_INITIALIZER (start_record , oa_start_record),
        STRUCT_INITIALIZER (end_record , oa_end_record), STRUCT_INITIALIZER (start_vector , oa_start_vector),
        STRUCT_INITIALIZER (end_vector , oa_end_vector), STRUCT_INITIALIZER (serialize_Bool , oa_serialize_bool),
        STRUCT_INITIALIZER (serialize_Int , oa_serialize_int),
        STRUCT_INITIALIZER (serialize_Long , oa_serialize_long) ,
        STRUCT_INITIALIZER (serialize_Buffer , oa_serialize_buffer),
        STRUCT_INITIALIZER (serialize_String , oa_serialize_string) };

struct iarchive *create_buffer_iarchive(char *buffer, int len)
{
    struct iarchive *ia = malloc(sizeof(*ia));
    struct buff_struct *buff = malloc(sizeof(struct buff_struct));
    if (!ia) return 0;
    if (!buff) {
        free(ia);
        return 0;
    }
    *ia = ia_default;
    buff->off = 0;
    buff->buffer = buffer;
    buff->len = len;
    ia->priv = buff;
    return ia;
}

struct oarchive *create_buffer_oarchive()
{
    struct oarchive *oa = malloc(sizeof(*oa));
    struct buff_struct *buff = malloc(sizeof(struct buff_struct));
    if (!oa) return 0;
    if (!buff) {
        free(oa);
        return 0;
    }
    *oa = oa_default;
    buff->off = 0;
    buff->buffer = malloc(128);
    buff->len = 128;
    oa->priv = buff;
    return oa;
}

void close_buffer_iarchive(struct iarchive **ia)
{
    free((*ia)->priv);
    free(*ia);
    *ia = 0;
}

void close_buffer_oarchive(struct oarchive **oa, int free_buffer)
{
    if (free_buffer) {
        struct buff_struct *buff = (struct buff_struct *)(*oa)->priv;
        if (buff->buffer) {
            free(buff->buffer);
        }
    }
    free((*oa)->priv);
    free(*oa);
    *oa = 0;
}

char *get_buffer(struct oarchive *oa)
{
    struct buff_struct *buff = oa->priv;
    return buff->buffer;
}
int get_buffer_len(struct oarchive *oa)
{
    struct buff_struct *buff = oa->priv;
    return buff->off;
}
