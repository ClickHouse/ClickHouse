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

#ifndef ZK_ADAPTOR_H_
#define ZK_ADAPTOR_H_
#include <zookeeper.jute.h>
#ifdef THREADED
#ifndef WIN32
#include <pthread.h>
#else
#include "winport.h"
#endif
#endif
#include "zookeeper.h"
#include "zk_hashtable.h"

/* predefined xid's values recognized as special by the server */
#define WATCHER_EVENT_XID -1 
#define PING_XID -2
#define AUTH_XID -4
#define SET_WATCHES_XID -8

/* zookeeper state constants */
#define EXPIRED_SESSION_STATE_DEF -112
#define AUTH_FAILED_STATE_DEF -113
#define CONNECTING_STATE_DEF 1
#define ASSOCIATING_STATE_DEF 2
#define CONNECTED_STATE_DEF 3
#define NOTCONNECTED_STATE_DEF 999

/* zookeeper event type constants */
#define CREATED_EVENT_DEF 1
#define DELETED_EVENT_DEF 2
#define CHANGED_EVENT_DEF 3
#define CHILD_EVENT_DEF 4
#define SESSION_EVENT_DEF -1
#define NOTWATCHING_EVENT_DEF -2

#ifdef __cplusplus
extern "C" {
#endif

struct _buffer_list;
struct _completion_list;

typedef struct _buffer_head {
    struct _buffer_list *volatile head;
    struct _buffer_list *last;
#ifdef THREADED
    pthread_mutex_t lock;
#endif
} buffer_head_t;

typedef struct _completion_head {
    struct _completion_list *volatile head;
    struct _completion_list *last;
#ifdef THREADED
    pthread_cond_t cond;
    pthread_mutex_t lock;
#endif
} completion_head_t;

int lock_buffer_list(buffer_head_t *l);
int unlock_buffer_list(buffer_head_t *l);
int lock_completion_list(completion_head_t *l);
int unlock_completion_list(completion_head_t *l);

struct sync_completion {
    int rc;
    union {
        struct {
            char *str;
            int str_len;
        } str;
        struct Stat stat;
        struct {
            char *buffer;
            int buff_len;
            struct Stat stat;
        } data;
        struct {
            struct ACL_vector acl;
            struct Stat stat;
        } acl;
        struct String_vector strs2;
        struct {
            struct String_vector strs2;
            struct Stat stat2;
        } strs_stat;
    } u;
    int complete;
#ifdef THREADED
    pthread_cond_t cond;
    pthread_mutex_t lock;
#endif
};

typedef struct _auth_info {
    int state; /* 0=>inactive, >0 => active */
    char* scheme;
    struct buffer auth;
    void_completion_t completion;
    const char* data;
    struct _auth_info *next;
} auth_info;

/**
 * This structure represents a packet being read or written.
 */
typedef struct _buffer_list {
    char *buffer;
    int len; /* This represents the length of sizeof(header) + length of buffer */
    int curr_offset; /* This is the offset into the header followed by offset into the buffer */
    struct _buffer_list *next;
} buffer_list_t;

/* the size of connect request */
#define HANDSHAKE_REQ_SIZE 44
/* connect request */
struct connect_req {
    int32_t protocolVersion;
    int64_t lastZxidSeen;
    int32_t timeOut;
    int64_t sessionId;
    int32_t passwd_len;
    char passwd[16];
};

/* the connect response */
struct prime_struct {
    int32_t len;
    int32_t protocolVersion;
    int32_t timeOut;
    int64_t sessionId;
    int32_t passwd_len;
    char passwd[16];
}; 

#ifdef THREADED
/* this is used by mt_adaptor internally for thread management */
struct adaptor_threads {
     pthread_t io;
     pthread_t completion;
     int threadsToWait;         // barrier
     pthread_cond_t cond;       // barrier's conditional
     pthread_mutex_t lock;      // ... and a lock
     pthread_mutex_t zh_lock;   // critical section lock
#ifdef WIN32
     SOCKET self_pipe[2];
#else
     int self_pipe[2];
#endif
};
#endif

/** the auth list for adding auth */
typedef struct _auth_list_head {
     auth_info *auth;
#ifdef THREADED
     pthread_mutex_t lock;
#endif
} auth_list_head_t;

/**
 * This structure represents the connection to zookeeper.
 */

struct _zhandle {
#ifdef WIN32
    SOCKET fd; /* the descriptor used to talk to zookeeper */
#else
    int fd; /* the descriptor used to talk to zookeeper */
#endif
    char *hostname; /* the hostname of zookeeper */
    struct sockaddr_storage *addrs; /* the addresses that correspond to the hostname */
    int addrs_count; /* The number of addresses in the addrs array */
    watcher_fn watcher; /* the registered watcher */
    struct timeval last_recv; /* The time that the last message was received */
    struct timeval last_send; /* The time that the last message was sent */
    struct timeval last_ping; /* The time that the last PING was sent */
    struct timeval next_deadline; /* The time of the next deadline */
    int recv_timeout; /* The maximum amount of time that can go by without 
     receiving anything from the zookeeper server */
    buffer_list_t *input_buffer; /* the current buffer being read in */
    buffer_head_t to_process; /* The buffers that have been read and are ready to be processed. */
    buffer_head_t to_send; /* The packets queued to send */
    completion_head_t sent_requests; /* The outstanding requests */
    completion_head_t completions_to_process; /* completions that are ready to run */
    int connect_index; /* The index of the address to connect to */
    clientid_t client_id;
    long long last_zxid;
    int outstanding_sync; /* Number of outstanding synchronous requests */
    struct _buffer_list primer_buffer; /* The buffer used for the handshake at the start of a connection */
    struct prime_struct primer_storage; /* the connect response */
    char primer_storage_buffer[40]; /* the true size of primer_storage */
    volatile int state;
    void *context;
    auth_list_head_t auth_h; /* authentication data list */
    /* zookeeper_close is not reentrant because it de-allocates the zhandler. 
     * This guard variable is used to defer the destruction of zhandle till 
     * right before top-level API call returns to the caller */
    int32_t ref_counter;
    volatile int close_requested;
    void *adaptor_priv;
    /* Used for debugging only: non-zero value indicates the time when the zookeeper_process
     * call returned while there was at least one unprocessed server response 
     * available in the socket recv buffer */
    struct timeval socket_readable;
    
    zk_hashtable* active_node_watchers;   
    zk_hashtable* active_exist_watchers;
    zk_hashtable* active_child_watchers;
    /** used for chroot path at the client side **/
    char *chroot;
};


int adaptor_init(zhandle_t *zh);
void adaptor_finish(zhandle_t *zh);
void adaptor_destroy(zhandle_t *zh);
struct sync_completion *alloc_sync_completion(void);
int wait_sync_completion(struct sync_completion *sc);
void free_sync_completion(struct sync_completion *sc);
void notify_sync_completion(struct sync_completion *sc);
int adaptor_send_queue(zhandle_t *zh, int timeout);
int process_async(int outstanding_sync);
void process_completions(zhandle_t *zh);
int flush_send_queue(zhandle_t*zh, int timeout);
char* sub_string(zhandle_t *zh, const char* server_path);
void free_duplicate_path(const char* free_path, const char* path);
int zoo_lock_auth(zhandle_t *zh);
int zoo_unlock_auth(zhandle_t *zh);

// critical section guards
int enter_critical(zhandle_t* zh);
int leave_critical(zhandle_t* zh);
// zhandle object reference counting
void api_prolog(zhandle_t* zh);
int api_epilog(zhandle_t *zh, int rc);
int32_t get_xid();
// returns the new value of the ref counter
int32_t inc_ref_counter(zhandle_t* zh,int i);

#ifdef THREADED
// atomic post-increment
int32_t fetch_and_add(volatile int32_t* operand, int incr);
// in mt mode process session event asynchronously by the completion thread
#define PROCESS_SESSION_EVENT(zh,newstate) queue_session_event(zh,newstate)
#else
// in single-threaded mode process session event immediately
//#define PROCESS_SESSION_EVENT(zh,newstate) deliverWatchers(zh,ZOO_SESSION_EVENT,newstate,0)
#define PROCESS_SESSION_EVENT(zh,newstate) queue_session_event(zh,newstate)
#endif

#ifdef __cplusplus
}
#endif

#endif /*ZK_ADAPTOR_H_*/


