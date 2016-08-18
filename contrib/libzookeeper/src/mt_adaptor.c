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
#ifndef THREADED
#define THREADED
#endif

#ifndef DLL_EXPORT
#  define USE_STATIC_LIB
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "zk_adaptor.h"
#include "zookeeper_log.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>

#ifndef WIN32
#include <signal.h>
#include <poll.h>
#include <unistd.h>
#include <sys/time.h>
#endif

int zoo_lock_auth(zhandle_t *zh)
{
    return pthread_mutex_lock(&zh->auth_h.lock);
}
int zoo_unlock_auth(zhandle_t *zh)
{
    return pthread_mutex_unlock(&zh->auth_h.lock);
}
int lock_buffer_list(buffer_head_t *l)
{
    return pthread_mutex_lock(&l->lock);
}
int  unlock_buffer_list(buffer_head_t *l)
{
    return pthread_mutex_unlock(&l->lock);
}
int lock_completion_list(completion_head_t *l)
{
    return pthread_mutex_lock(&l->lock);
}
int unlock_completion_list(completion_head_t *l)
{
    pthread_cond_broadcast(&l->cond);
    return pthread_mutex_unlock(&l->lock);
}
struct sync_completion *alloc_sync_completion(void)
{
    struct sync_completion *sc = (struct sync_completion*)calloc(1, sizeof(struct sync_completion));
    if (sc) {
       pthread_cond_init(&sc->cond, 0);
       pthread_mutex_init(&sc->lock, 0);
    }
    return sc;
}
int wait_sync_completion(struct sync_completion *sc)
{
    pthread_mutex_lock(&sc->lock);
    while (!sc->complete) {
        pthread_cond_wait(&sc->cond, &sc->lock);
    }
    pthread_mutex_unlock(&sc->lock);
    return 0;
}

void free_sync_completion(struct sync_completion *sc)
{
    if (sc) {
        pthread_mutex_destroy(&sc->lock);
        pthread_cond_destroy(&sc->cond);
        free(sc);
    }
}

void notify_sync_completion(struct sync_completion *sc)
{
    pthread_mutex_lock(&sc->lock);
    sc->complete = 1;
    pthread_cond_broadcast(&sc->cond);
    pthread_mutex_unlock(&sc->lock);
}

int process_async(int outstanding_sync)
{
    return 0;
}

#ifdef WIN32
unsigned __stdcall do_io( void * );
unsigned __stdcall do_completion( void * );

int handle_error(SOCKET sock, char* message)
{
       LOG_ERROR(("%s. %d",message, WSAGetLastError()));
       closesocket (sock);
       return -1;
}

//--create socket pair for interupting selects.
int create_socket_pair(SOCKET fds[2]) 
{ 
    struct sockaddr_in inaddr; 
    struct sockaddr addr; 
    int yes=1; 
    int len=0;
       
    SOCKET lst=socket(AF_INET, SOCK_STREAM,IPPROTO_TCP); 
    if (lst ==  INVALID_SOCKET ){
       LOG_ERROR(("Error creating socket. %d",WSAGetLastError()));
       return -1;
    }
    memset(&inaddr, 0, sizeof(inaddr)); 
    memset(&addr, 0, sizeof(addr)); 
    inaddr.sin_family = AF_INET; 
    inaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); 
    inaddr.sin_port = 0; //--system assigns the port

    if ( setsockopt(lst,SOL_SOCKET,SO_REUSEADDR,(char*)&yes,sizeof(yes)) == SOCKET_ERROR  ) {
       return handle_error(lst,"Error trying to set socket option.");          
    }  
    if (bind(lst,(struct sockaddr *)&inaddr,sizeof(inaddr)) == SOCKET_ERROR){
       return handle_error(lst,"Error trying to bind socket.");                
    }
    if (listen(lst,1) == SOCKET_ERROR){
       return handle_error(lst,"Error trying to listen on socket.");
    }
    len=sizeof(inaddr); 
    getsockname(lst, &addr,&len); 
    fds[0]=socket(AF_INET, SOCK_STREAM,0); 
    if (connect(fds[0],&addr,len) == SOCKET_ERROR){
       return handle_error(lst, "Error while connecting to socket.");
    }
    if ((fds[1]=accept(lst,0,0)) == INVALID_SOCKET){
       closesocket(fds[0]);
       return handle_error(lst, "Error while accepting socket connection.");
    }
    closesocket(lst);  
    return 0;
} 
#else
void *do_io(void *);
void *do_completion(void *);
#endif


int wakeup_io_thread(zhandle_t *zh);

#ifdef WIN32
static int set_nonblock(SOCKET fd){
    ULONG nonblocking_flag = 1;
    if (ioctlsocket(fd, FIONBIO, &nonblocking_flag) == 0)
        return 1;
    else 
        return -1;
}
#else
static int set_nonblock(int fd){
    long l = fcntl(fd, F_GETFL);
    if(l & O_NONBLOCK) return 0;
    return fcntl(fd, F_SETFL, l | O_NONBLOCK);
}
#endif

void wait_for_others(zhandle_t* zh)
{
    struct adaptor_threads* adaptor=zh->adaptor_priv;
    pthread_mutex_lock(&adaptor->lock);
    while(adaptor->threadsToWait>0) 
        pthread_cond_wait(&adaptor->cond,&adaptor->lock);
    pthread_mutex_unlock(&adaptor->lock);    
}

void notify_thread_ready(zhandle_t* zh)
{
    struct adaptor_threads* adaptor=zh->adaptor_priv;
    pthread_mutex_lock(&adaptor->lock);
    adaptor->threadsToWait--;
    pthread_cond_broadcast(&adaptor->cond);
    while(adaptor->threadsToWait>0) 
        pthread_cond_wait(&adaptor->cond,&adaptor->lock);
    pthread_mutex_unlock(&adaptor->lock);
}


void start_threads(zhandle_t* zh)
{
    int rc = 0;
    struct adaptor_threads* adaptor=zh->adaptor_priv;
    pthread_cond_init(&adaptor->cond,0);
    pthread_mutex_init(&adaptor->lock,0);
    adaptor->threadsToWait=2;  // wait for 2 threads before opening the barrier
    
    // use api_prolog() to make sure zhandle doesn't get destroyed
    // while initialization is in progress
    api_prolog(zh);
    LOG_DEBUG(("starting threads..."));
    rc=pthread_create(&adaptor->io, 0, do_io, zh);
    assert("pthread_create() failed for the IO thread"&&!rc);
    rc=pthread_create(&adaptor->completion, 0, do_completion, zh);
    assert("pthread_create() failed for the completion thread"&&!rc);
    wait_for_others(zh);
    api_epilog(zh, 0);    
}

int adaptor_init(zhandle_t *zh)
{
    pthread_mutexattr_t recursive_mx_attr;
    struct adaptor_threads *adaptor_threads = calloc(1, sizeof(*adaptor_threads));
    if (!adaptor_threads) {
        LOG_ERROR(("Out of memory"));
        return -1;
    }

    /* We use a pipe for interrupting select() in unix/sol and socketpair in windows. */
#ifdef WIN32   
    if (create_socket_pair(adaptor_threads->self_pipe) == -1){
       LOG_ERROR(("Can't make a socket."));
#else
    if(pipe(adaptor_threads->self_pipe)==-1) {
        LOG_ERROR(("Can't make a pipe %d",errno));
#endif
        free(adaptor_threads);
        return -1;
    }
    set_nonblock(adaptor_threads->self_pipe[1]);
    set_nonblock(adaptor_threads->self_pipe[0]);

    pthread_mutex_init(&zh->auth_h.lock,0);

    zh->adaptor_priv = adaptor_threads;
    pthread_mutex_init(&zh->to_process.lock,0);
    pthread_mutex_init(&adaptor_threads->zh_lock,0);
    // to_send must be recursive mutex    
    pthread_mutexattr_init(&recursive_mx_attr);
    pthread_mutexattr_settype(&recursive_mx_attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&zh->to_send.lock,&recursive_mx_attr);
    pthread_mutexattr_destroy(&recursive_mx_attr);
    
    pthread_mutex_init(&zh->sent_requests.lock,0);
    pthread_cond_init(&zh->sent_requests.cond,0);
    pthread_mutex_init(&zh->completions_to_process.lock,0);
    pthread_cond_init(&zh->completions_to_process.cond,0);
    start_threads(zh);
    return 0;
}

void adaptor_finish(zhandle_t *zh)
{
    struct adaptor_threads *adaptor_threads;
    // make sure zh doesn't get destroyed until after we're done here
    api_prolog(zh); 
    adaptor_threads = zh->adaptor_priv;
    if(adaptor_threads==0) {
        api_epilog(zh,0);
        return;
    }

    if(!pthread_equal(adaptor_threads->io,pthread_self())){
        wakeup_io_thread(zh);
        pthread_join(adaptor_threads->io, 0);
    }else
        pthread_detach(adaptor_threads->io);
    
    if(!pthread_equal(adaptor_threads->completion,pthread_self())){
        pthread_mutex_lock(&zh->completions_to_process.lock);
        pthread_cond_broadcast(&zh->completions_to_process.cond);
        pthread_mutex_unlock(&zh->completions_to_process.lock);
        pthread_join(adaptor_threads->completion, 0);
    }else
        pthread_detach(adaptor_threads->completion);
    
    api_epilog(zh,0);
}

void adaptor_destroy(zhandle_t *zh)
{
    struct adaptor_threads *adaptor = zh->adaptor_priv;
    if(adaptor==0) return;
    
    pthread_cond_destroy(&adaptor->cond);
    pthread_mutex_destroy(&adaptor->lock);
    pthread_mutex_destroy(&zh->to_process.lock);
    pthread_mutex_destroy(&zh->to_send.lock);
    pthread_mutex_destroy(&zh->sent_requests.lock);
    pthread_cond_destroy(&zh->sent_requests.cond);
    pthread_mutex_destroy(&zh->completions_to_process.lock);
    pthread_cond_destroy(&zh->completions_to_process.cond);
    pthread_mutex_destroy(&adaptor->zh_lock);

    pthread_mutex_destroy(&zh->auth_h.lock);

    close(adaptor->self_pipe[0]);
    close(adaptor->self_pipe[1]);
    free(adaptor);
    zh->adaptor_priv=0;
}

int wakeup_io_thread(zhandle_t *zh)
{
    struct adaptor_threads *adaptor_threads = zh->adaptor_priv;
    char c=0;
#ifndef WIN32
    return write(adaptor_threads->self_pipe[1],&c,1)==1? ZOK: ZSYSTEMERROR;    
#else
    return send(adaptor_threads->self_pipe[1], &c, 1, 0)==1? ZOK: ZSYSTEMERROR;    
#endif         
}

int adaptor_send_queue(zhandle_t *zh, int timeout)
{
    if(!zh->close_requested)
        return wakeup_io_thread(zh);
    // don't rely on the IO thread to send the messages if the app has
    // requested to close 
    return flush_send_queue(zh, timeout);
}

/* These two are declared here because we will run the event loop
 * and not the client */
#ifdef WIN32
int zookeeper_interest(zhandle_t *zh, SOCKET *fd, int *interest,
        struct timeval *tv);
#else
int zookeeper_interest(zhandle_t *zh, int *fd, int *interest,
        struct timeval *tv);
#endif
int zookeeper_process(zhandle_t *zh, int events);

#ifdef WIN32
unsigned __stdcall do_io( void * v)
#else
void *do_io(void *v)
#endif
{
    zhandle_t *zh = (zhandle_t*)v;
#ifndef WIN32
    struct pollfd fds[2];
    struct adaptor_threads *adaptor_threads = zh->adaptor_priv;

    api_prolog(zh);
    notify_thread_ready(zh);
    LOG_DEBUG(("started IO thread"));
    fds[0].fd=adaptor_threads->self_pipe[0];
    fds[0].events=POLLIN;
    while(!zh->close_requested) {
        struct timeval tv;
        int fd;
        int interest;
        int timeout;
        int maxfd=1;
        int rc;
        
        zookeeper_interest(zh, &fd, &interest, &tv);
        if (fd != -1) {
            fds[1].fd=fd;
            fds[1].events=(interest&ZOOKEEPER_READ)?POLLIN:0;
            fds[1].events|=(interest&ZOOKEEPER_WRITE)?POLLOUT:0;
            maxfd=2;
        }
        timeout=tv.tv_sec * 1000 + (tv.tv_usec/1000);
        
        poll(fds,maxfd,timeout);
        if (fd != -1) {
            interest=(fds[1].revents&POLLIN)?ZOOKEEPER_READ:0;
            interest|=((fds[1].revents&POLLOUT)||(fds[1].revents&POLLHUP))?ZOOKEEPER_WRITE:0;
        }
        if(fds[0].revents&POLLIN){
            // flush the pipe
            char b[128];
            while(read(adaptor_threads->self_pipe[0],b,sizeof(b))==sizeof(b)){}
        }        
#else
    fd_set rfds, wfds, efds;
    struct adaptor_threads *adaptor_threads = zh->adaptor_priv;
    api_prolog(zh);
    notify_thread_ready(zh);
    LOG_DEBUG(("started IO thread"));
    FD_ZERO(&rfds);   FD_ZERO(&wfds);    FD_ZERO(&efds);
    while(!zh->close_requested) {      
        struct timeval tv;
        SOCKET fd;
        SOCKET maxfd=adaptor_threads->self_pipe[0];
        int interest;        
        int rc;
               
       zookeeper_interest(zh, &fd, &interest, &tv);
       if (fd != -1) {
           if (interest&ZOOKEEPER_READ) {
                FD_SET(fd, &rfds);
            } else {
                FD_CLR(fd, &rfds);
            }
           if (interest&ZOOKEEPER_WRITE) {
                FD_SET(fd, &wfds);
            } else {
                FD_CLR(fd, &wfds);
            }                  
        }
       FD_SET( adaptor_threads->self_pipe[0] ,&rfds );        
       rc = select((int)maxfd, &rfds, &wfds, &efds, &tv);
       if (fd != -1) 
       {
           interest = (FD_ISSET(fd, &rfds))? ZOOKEEPER_READ:0;
           interest|= (FD_ISSET(fd, &wfds))? ZOOKEEPER_WRITE:0;
        }
               
       if (FD_ISSET(adaptor_threads->self_pipe[0], &rfds)){
            // flush the pipe/socket
            char b[128];
           while(recv(adaptor_threads->self_pipe[0],b,sizeof(b), 0)==sizeof(b)){}
       }
#endif
        // dispatch zookeeper events
        rc = zookeeper_process(zh, interest);
        // check the current state of the zhandle and terminate 
        // if it is_unrecoverable()
        if(is_unrecoverable(zh))
            break;
    }
    api_epilog(zh, 0);    
    LOG_DEBUG(("IO thread terminated"));
    return 0;
}

#ifdef WIN32
unsigned __stdcall do_completion( void * v)
#else
void *do_completion(void *v)
#endif
{
    zhandle_t *zh = v;
    api_prolog(zh);
    notify_thread_ready(zh);
    LOG_DEBUG(("started completion thread"));
    while(!zh->close_requested) {
        pthread_mutex_lock(&zh->completions_to_process.lock);
        while(!zh->completions_to_process.head && !zh->close_requested) {
            pthread_cond_wait(&zh->completions_to_process.cond, &zh->completions_to_process.lock);
        }
        pthread_mutex_unlock(&zh->completions_to_process.lock);
        process_completions(zh);
    }
    api_epilog(zh, 0);    
    LOG_DEBUG(("completion thread terminated"));
    return 0;
}

int32_t inc_ref_counter(zhandle_t* zh,int i)
{
    int incr=(i<0?-1:(i>0?1:0));
    // fetch_and_add implements atomic post-increment
    int v=fetch_and_add(&zh->ref_counter,incr);
    // inc_ref_counter wants pre-increment
    v+=incr;   // simulate pre-increment
    return v;
}

int32_t fetch_and_add(volatile int32_t* operand, int incr)
{
#ifndef WIN32
    int32_t result;
    asm __volatile__(
         "lock xaddl %0,%1\n"
         : "=r"(result), "=m"(*(int *)operand)
         : "0"(incr)
         : "memory");
   return result;
#else
    volatile int32_t result;
    _asm
    {
        mov eax, operand; //eax = v;
       mov ebx, incr; // ebx = i;
        mov ecx, 0x0; // ecx = 0;
        lock xadd dword ptr [eax], ecx; 
       lock xadd dword ptr [eax], ebx; 
        mov result, ecx; // result = ebx;        
     }
     return result;    
#endif
}

// make sure the static xid is initialized before any threads started
__attribute__((constructor)) int32_t get_xid()
{
    static int32_t xid = -1;
    if (xid == -1) {
        xid = time(0);
    }
    return fetch_and_add(&xid,1);
}

int enter_critical(zhandle_t* zh)
{
    struct adaptor_threads *adaptor = zh->adaptor_priv;
    if (adaptor) {
        return pthread_mutex_lock(&adaptor->zh_lock);
    } else {
        return 0;
    }
}

int leave_critical(zhandle_t* zh)
{
    struct adaptor_threads *adaptor = zh->adaptor_priv;
    if (adaptor) {
        return pthread_mutex_unlock(&adaptor->zh_lock);
    } else {
        return 0;
    }
}
