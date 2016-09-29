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

#include "zookeeper_log.h"
#ifndef WIN32
#include <unistd.h>
#endif

#include <stdarg.h>
#include <time.h>

#define TIME_NOW_BUF_SIZE 1024
#define FORMAT_LOG_BUF_SIZE 4096

#ifdef THREADED
#ifndef WIN32
#include <pthread.h>
#else 
#include "winport.h"
#endif

static pthread_key_t time_now_buffer;
static pthread_key_t format_log_msg_buffer;

void freeBuffer(void* p){
    if(p) free(p);
}

__attribute__((constructor)) void prepareTSDKeys() {
    pthread_key_create (&time_now_buffer, freeBuffer);
    pthread_key_create (&format_log_msg_buffer, freeBuffer);
}

char* getTSData(pthread_key_t key,int size){
    char* p=pthread_getspecific(key);
    if(p==0){
        int res;
        p=calloc(1,size);
        res=pthread_setspecific(key,p);
        if(res!=0){
            fprintf(stderr,"Failed to set TSD key: %d",res);
        }
    }
    return p;
}

char* get_time_buffer(){
    return getTSData(time_now_buffer,TIME_NOW_BUF_SIZE);
}

char* get_format_log_buffer(){  
    return getTSData(format_log_msg_buffer,FORMAT_LOG_BUF_SIZE);
}
#else
char* get_time_buffer(){
    static char buf[TIME_NOW_BUF_SIZE];
    return buf;    
}

char* get_format_log_buffer(){
    static char buf[FORMAT_LOG_BUF_SIZE];
    return buf;
}

#endif

ZooLogLevel logLevel=ZOO_LOG_LEVEL_INFO;

static FILE* logStream=0;
FILE* getLogStream(){
    if(logStream==0)
        logStream=stderr;
    return logStream;
}

void zoo_set_log_stream(FILE* stream){
    logStream=stream;
}

static const char* time_now(char* now_str){
    struct timeval tv;
    struct tm lt;
    time_t now = 0;
    size_t len = 0;
    
    gettimeofday(&tv,0);

    now = tv.tv_sec;
    localtime_r(&now, &lt);

    // clone the format used by log4j ISO8601DateFormat
    // specifically: "yyyy-MM-dd HH:mm:ss,SSS"

    len = strftime(now_str, TIME_NOW_BUF_SIZE,
                          "%Y-%m-%d %H:%M:%S",
                          &lt);

    len += snprintf(now_str + len,
                    TIME_NOW_BUF_SIZE - len,
                    ",%03d",
                    (int)(tv.tv_usec/1000));

    return now_str;
}

void log_message(ZooLogLevel curLevel,int line,const char* funcName,
    const char* message)
{
    static const char* dbgLevelStr[]={"ZOO_INVALID","ZOO_ERROR","ZOO_WARN",
            "ZOO_INFO","ZOO_DEBUG"};
    static pid_t pid=0;
#ifdef WIN32
    char timebuf [TIME_NOW_BUF_SIZE];
#endif
    if(pid==0)pid=getpid();
#ifndef THREADED
    // pid_t is long on Solaris
    fprintf(LOGSTREAM, "%s:%ld:%s@%s@%d: %s\n", time_now(get_time_buffer()),(long)pid,
            dbgLevelStr[curLevel],funcName,line,message);
#else
#ifdef WIN32
    fprintf(LOGSTREAM, "%s:%d(0x%lx):%s@%s@%d: %s\n", time_now(timebuf),pid,
            (unsigned long int)(pthread_self().thread_id),
            dbgLevelStr[curLevel],funcName,line,message);      
#else
    fprintf(LOGSTREAM, "%s:%ld(0x%lx):%s@%s@%d: %s\n", time_now(get_time_buffer()),(long)pid,
            (unsigned long int)pthread_self(),
            dbgLevelStr[curLevel],funcName,line,message);      
#endif
#endif
    fflush(LOGSTREAM);
}

const char* format_log_message(const char* format,...)
{
    va_list va;
    char* buf=get_format_log_buffer();
    if(!buf)
        return "format_log_message: Unable to allocate memory buffer";
    
    va_start(va,format);
    vsnprintf(buf, FORMAT_LOG_BUF_SIZE-1,format,va);
    va_end(va); 
    return buf;
}

void zoo_set_debug_level(ZooLogLevel level)
{
    if(level==0){
        // disable logging (unit tests do this)
        logLevel=(ZooLogLevel)0;
        return;
    }
    if(level<ZOO_LOG_LEVEL_ERROR)level=ZOO_LOG_LEVEL_ERROR;
    if(level>ZOO_LOG_LEVEL_DEBUG)level=ZOO_LOG_LEVEL_DEBUG;
    logLevel=level;
}

