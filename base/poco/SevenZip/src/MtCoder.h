/* MtCoder.h -- Multi-thread Coder
2009-11-19 : Igor Pavlov : Public domain */

#ifndef __MT_CODER_H
#define __MT_CODER_H

#include "Threads.h"

EXTERN_C_BEGIN

typedef struct
{
  CThread thread;
  CAutoResetEvent startEvent;
  CAutoResetEvent finishedEvent;
  int stop;
  
  THREAD_FUNC_TYPE func;
  LPVOID param;
  THREAD_FUNC_RET_TYPE res;
} CLoopThread;

void LoopThread_Construct(CLoopThread *p);
void LoopThread_Close(CLoopThread *p);
WRes LoopThread_Create(CLoopThread *p);
WRes LoopThread_StopAndWait(CLoopThread *p);
WRes LoopThread_StartSubThread(CLoopThread *p);
WRes LoopThread_WaitSubThread(CLoopThread *p);

#ifndef _7ZIP_ST
#define NUM_MT_CODER_THREADS_MAX 32
#else
#define NUM_MT_CODER_THREADS_MAX 1
#endif

typedef struct
{
  UInt64 totalInSize;
  UInt64 totalOutSize;
  ICompressProgress *progress;
  SRes res;
  CCriticalSection cs;
  UInt64 inSizes[NUM_MT_CODER_THREADS_MAX];
  UInt64 outSizes[NUM_MT_CODER_THREADS_MAX];
} CMtProgress;

SRes MtProgress_Set(CMtProgress *p, unsigned index, UInt64 inSize, UInt64 outSize);

struct _CMtCoder;

typedef struct
{
  struct _CMtCoder *mtCoder;
  Byte *outBuf;
  size_t outBufSize;
  Byte *inBuf;
  size_t inBufSize;
  unsigned index;
  CLoopThread thread;

  Bool stopReading;
  Bool stopWriting;
  CAutoResetEvent canRead;
  CAutoResetEvent canWrite;
} CMtThread;

typedef struct
{
  SRes (*Code)(void *p, unsigned index, Byte *dest, size_t *destSize,
      const Byte *src, size_t srcSize, int finished);
} IMtCoderCallback;

typedef struct _CMtCoder
{
  size_t blockSize;
  size_t destBlockSize;
  unsigned numThreads;
  
  ISeqInStream *inStream;
  ISeqOutStream *outStream;
  ICompressProgress *progress;
  ISzAlloc *alloc;

  IMtCoderCallback *mtCallback;
  CCriticalSection cs;
  SRes res;

  CMtProgress mtProgress;
  CMtThread threads[NUM_MT_CODER_THREADS_MAX];
} CMtCoder;

void MtCoder_Construct(CMtCoder* p);
void MtCoder_Destruct(CMtCoder* p);
SRes MtCoder_Code(CMtCoder *p);

EXTERN_C_END

#endif
