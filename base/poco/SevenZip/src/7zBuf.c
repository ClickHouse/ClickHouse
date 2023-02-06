/* 7zBuf.c -- Byte Buffer
2008-03-28
Igor Pavlov
Public domain */

#include "7zBuf.h"

void Buf_Init(CBuf *p)
{
  p->data = 0;
  p->size = 0;
}

int Buf_Create(CBuf *p, size_t size, ISzAlloc *alloc)
{
  p->size = 0;
  if (size == 0)
  {
    p->data = 0;
    return 1;
  }
  p->data = (Byte *)alloc->Alloc(alloc, size);
  if (p->data != 0)
  {
    p->size = size;
    return 1;
  }
  return 0;
}

void Buf_Free(CBuf *p, ISzAlloc *alloc)
{
  alloc->Free(alloc, p->data);
  p->data = 0;
  p->size = 0;
}
