/* 7zAlloc.c -- Allocation functions
2010-10-29 : Igor Pavlov : Public domain */

#include "7zAlloc.h"

/* #define _SZ_ALLOC_DEBUG */
/* use _SZ_ALLOC_DEBUG to debug alloc/free operations */

#ifdef _SZ_ALLOC_DEBUG

#ifdef _WIN32
#include <windows.h>
#endif

#include <stdio.h>
int g_allocCount = 0;
int g_allocCountTemp = 0;

#endif

void *SzAlloc(void *p, size_t size)
{
  p = p;
  if (size == 0)
    return 0;
  #ifdef _SZ_ALLOC_DEBUG
  fprintf(stderr, "\nAlloc %10d bytes; count = %10d", size, g_allocCount);
  g_allocCount++;
  #endif
  return malloc(size);
}

void SzFree(void *p, void *address)
{
  p = p;
  #ifdef _SZ_ALLOC_DEBUG
  if (address != 0)
  {
    g_allocCount--;
    fprintf(stderr, "\nFree; count = %10d", g_allocCount);
  }
  #endif
  free(address);
}

void *SzAllocTemp(void *p, size_t size)
{
  p = p;
  if (size == 0)
    return 0;
  #ifdef _SZ_ALLOC_DEBUG
  fprintf(stderr, "\nAlloc_temp %10d bytes;  count = %10d", size, g_allocCountTemp);
  g_allocCountTemp++;
  #ifdef _WIN32
  return HeapAlloc(GetProcessHeap(), 0, size);
  #endif
  #endif
  return malloc(size);
}

void SzFreeTemp(void *p, void *address)
{
  p = p;
  #ifdef _SZ_ALLOC_DEBUG
  if (address != 0)
  {
    g_allocCountTemp--;
    fprintf(stderr, "\nFree_temp; count = %10d", g_allocCountTemp);
  }
  #ifdef _WIN32
  HeapFree(GetProcessHeap(), 0, address);
  return;
  #endif
  #endif
  free(address);
}
