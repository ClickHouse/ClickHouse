/* Delta.c -- Delta converter
2009-05-26 : Igor Pavlov : Public domain */

#include "Delta.h"

void Delta_Init(Byte *state)
{
  unsigned i;
  for (i = 0; i < DELTA_STATE_SIZE; i++)
    state[i] = 0;
}

static void MyMemCpy(Byte *dest, const Byte *src, unsigned size)
{
  unsigned i;
  for (i = 0; i < size; i++)
    dest[i] = src[i];
}

void Delta_Encode(Byte *state, unsigned delta, Byte *data, SizeT size)
{
  Byte buf[DELTA_STATE_SIZE];
  unsigned j = 0;
  MyMemCpy(buf, state, delta);
  {
    SizeT i;
    for (i = 0; i < size;)
    {
      for (j = 0; j < delta && i < size; i++, j++)
      {
        Byte b = data[i];
        data[i] = (Byte)(b - buf[j]);
        buf[j] = b;
      }
    }
  }
  if (j == delta)
    j = 0;
  MyMemCpy(state, buf + j, delta - j);
  MyMemCpy(state + delta - j, buf, j);
}

void Delta_Decode(Byte *state, unsigned delta, Byte *data, SizeT size)
{
  Byte buf[DELTA_STATE_SIZE];
  unsigned j = 0;
  MyMemCpy(buf, state, delta);
  {
    SizeT i;
    for (i = 0; i < size;)
    {
      for (j = 0; j < delta && i < size; i++, j++)
      {
        buf[j] = data[i] = (Byte)(buf[j] + data[i]);
      }
    }
  }
  if (j == delta)
    j = 0;
  MyMemCpy(state, buf + j, delta - j);
  MyMemCpy(state + delta - j, buf, j);
}
