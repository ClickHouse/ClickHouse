/* Delta.h -- Delta converter
2009-04-15 : Igor Pavlov : Public domain */

#ifndef __DELTA_H
#define __DELTA_H

#include "Types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DELTA_STATE_SIZE 256

void Delta_Init(Byte *state);
void Delta_Encode(Byte *state, unsigned delta, Byte *data, SizeT size);
void Delta_Decode(Byte *state, unsigned delta, Byte *data, SizeT size);

#ifdef __cplusplus
}
#endif

#endif
