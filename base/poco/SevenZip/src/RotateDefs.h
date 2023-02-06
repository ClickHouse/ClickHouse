/* RotateDefs.h -- Rotate functions
2009-02-07 : Igor Pavlov : Public domain */

#ifndef __ROTATE_DEFS_H
#define __ROTATE_DEFS_H

#ifdef _MSC_VER

#include <stdlib.h>
#define rotlFixed(x, n) _rotl((x), (n))
#define rotrFixed(x, n) _rotr((x), (n))

#else

#define rotlFixed(x, n) (((x) << (n)) | ((x) >> (32 - (n))))
#define rotrFixed(x, n) (((x) >> (n)) | ((x) << (32 - (n))))

#endif

#endif
