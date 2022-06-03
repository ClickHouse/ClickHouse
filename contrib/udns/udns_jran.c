/* udns_jran.c: small non-cryptographic random number generator
 * taken from http://burtleburtle.net/bob/rand/smallprng.html
 * by Bob Jenkins, Public domain.
 */

#include "udns.h"

#define rot32(x,k) (((x) << (k)) | ((x) >> (32-(k))))
#define rot64(x,k) (((x) << (k)) | ((x) >> (64-(k))))
#define tr32(x) ((x)&0xffffffffu)

unsigned udns_jranval(struct udns_jranctx *x) {
  /* This routine can be made to work with either 32 or 64bit words -
   * if JRAN_32_64 is defined when compiling the file.
   * We use if() instead of #if since there's no good
   * portable way to check sizeof() in preprocessor without
   * introducing some ugly configure-time checks.
   * Most compilers will optimize the wrong branches away anyway.
   * By default it assumes 32bit integers
   */
#ifdef JRAN_32_64
  if (sizeof(unsigned) == 4) {
#endif
    unsigned e = tr32(x->a - rot32(x->b, 27));
    x->a = tr32(x->b ^ rot32(x->c, 17));
    x->b = tr32(x->c + x->d);
    x->c = tr32(x->d + e);
    x->d = tr32(e + x->a);
#ifdef JRAN_32_64
  }
  else if (sizeof(unsigned) == 8) { /* assuming it's 64bits */
    unsigned e = x->a - rot64(x->b, 7);
    x->a = x->b ^ rot64(x->c, 13);
    x->b = x->c + rot64(x->d, 37);
    x->c = x->d + e;
    x->d = e + x->a;
  }
  else {
    unsigned e = 0;
    x->d = 1/e; /* bail */
  }
#endif
  return x->d;
}

void udns_jraninit(struct udns_jranctx *x, unsigned seed) {
  unsigned i;
  x->a = 0xf1ea5eed;
  x->b = x->c = x->d = seed;
  for (i = 0; i < 20; ++i)
     (void)udns_jranval(x);
}
