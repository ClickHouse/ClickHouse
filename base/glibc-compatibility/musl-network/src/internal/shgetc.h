#include "stdio_impl.h"

/* Scan helper "stdio" functions for use by scanf-family and strto*-family
 * functions. These accept either a valid stdio FILE, or a minimal pseudo
 * FILE whose buffer pointers point into a null-terminated string. In the
 * latter case, the sh_fromstring macro should be used to setup the FILE;
 * the rest of the structure can be left uninitialized.
 *
 * To begin using these functions, shlim must first be called on the FILE
 * to set a field width limit, or 0 for no limit. For string pseudo-FILEs,
 * a nonzero limit is not valid and produces undefined behavior. After that,
 * shgetc, shunget, and shcnt are valid as long as no other stdio functions
 * are called on the stream.
 *
 * When used with a real FILE object, shunget has only one byte of pushback
 * available. Further shunget (up to a limit of the stdio UNGET buffer size)
 * will adjust the position but will not restore the data to be read again.
 * This functionality is needed for the wcsto*-family functions, where it's
 * okay because the FILE will be discarded immediately anyway. When used
 * with string pseudo-FILEs, shunget has unlimited pushback, back to the
 * beginning of the string. */

hidden void __shlim(FILE *, off_t);
hidden int __shgetc(FILE *);

#define shcnt(f) ((f)->shcnt + ((f)->rpos - (f)->buf))
#define shlim(f, lim) __shlim((f), (lim))
#define shgetc(f) (((f)->rpos != (f)->shend) ? *(f)->rpos++ : __shgetc(f))
#define shunget(f) ((f)->shlim>=0 ? (void)(f)->rpos-- : (void)0)

#define sh_fromstring(f, s) \
	((f)->buf = (f)->rpos = (void *)(s), (f)->rend = (void*)-1)
