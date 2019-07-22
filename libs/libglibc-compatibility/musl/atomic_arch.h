#include <stdint.h>

#define a_cas a_cas
static inline int a_cas(volatile int *p, int t, int s)
{
	__asm__ __volatile__ (
		"lock ; cmpxchg %3, %1"
		: "=a"(t), "=m"(*p) : "a"(t), "r"(s) : "memory" );
	return t;
}

#define a_cas_p a_cas_p
static inline void *a_cas_p(volatile void *p, void *t, void *s)
{
	__asm__( "lock ; cmpxchg %3, %1"
		: "=a"(t), "=m"(*(void *volatile *)p)
		: "a"(t), "r"(s) : "memory" );
	return t;
}

#define a_swap a_swap
static inline int a_swap(volatile int *p, int v)
{
	__asm__ __volatile__(
		"xchg %0, %1"
		: "=r"(v), "=m"(*p) : "0"(v) : "memory" );
	return v;
}

#define a_fetch_add a_fetch_add
static inline int a_fetch_add(volatile int *p, int v)
{
	__asm__ __volatile__(
		"lock ; xadd %0, %1"
		: "=r"(v), "=m"(*p) : "0"(v) : "memory" );
	return v;
}

#define a_and a_and
static inline void a_and(volatile int *p, int v)
{
	__asm__ __volatile__(
		"lock ; and %1, %0"
		: "=m"(*p) : "r"(v) : "memory" );
}

#define a_or a_or
static inline void a_or(volatile int *p, int v)
{
	__asm__ __volatile__(
		"lock ; or %1, %0"
		: "=m"(*p) : "r"(v) : "memory" );
}

#define a_and_64 a_and_64
static inline void a_and_64(volatile uint64_t *p, uint64_t v)
{
	__asm__ __volatile(
		"lock ; and %1, %0"
		 : "=m"(*p) : "r"(v) : "memory" );
}

#define a_or_64 a_or_64
static inline void a_or_64(volatile uint64_t *p, uint64_t v)
{
	__asm__ __volatile__(
		"lock ; or %1, %0"
		 : "=m"(*p) : "r"(v) : "memory" );
}

#define a_inc a_inc
static inline void a_inc(volatile int *p)
{
	__asm__ __volatile__(
		"lock ; incl %0"
		: "=m"(*p) : "m"(*p) : "memory" );
}

#define a_dec a_dec
static inline void a_dec(volatile int *p)
{
	__asm__ __volatile__(
		"lock ; decl %0"
		: "=m"(*p) : "m"(*p) : "memory" );
}

#define a_store a_store
static inline void a_store(volatile int *p, int x)
{
	__asm__ __volatile__(
		"mov %1, %0 ; lock ; orl $0,(%%rsp)"
		: "=m"(*p) : "r"(x) : "memory" );
}

#define a_barrier a_barrier
static inline void a_barrier()
{
	__asm__ __volatile__( "" : : : "memory" );
}

#define a_spin a_spin
static inline void a_spin()
{
	__asm__ __volatile__( "pause" : : : "memory" );
}

#define a_crash a_crash
static inline void a_crash()
{
	__asm__ __volatile__( "hlt" : : : "memory" );
}

#define a_ctz_64 a_ctz_64
static inline int a_ctz_64(uint64_t x)
{
	__asm__( "bsf %1,%0" : "=r"(x) : "r"(x) );
	return x;
}

#define a_clz_64 a_clz_64
static inline int a_clz_64(uint64_t x)
{
	__asm__( "bsr %1,%0 ; xor $63,%0" : "=r"(x) : "r"(x) );
	return x;
}
