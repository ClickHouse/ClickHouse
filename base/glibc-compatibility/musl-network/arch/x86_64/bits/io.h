static __inline void outb(unsigned char __val, unsigned short __port)
{
	__asm__ volatile ("outb %0,%1" : : "a" (__val), "dN" (__port));
}

static __inline void outw(unsigned short __val, unsigned short __port)
{
	__asm__ volatile ("outw %0,%1" : : "a" (__val), "dN" (__port));
}

static __inline void outl(unsigned int __val, unsigned short __port)
{
	__asm__ volatile ("outl %0,%1" : : "a" (__val), "dN" (__port));
}

static __inline unsigned char inb(unsigned short __port)
{
	unsigned char __val;
	__asm__ volatile ("inb %1,%0" : "=a" (__val) : "dN" (__port));
	return __val;
}

static __inline unsigned short inw(unsigned short __port)
{
	unsigned short __val;
	__asm__ volatile ("inw %1,%0" : "=a" (__val) : "dN" (__port));
	return __val;
}

static __inline unsigned int inl(unsigned short __port)
{
	unsigned int __val;
	__asm__ volatile ("inl %1,%0" : "=a" (__val) : "dN" (__port));
	return __val;
}

static __inline void outsb(unsigned short __port, const void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; outsb"
		      : "+S" (__buf), "+c" (__n)
		      : "d" (__port));
}

static __inline void outsw(unsigned short __port, const void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; outsw"
		      : "+S" (__buf), "+c" (__n)
		      : "d" (__port));
}

static __inline void outsl(unsigned short __port, const void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; outsl"
		      : "+S" (__buf), "+c"(__n)
		      : "d" (__port));
}

static __inline void insb(unsigned short __port, void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; insb"
		      : "+D" (__buf), "+c" (__n)
		      : "d" (__port));
}

static __inline void insw(unsigned short __port, void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; insw"
		      : "+D" (__buf), "+c" (__n)
		      : "d" (__port));
}

static __inline void insl(unsigned short __port, void *__buf, unsigned long __n)
{
	__asm__ volatile ("cld; rep; insl"
		      : "+D" (__buf), "+c" (__n)
		      : "d" (__port));
}
