#ifndef _SYS_SYSMACROS_H
#define _SYS_SYSMACROS_H

#define major(x) \
	((unsigned)( (((x)>>31>>1) & 0xfffff000) | (((x)>>8) & 0x00000fff) ))
#define minor(x) \
	((unsigned)( (((x)>>12) & 0xffffff00) | ((x) & 0x000000ff) ))

#define makedev(x,y) ( \
        (((x)&0xfffff000ULL) << 32) | \
	(((x)&0x00000fffULL) << 8) | \
        (((y)&0xffffff00ULL) << 12) | \
	(((y)&0x000000ffULL)) )

#endif
