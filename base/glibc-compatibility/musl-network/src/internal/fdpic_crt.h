#include <stdint.h>
#include <features.h>

hidden void *__fdpic_fixup(void *map, uintptr_t *a, uintptr_t *z)
{
	/* If map is a null pointer, the program was loaded by a
	 * non-FDPIC-aware ELF loader, and fixups are not needed,
	 * but the value for the GOT pointer is. */
	if (!map) return (void *)z[-1];

	struct {
		unsigned short version, nsegs;
		struct fdpic_loadseg {
			uintptr_t addr, p_vaddr, p_memsz;
		} segs[];
	} *lm = map;
	int nsegs = lm->nsegs, rseg = 0, vseg = 0;
	for (;;) {
		while (*a-lm->segs[rseg].p_vaddr >= lm->segs[rseg].p_memsz)
			if (++rseg == nsegs) rseg = 0;
		uintptr_t *r = (uintptr_t *)
			(*a + lm->segs[rseg].addr - lm->segs[rseg].p_vaddr);
		if (++a == z) return r;
		while (*r-lm->segs[vseg].p_vaddr >= lm->segs[vseg].p_memsz)
			if (++vseg == nsegs) vseg = 0;
		*r += lm->segs[vseg].addr - lm->segs[vseg].p_vaddr;
	}
}
