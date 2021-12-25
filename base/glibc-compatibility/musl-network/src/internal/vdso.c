#include <elf.h>
#include <link.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include "libc.h"
#include "syscall.h"

#ifdef VDSO_USEFUL

#if ULONG_MAX == 0xffffffff
typedef Elf32_Ehdr Ehdr;
typedef Elf32_Phdr Phdr;
typedef Elf32_Sym Sym;
typedef Elf32_Verdef Verdef;
typedef Elf32_Verdaux Verdaux;
#else
typedef Elf64_Ehdr Ehdr;
typedef Elf64_Phdr Phdr;
typedef Elf64_Sym Sym;
typedef Elf64_Verdef Verdef;
typedef Elf64_Verdaux Verdaux;
#endif

static int checkver(Verdef *def, int vsym, const char *vername, char *strings)
{
	vsym &= 0x7fff;
	for (;;) {
		if (!(def->vd_flags & VER_FLG_BASE)
		  && (def->vd_ndx & 0x7fff) == vsym)
			break;
		if (def->vd_next == 0)
			return 0;
		def = (Verdef *)((char *)def + def->vd_next);
	}
	Verdaux *aux = (Verdaux *)((char *)def + def->vd_aux);
	return !strcmp(vername, strings + aux->vda_name);
}

#define OK_TYPES (1<<STT_NOTYPE | 1<<STT_OBJECT | 1<<STT_FUNC | 1<<STT_COMMON)
#define OK_BINDS (1<<STB_GLOBAL | 1<<STB_WEAK | 1<<STB_GNU_UNIQUE)

void *__vdsosym(const char *vername, const char *name)
{
	size_t i;
	for (i=0; libc.auxv[i] != AT_SYSINFO_EHDR; i+=2)
		if (!libc.auxv[i]) return 0;
	if (!libc.auxv[i+1]) return 0;
	Ehdr *eh = (void *)libc.auxv[i+1];
	Phdr *ph = (void *)((char *)eh + eh->e_phoff);
	size_t *dynv=0, base=-1;
	for (i=0; i<eh->e_phnum; i++, ph=(void *)((char *)ph+eh->e_phentsize)) {
		if (ph->p_type == PT_LOAD)
			base = (size_t)eh + ph->p_offset - ph->p_vaddr;
		else if (ph->p_type == PT_DYNAMIC)
			dynv = (void *)((char *)eh + ph->p_offset);
	}
	if (!dynv || base==(size_t)-1) return 0;

	char *strings = 0;
	Sym *syms = 0;
	Elf_Symndx *hashtab = 0;
	uint16_t *versym = 0;
	Verdef *verdef = 0;
	
	for (i=0; dynv[i]; i+=2) {
		void *p = (void *)(base + dynv[i+1]);
		switch(dynv[i]) {
		case DT_STRTAB: strings = p; break;
		case DT_SYMTAB: syms = p; break;
		case DT_HASH: hashtab = p; break;
		case DT_VERSYM: versym = p; break;
		case DT_VERDEF: verdef = p; break;
		}
	}	

	if (!strings || !syms || !hashtab) return 0;
	if (!verdef) versym = 0;

	for (i=0; i<hashtab[1]; i++) {
		if (!(1<<(syms[i].st_info&0xf) & OK_TYPES)) continue;
		if (!(1<<(syms[i].st_info>>4) & OK_BINDS)) continue;
		if (!syms[i].st_shndx) continue;
		if (strcmp(name, strings+syms[i].st_name)) continue;
		if (versym && !checkver(verdef, versym[i], vername, strings))
			continue;
		return (void *)(base + syms[i].st_value);
	}

	return 0;
}

#endif
