#ifndef _LINK_H
#define _LINK_H

#ifdef __cplusplus
extern "C" {
#endif

#include <elf.h>
#define __NEED_size_t
#define __NEED_uint32_t
#include <bits/alltypes.h>

#if UINTPTR_MAX > 0xffffffff
#define ElfW(type) Elf64_ ## type
#else
#define ElfW(type) Elf32_ ## type
#endif

#include <bits/link.h>

struct dl_phdr_info {
	ElfW(Addr) dlpi_addr;
	const char *dlpi_name;
	const ElfW(Phdr) *dlpi_phdr;
	ElfW(Half) dlpi_phnum;
	unsigned long long int dlpi_adds;
	unsigned long long int dlpi_subs;
	size_t dlpi_tls_modid;
	void *dlpi_tls_data;
};

struct link_map {
	ElfW(Addr) l_addr;
	char *l_name;
	ElfW(Dyn) *l_ld;
	struct link_map *l_next, *l_prev;
};

struct r_debug {
	int r_version;
	struct link_map *r_map;
	ElfW(Addr) r_brk;
	enum { RT_CONSISTENT, RT_ADD, RT_DELETE } r_state;
	ElfW(Addr) r_ldbase;
};

int dl_iterate_phdr(int (*)(struct dl_phdr_info *, size_t, void *), void *);

#ifdef __cplusplus
}
#endif

#endif
