#ifndef _SYS_SYSINFO_H
#define _SYS_SYSINFO_H

#ifdef __cplusplus
extern "C" {
#endif

#define SI_LOAD_SHIFT 16

struct sysinfo {
	unsigned long uptime;
	unsigned long loads[3];
	unsigned long totalram;
	unsigned long freeram;
	unsigned long sharedram;
	unsigned long bufferram;
	unsigned long totalswap;
	unsigned long freeswap;
	unsigned short procs, pad;
	unsigned long totalhigh;
	unsigned long freehigh;
	unsigned mem_unit;
	char __reserved[256];
};

int sysinfo (struct sysinfo *);
int get_nprocs_conf (void);
int get_nprocs (void);
long get_phys_pages (void);
long get_avphys_pages (void);

#ifdef __cplusplus
}
#endif

#endif
