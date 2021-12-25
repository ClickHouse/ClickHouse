#ifndef _AR_H
#define _AR_H

#ifdef __cplusplus
extern "C" {
#endif

#define ARMAG "!<arch>\n"
#define SARMAG 8
#define ARFMAG "`\n"

struct ar_hdr {
	char ar_name[16];
	char ar_date[12];
	char ar_uid[6], ar_gid[6];
	char ar_mode[8];
	char ar_size[10];
	char ar_fmag[2];
};

#ifdef __cplusplus
}
#endif

#endif
