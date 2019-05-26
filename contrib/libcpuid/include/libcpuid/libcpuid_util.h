/*
 * Copyright 2008  Veselin Georgiev,
 * anrieffNOSPAM @ mgail_DOT.com (convert to gmail)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef __LIBCPUID_UTIL_H__
#define __LIBCPUID_UTIL_H__

#define COUNT_OF(array) (sizeof(array) / sizeof(array[0]))

struct feature_map_t {
	unsigned bit;
	cpu_feature_t feature;
};
 
void match_features(const struct feature_map_t* matchtable, int count,
                    uint32_t reg, struct cpu_id_t* data);

struct match_entry_t {
	int family, model, stepping, ext_family, ext_model;
	int ncores, l2cache, l3cache, brand_code, model_code;
	char name[32];
};

// returns the match score:
int match_cpu_codename(const struct match_entry_t* matchtable, int count,
                        struct cpu_id_t* data, int brand_code, int model_code);

void warnf(const char* format, ...)
#ifdef __GNUC__
__attribute__((format(printf, 1, 2)))
#endif
;
void debugf(int verboselevel, const char* format, ...)
#ifdef __GNUC__
__attribute__((format(printf, 2, 3)))
#endif
;
void generic_get_cpu_list(const struct match_entry_t* matchtable, int count,
                          struct cpu_list_t* list);

/*
 * Seek for a pattern in `haystack'.
 * Pattern may be an fixed string, or contain the special metacharacters
 * '.' - match any single character
 * '#' - match any digit
 * '[<chars>] - match any of the given chars (regex-like ranges are not
 *              supported)
 * Return val: 0 if the pattern is not found. Nonzero if it is found (actually,
 *             x + 1 where x is the index where the match is found).
 */
int match_pattern(const char* haystack, const char* pattern);

/*
 * Gets an initialized cpu_id_t. It is cached, so that internal libcpuid
 * machinery doesn't need to issue cpu_identify more than once.
 */
struct cpu_id_t* get_cached_cpuid(void);

/*
 * Sets the current errno
 */
int set_error(cpu_error_t err);

extern libcpuid_warn_fn_t _warn_fun;
extern int _current_verboselevel;

#endif /* __LIBCPUID_UTIL_H__ */
