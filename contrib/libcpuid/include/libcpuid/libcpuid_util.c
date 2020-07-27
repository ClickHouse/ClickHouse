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

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>
#include "libcpuid.h"
#include "libcpuid_util.h"

int _current_verboselevel;

void match_features(const struct feature_map_t* matchtable, int count, uint32_t reg, struct cpu_id_t* data)
{
	int i;
	for (i = 0; i < count; i++)
		if (reg & (1u << matchtable[i].bit))
			data->flags[matchtable[i].feature] = 1;
}

static void default_warn(const char *msg)
{
	fprintf(stderr, "%s", msg);
}

libcpuid_warn_fn_t _warn_fun = default_warn;

#if defined(_MSC_VER)
#	define vsnprintf _vsnprintf
#endif
void warnf(const char* format, ...)
{
	char buff[1024];
	va_list va;
	if (!_warn_fun) return;
	va_start(va, format);
	vsnprintf(buff, sizeof(buff), format, va);
	va_end(va);
	_warn_fun(buff);
}

void debugf(int verboselevel, const char* format, ...)
{
	char buff[1024];
	va_list va;
	if (verboselevel > _current_verboselevel) return;
	va_start(va, format);
	vsnprintf(buff, sizeof(buff), format, va);
	va_end(va);
	_warn_fun(buff);
}

static int score(const struct match_entry_t* entry, const struct cpu_id_t* data,
                 int brand_code, int model_code)
{
	int res = 0;
	if (entry->family	== data->family    ) res += 2;
	if (entry->model	== data->model     ) res += 2;
	if (entry->stepping	== data->stepping  ) res += 2;
	if (entry->ext_family	== data->ext_family) res += 2;
	if (entry->ext_model	== data->ext_model ) res += 2;
	if (entry->ncores	== data->num_cores ) res += 2;
	if (entry->l2cache	== data->l2_cache  ) res += 1;
	if (entry->l3cache	== data->l3_cache  ) res += 1;
	if (entry->brand_code   == brand_code      ) res += 2;
	if (entry->model_code   == model_code      ) res += 2;
	return res;
}

int match_cpu_codename(const struct match_entry_t* matchtable, int count,
                        struct cpu_id_t* data, int brand_code, int model_code)
{
	int bestscore = -1;
	int bestindex = 0;
	int i, t;
	
	debugf(3, "Matching cpu f:%d, m:%d, s:%d, xf:%d, xm:%d, ncore:%d, l2:%d, bcode:%d, code:%d\n",
		data->family, data->model, data->stepping, data->ext_family,
		data->ext_model, data->num_cores, data->l2_cache, brand_code, model_code);
	
	for (i = 0; i < count; i++) {
		t = score(&matchtable[i], data, brand_code, model_code);
		debugf(3, "Entry %d, `%s', score %d\n", i, matchtable[i].name, t);
		if (t > bestscore) {
			debugf(2, "Entry `%s' selected - best score so far (%d)\n", matchtable[i].name, t);
			bestscore = t;
			bestindex = i;
		}
	}
	strcpy(data->cpu_codename, matchtable[bestindex].name);
	return bestscore;
}

void generic_get_cpu_list(const struct match_entry_t* matchtable, int count,
                          struct cpu_list_t* list)
{
	int i, j, n, good;
	n = 0;
	list->names = (char**) malloc(sizeof(char*) * count);
	for (i = 0; i < count; i++) {
		if (strstr(matchtable[i].name, "Unknown")) continue;
		good = 1;
		for (j = n - 1; j >= 0; j--)
			if (!strcmp(list->names[j], matchtable[i].name)) {
				good = 0;
				break;
			}
		if (!good) continue;
#if defined(_MSC_VER)
		list->names[n++] = _strdup(matchtable[i].name);
#else
		list->names[n++] = strdup(matchtable[i].name);
#endif
	}
	list->num_entries = n;
}

static int xmatch_entry(char c, const char* p)
{
	int i, j;
	if (c == 0) return -1;
	if (c == p[0]) return 1;
	if (p[0] == '.') return 1;
	if (p[0] == '#' && isdigit(c)) return 1;
	if (p[0] == '[') {
		j = 1;
		while (p[j] && p[j] != ']') j++;
		if (!p[j]) return -1;
		for (i = 1; i < j; i++)
			if (p[i] == c) return j + 1;
	}
	return -1;
}

int match_pattern(const char* s, const char* p)
{
	int i, j, dj, k, n, m;
	n = (int) strlen(s);
	m = (int) strlen(p);
	for (i = 0; i < n; i++) {
		if (xmatch_entry(s[i], p) != -1) {
			j = 0;
			k = 0;
			while (j < m && ((dj = xmatch_entry(s[i + k], p + j)) != -1)) {
				k++;
				j += dj;
			}
			if (j == m) return i + 1;
		}
	}
	return 0;
}

struct cpu_id_t* get_cached_cpuid(void)
{
	static int initialized = 0;
	static struct cpu_id_t id;
	if (initialized) return &id;
	if (cpu_identify(NULL, &id))
		memset(&id, 0, sizeof(id));
	initialized = 1;
	return &id;
}
