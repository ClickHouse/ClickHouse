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
#include <string.h>
#include "libcpuid.h"
#include "libcpuid_util.h"
#include "asm-bits.h"
#include "rdtsc.h"

#ifdef _WIN32
#include <windows.h>
void sys_precise_clock(uint64_t *result)
{
	double c, f;
	LARGE_INTEGER freq, counter;
	QueryPerformanceCounter(&counter);
	QueryPerformanceFrequency(&freq);
	c = (double) counter.QuadPart;
	f = (double) freq.QuadPart;
	*result = (uint64_t) ( c * 1000000.0 / f );
}
#else
/* assuming Linux, Mac OS or other POSIX */
#include <sys/time.h>
void sys_precise_clock(uint64_t *result)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*result = (uint64_t) tv.tv_sec * (uint64_t) 1000000 +
	          (uint64_t) tv.tv_usec;
}
#endif /* _WIN32 */

/* out = a - b */
static void mark_t_subtract(struct cpu_mark_t* a, struct cpu_mark_t* b, struct cpu_mark_t *out)
{
	out->tsc = a->tsc - b->tsc;
	out->sys_clock = a->sys_clock - b->sys_clock;
}

void cpu_tsc_mark(struct cpu_mark_t* mark)
{
	cpu_rdtsc(&mark->tsc);
	sys_precise_clock(&mark->sys_clock);
}

void cpu_tsc_unmark(struct cpu_mark_t* mark)
{
	struct cpu_mark_t temp;
	cpu_tsc_mark(&temp);
	mark_t_subtract(&temp, mark, mark);
}


int cpu_clock_by_mark(struct cpu_mark_t* mark)
{
	uint64_t result;
	
	/* Check if some subtraction resulted in a negative number: */
	if ((mark->tsc >> 63) != 0 || (mark->sys_clock >> 63) != 0) return -1;
	
	/* Divide-by-zero check: */
	if (mark->sys_clock == 0) return -1;
	
	/* Check if the result fits in 32bits */
	result = mark->tsc / mark->sys_clock;
	if (result > (uint64_t) 0x7fffffff) return -1;
	return (int) result;
}

#ifdef _WIN32
int cpu_clock_by_os(void)
{
	HKEY key;
	DWORD result;
	DWORD size = 4;
	
	if (RegOpenKeyEx(HKEY_LOCAL_MACHINE, TEXT("HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0"), 0, KEY_READ, &key) != ERROR_SUCCESS)
		return -1;
	
	if (RegQueryValueEx(key, TEXT("~MHz"), NULL, NULL, (LPBYTE) &result, (LPDWORD) &size) != ERROR_SUCCESS) {
		RegCloseKey(key);
		return -1;
	}
	RegCloseKey(key);
	
	return (int)result;
}
#else
#ifdef __APPLE__
#include <sys/types.h>
#include <sys/sysctl.h>
/* Assuming Mac OS X with hw.cpufrequency sysctl */
int cpu_clock_by_os(void)
{
	long long result = -1;
	size_t size = sizeof(result);
	if (sysctlbyname("hw.cpufrequency", &result, &size, NULL, 0))
		return -1;
	return (int) (result / (long long) 1000000);
}
#else
/* Assuming Linux with /proc/cpuinfo */
int cpu_clock_by_os(void)
{
	FILE *f;
	char line[1024], *s;
	int result;
	
	f = fopen("/proc/cpuinfo", "rt");
	if (!f) return -1;
	
	while (fgets(line, sizeof(line), f)) {
		if (!strncmp(line, "cpu MHz", 7)) {
			s = strchr(line, ':');
			if (s && 1 == sscanf(s, ":%d.", &result)) {
				fclose(f);
				return result;
			}
		}
	}
	fclose(f);
	return -1;
}
#endif /* __APPLE__ */
#endif /* _WIN32 */

/* Emulate doing useful CPU intensive work */
static int busy_loop(int amount)
{
	int i, j, k, s = 0;
	static volatile int data[42] = {32, 12, -1, 5, 23, 0 };
	for (i = 0; i < amount; i++)
		for (j = 0; j < 65536; j++)
			for (k = 0; k < 42; k++)
				s += data[k];
	return s;
}

int busy_loop_delay(int milliseconds)
{
	int cycles = 0, r = 0, first = 1;
	uint64_t a, b, c;
	sys_precise_clock(&a);
	while (1) {
		sys_precise_clock(&c);
		if ((c - a) / 1000 > milliseconds) return r;
		r += busy_loop(cycles);
		if (first) {
			first = 0;
		} else {
			if (c - b < 1000) cycles *= 2;
			if (c - b > 10000) cycles /= 2;
		}
		b = c;
	}
}

int cpu_clock_measure(int millis, int quad_check)
{
	struct cpu_mark_t begin[4], end[4], temp, temp2;
	int results[4], cycles, n, k, i, j, bi, bj, mdiff, diff, _zero = 0;
	uint64_t tl;
	
	if (millis < 1) return -1;
	tl = millis * (uint64_t) 1000;
	if (quad_check)
		tl /= 4;
	n = quad_check ? 4 : 1;
	cycles = 1;
	for (k = 0; k < n; k++) {
		cpu_tsc_mark(&begin[k]);
		end[k] = begin[k];
		do {
			/* Run busy loop, and fool the compiler that we USE the garbishy
			   value it calculates */
			_zero |= (1 & busy_loop(cycles));
			cpu_tsc_mark(&temp);
			mark_t_subtract(&temp, &end[k], &temp2);
			/* If busy loop is too short, increase it */
			if (temp2.sys_clock < tl / 8)
				cycles *= 2;
			end[k] = temp;
		} while (end[k].sys_clock - begin[k].sys_clock < tl);
		mark_t_subtract(&end[k], &begin[k], &temp);
		results[k] = cpu_clock_by_mark(&temp);
	}
	if (n == 1) return results[0];
	mdiff = 0x7fffffff;
	bi = bj = -1;
	for (i = 0; i < 4; i++) {
		for (j = i + 1; j < 4; j++) {
			diff = results[i] - results[j];
			if (diff < 0) diff = -diff;
			if (diff < mdiff) {
				mdiff = diff;
				bi = i;
				bj = j;
			}
		}
	}
	if (results[bi] == -1) return -1;
	return (results[bi] + results[bj] + _zero) / 2;
}

int cpu_clock_by_ic(int millis, int runs)
{
	int max_value = 0, cur_value, i, ri, cycles_inner, cycles_outer, c;
	struct cpu_id_t* id;
	uint64_t t0, t1, tl, hz;
	int multiplier_numerator = 1, multiplier_denom = 1;
	if (millis <= 0 || runs <= 0) return -2;
	id = get_cached_cpuid();
	// if there aren't SSE instructions - we can't run the test at all
	if (!id || !id->flags[CPU_FEATURE_SSE]) return -1;
	//
	if (id->sse_size < 128) {
		debugf(1, "SSE execution path is 64-bit\n");
		// on a CPU with half SSE unit length, SSE instructions execute at 0.5 IPC;
		// the resulting value must be multiplied by 2:
		multiplier_numerator = 2;
	} else {
		debugf(1, "SSE execution path is 128-bit\n");
	}
	//
	// on a Bulldozer or later CPU, SSE instructions execute at 1.4 IPC, handle that as well:
	if (id->vendor == VENDOR_AMD && id->ext_family >= 21) {
		debugf(1, "cpu_clock_by_ic: Bulldozer (or later) detected, dividing result by 1.4\n");
		multiplier_numerator = 5;
		multiplier_denom = 7; // multiply by 5/7, to divide by 1.4
	}
	//
	tl = millis * 125; // (*1000 / 8)
	cycles_inner = 128;
	cycles_outer = 1;
	do {
		if (cycles_inner < 1000000000) cycles_inner *= 2;
		else cycles_outer *= 2;
		sys_precise_clock(&t0);
		for (i = 0; i < cycles_outer; i++)
			busy_sse_loop(cycles_inner);
		sys_precise_clock(&t1);
	} while (t1 - t0 < tl);
	debugf(2, "inner: %d, outer: %d\n", cycles_inner, cycles_outer);
	for (ri = 0; ri < runs; ri++) {
		sys_precise_clock(&t0);
		c = 0;
		do {
			c++;
			for (i = 0; i < cycles_outer; i++)
				busy_sse_loop(cycles_inner);
			sys_precise_clock(&t1);
		} while (t1 - t0 < tl * (uint64_t) 8);
		// cpu_Hz = cycles_inner * cycles_outer * 256 / (t1 - t0) * 1000000
		debugf(2, "c = %d, td = %d\n", c, (int) (t1 - t0));
		hz = ((uint64_t) cycles_inner * (uint64_t) 256 + 12) * 
		     (uint64_t) cycles_outer * (uint64_t) multiplier_numerator * (uint64_t) c * (uint64_t) 1000000
		     / ((t1 - t0) * (uint64_t) multiplier_denom);
		cur_value = (int) (hz / 1000000);
		if (cur_value > max_value) max_value = cur_value;
	}
	return max_value;
}

int cpu_clock(void)
{
	int result;
	result = cpu_clock_by_os();
	if (result <= 0)
		result = cpu_clock_measure(200, 1);
	return result;
}
