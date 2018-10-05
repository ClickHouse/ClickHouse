/*
 * Copyright 2009  Veselin Georgiev,
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
#define _XOPEN_SOURCE 500
#include <stdio.h>
#include <stdlib.h>
#include "libcpuid.h"
#include "asm-bits.h"
#include "libcpuid_util.h"
#include "libcpuid_internal.h"
#include "rdtsc.h"

#if defined (__linux__) || defined (__gnu_linux__)
/* Assuming linux with /dev/cpu/x/msr: */
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
struct msr_driver_t { int fd; };
static int rdmsr_supported(void);
static int load_driver(char *msr_path)
{
	const int file_exists   = !access(msr_path, F_OK);
	const int file_readable = !access(msr_path, R_OK);

	if (file_exists && file_readable)
		return 1;
	else if (file_exists && !file_readable)
		return 0;
	else if (getuid() != 0)
		return 0;
	else
		return !system("modprobe msr 2> /dev/null");
}

struct msr_driver_t* cpu_msr_driver_open(void)
{
	return cpu_msr_driver_open_core(0);
}

struct msr_driver_t* cpu_msr_driver_open_core(unsigned core_num)
{
	char msr[32];
	struct msr_driver_t* handle;
	if (core_num >= cpuid_get_total_cpus()) {
		set_error(ERR_INVCNB);
		return NULL;
	}
	if (!rdmsr_supported()) {
		set_error(ERR_NO_RDMSR);
		return NULL;
	}
	sprintf(msr, "/dev/cpu/%u/msr", core_num);
	if(!load_driver(msr)) {
		set_error(ERR_NO_DRIVER);
		return NULL;
	}
	int fd = open(msr, O_RDONLY);
	if (fd < 0) {
		if (errno == EIO) {
			set_error(ERR_NO_RDMSR);
			return NULL;
		}
		set_error(ERR_NO_DRIVER);
		return NULL;
	}
	handle = (struct msr_driver_t*) malloc(sizeof(struct msr_driver_t));
	handle->fd = fd;
	return handle;
}

int cpu_rdmsr(struct msr_driver_t* driver, uint32_t msr_index, uint64_t* result)
{
	ssize_t ret;

	if (!driver || driver->fd < 0)
		return set_error(ERR_HANDLE);
	ret = pread(driver->fd, result, 8, msr_index);
	if (ret != 8)
		return set_error(ERR_INVMSR);
	return 0;
}

int cpu_msr_driver_close(struct msr_driver_t* drv)
{
	if (drv && drv->fd >= 0) {
		close(drv->fd);
		free(drv);
	}
	return 0;
}

/* #endif defined (__linux__) || defined (__gnu_linux__) */

#elif defined (__FreeBSD__) || defined (__DragonFly__)
/* Assuming FreeBSD with /dev/cpuctlX */
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/cpuctl.h>

struct msr_driver_t { int fd; };
static int rdmsr_supported(void);
static int load_driver(char *msr_path)
{
	const int file_exists   = !access(msr_path, F_OK);
	const int file_readable = !access(msr_path, R_OK);

	if (file_exists && file_readable)
		return 1;
	else if (file_exists && !file_readable)
		return 0;
	else if (getuid() != 0)
		return 0;
	else
		return !system("kldload -n cpuctl 2> /dev/null");
}

struct msr_driver_t* cpu_msr_driver_open(void)
{
	return cpu_msr_driver_open_core(0);
}

struct msr_driver_t* cpu_msr_driver_open_core(unsigned core_num)
{
	char msr[32];
	struct msr_driver_t* handle;
	if (core_num >= cpuid_get_total_cpus()) {
		set_error(ERR_INVCNB);
		return NULL;
	}
	if (!rdmsr_supported()) {
		set_error(ERR_NO_RDMSR);
		return NULL;
	}
	sprintf(msr, "/dev/cpuctl%u", core_num);
	if(!load_driver(msr)) {
		set_error(ERR_NO_DRIVER);
		return NULL;
	}
	int fd = open(msr, O_RDONLY);
	if (fd < 0) {
		if (errno == EIO) {
			set_error(ERR_NO_RDMSR);
			return NULL;
		}
		set_error(ERR_NO_DRIVER);
		return NULL;
	}
	handle = (struct msr_driver_t*) malloc(sizeof(struct msr_driver_t));
	handle->fd = fd;
	return handle;
}

int cpu_rdmsr(struct msr_driver_t* driver, uint32_t msr_index, uint64_t* result)
{
	cpuctl_msr_args_t args;
	args.msr = msr_index;

	if (!driver || driver->fd < 0)
		return set_error(ERR_HANDLE);

	if(ioctl(driver->fd, CPUCTL_RDMSR, &args))
		return set_error(ERR_INVMSR);

	*result = args.data; 
	return 0;
}

int cpu_msr_driver_close(struct msr_driver_t* drv)
{
	if (drv && drv->fd >= 0) {
		close(drv->fd);
		free(drv);
	}
	return 0;
}

/* #endif defined (__FreeBSD__) || defined (__DragonFly__) */

#elif defined (_WIN32)
#include <windows.h>
#include <winioctl.h>
#include <winerror.h>

extern uint8_t cc_x86driver_code[];
extern int cc_x86driver_code_size;
extern uint8_t cc_x64driver_code[];
extern int cc_x64driver_code_size;

struct msr_driver_t {
	char driver_path[MAX_PATH + 1];
	SC_HANDLE scManager;
	volatile SC_HANDLE scDriver;
	HANDLE hhDriver;
	OVERLAPPED ovl;
	int errorcode;
};

static int rdmsr_supported(void);
static int extract_driver(struct msr_driver_t* driver);
static int load_driver(struct msr_driver_t* driver);

struct msr_driver_t* cpu_msr_driver_open(void)
{
	struct msr_driver_t* drv;
	int status;
	if (!rdmsr_supported()) {
		set_error(ERR_NO_RDMSR);
		return NULL;
	}
	
	drv = (struct msr_driver_t*) malloc(sizeof(struct msr_driver_t));
	if (!drv) {
		set_error(ERR_NO_MEM);
		return NULL;
	}
	memset(drv, 0, sizeof(struct msr_driver_t));

	if (!extract_driver(drv)) {
		free(drv);
		set_error(ERR_EXTRACT);
		return NULL;
	}
	
	status = load_driver(drv);
	if (!DeleteFile(drv->driver_path))
		debugf(1, "Deleting temporary driver file failed.\n");
	if (!status) {
		set_error(drv->errorcode ? drv->errorcode : ERR_NO_DRIVER);
		free(drv);
		return NULL;
	}
	return drv;
}

struct msr_driver_t* cpu_msr_driver_open_core(unsigned core_num)
{
	warnf("cpu_msr_driver_open_core(): parameter ignored (function is the same as cpu_msr_driver_open)\n");
	return cpu_msr_driver_open();
}

typedef BOOL (WINAPI *LPFN_ISWOW64PROCESS) (HANDLE, PBOOL);
static BOOL is_running_x64(void)
{
	BOOL bIsWow64 = FALSE;

	LPFN_ISWOW64PROCESS fnIsWow64Process = (LPFN_ISWOW64PROCESS)GetProcAddress(GetModuleHandle(__TEXT("kernel32")), "IsWow64Process");
	if(NULL != fnIsWow64Process)
		fnIsWow64Process(GetCurrentProcess(), &bIsWow64);
	return bIsWow64;
}


static int extract_driver(struct msr_driver_t* driver)
{
	FILE *f;
	if (!GetTempPath(sizeof(driver->driver_path), driver->driver_path)) return 0;
	strcat(driver->driver_path, "TmpRdr.sys");
	
	f = fopen(driver->driver_path, "wb");
	if (!f) return 0;
	if (is_running_x64())
		fwrite(cc_x64driver_code, 1, cc_x64driver_code_size, f);
	else
		fwrite(cc_x86driver_code, 1, cc_x86driver_code_size, f);
	fclose(f);
	return 1;
}

static BOOL wait_for_service_state(SC_HANDLE hService, DWORD dwDesiredState, SERVICE_STATUS *lpsrvStatus){
	BOOL fOK = FALSE;
	DWORD dwWaitHint;

	if(hService != NULL){
		while(TRUE){
			fOK = QueryServiceStatus(hService, lpsrvStatus);
			if(!fOK) 
				break;
			if(lpsrvStatus->dwCurrentState == dwDesiredState) 
				break;

			dwWaitHint = lpsrvStatus->dwWaitHint / 10;    // Poll 1/10 of the wait hint
			if (dwWaitHint <  1000) 
				dwWaitHint = 1000;  // At most once per second
			if (dwWaitHint > 10000) 
				dwWaitHint = 10000; // At least every 10 seconds
			Sleep(dwWaitHint);
		}
	}

	return fOK;
}

static int load_driver(struct msr_driver_t* drv)
{
	LPTSTR		lpszInfo = __TEXT("RDMSR Executor Driver");
	USHORT		uLen = 0;
	SERVICE_STATUS srvStatus = {0};
	BOOL		fRunning = FALSE;
	DWORD		dwLastError;
	LPTSTR		lpszDriverServiceName = __TEXT("TmpRdr");
	TCHAR		lpszDriverName[] = __TEXT("\\\\.\\Global\\TmpRdr");

	if((LPVOID)(drv->scManager = OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS)) != NULL) {
		drv->scDriver = CreateService(drv->scManager, lpszDriverServiceName, lpszInfo, SERVICE_ALL_ACCESS,
		                              SERVICE_KERNEL_DRIVER, SERVICE_DEMAND_START, SERVICE_ERROR_NORMAL,
				                      drv->driver_path, NULL, NULL, NULL, NULL, NULL);
		if(drv->scDriver == NULL){
			switch(dwLastError = GetLastError()){
				case ERROR_SERVICE_EXISTS:
				case ERROR_SERVICE_MARKED_FOR_DELETE:{
					LPQUERY_SERVICE_CONFIG lpqsc;
					DWORD dwBytesNeeded;

					drv->scDriver = OpenService(drv->scManager, lpszDriverServiceName, SERVICE_ALL_ACCESS);
					if(drv->scDriver == NULL){
						debugf(1, "Error opening service: %d\n", GetLastError());
						break;
					}

					QueryServiceConfig(drv->scDriver, NULL, 0, &dwBytesNeeded);
					if((dwLastError = GetLastError()) == ERROR_INSUFFICIENT_BUFFER){
						lpqsc = calloc(1, dwBytesNeeded);
						if(!QueryServiceConfig(drv->scDriver, lpqsc, dwBytesNeeded, &dwBytesNeeded)){
							free(lpqsc);
							debugf(1, "Error query service config(adjusted buffer): %d\n", GetLastError());
							goto clean_up;
						}
						else{
							free(lpqsc);
						}
					}
					else{
						debugf(1, "Error query service config: %d\n", dwLastError);
						goto clean_up;
					}

					break;
				}
				case ERROR_ACCESS_DENIED:
					drv->errorcode = ERR_NO_PERMS;
					break;
				default:
					debugf(1, "Create driver service failed: %d\n", dwLastError);
					break;
			}				
		}
		if(drv->scDriver != NULL){
			if(StartService(drv->scDriver, 0, NULL)){
				if(!wait_for_service_state(drv->scDriver, SERVICE_RUNNING, &srvStatus)){
					debugf(1, "Driver load failed.\n");
					DeleteService(drv->scDriver);
					CloseServiceHandle(drv->scManager);
					drv->scDriver = NULL;
					goto clean_up;
				} else {
					fRunning = TRUE;
				}
			} else{
				if((dwLastError = GetLastError()) == ERROR_SERVICE_ALREADY_RUNNING)
					fRunning = TRUE;
				else{
					debugf(1, "Driver start failed.\n");
					DeleteService(drv->scDriver);
					CloseServiceHandle(drv->scManager);
					drv->scDriver = NULL;
					goto clean_up;
				}

			}
			if(fRunning)
				debugf(1, "Driver already running.\n");
			else
				debugf(1, "Driver loaded.\n"); 
			CloseServiceHandle(drv->scManager);
			drv->hhDriver = CreateFile(lpszDriverName, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, 0, OPEN_EXISTING, FILE_FLAG_OVERLAPPED, 0);
			drv->ovl.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
			return 1;
		}
	} else {
		debugf(1, "Open SCM failed: %d\n", GetLastError());
	}

clean_up:
	if(drv->scManager != NULL){
		CloseServiceHandle(drv->scManager);
		drv->scManager = 0; // pointless
	}
	if(drv->scDriver != NULL){
		if(!DeleteService(drv->scDriver))
			debugf(1, "Delete driver service failed: %d\n", GetLastError());
		CloseServiceHandle(drv->scDriver);
		drv->scDriver = 0;
	}

	return 0;
}

#define FILE_DEVICE_UNKNOWN             0x00000022
#define IOCTL_UNKNOWN_BASE              FILE_DEVICE_UNKNOWN
#define IOCTL_PROCVIEW_RDMSR		CTL_CODE(IOCTL_UNKNOWN_BASE, 0x0803, METHOD_BUFFERED, FILE_READ_ACCESS | FILE_WRITE_ACCESS)

int cpu_rdmsr(struct msr_driver_t* driver, uint32_t msr_index, uint64_t* result)
{
	DWORD dwBytesReturned;
	__int64 msrdata;
	SERVICE_STATUS srvStatus = {0};

	if (!driver)
		return set_error(ERR_HANDLE);
	DeviceIoControl(driver->hhDriver, IOCTL_PROCVIEW_RDMSR, &msr_index, sizeof(int), &msrdata, sizeof(__int64), &dwBytesReturned, &driver->ovl);
	GetOverlappedResult(driver->hhDriver, &driver->ovl, &dwBytesReturned, TRUE);	
	*result = msrdata;
	return 0;
}

int cpu_msr_driver_close(struct msr_driver_t* drv)
{
	SERVICE_STATUS srvStatus = {0};
	if (drv == NULL) return 0;
	if(drv->scDriver != NULL){
		if (drv->hhDriver) CancelIo(drv->hhDriver);
		if(drv->ovl.hEvent != NULL)
			CloseHandle(drv->ovl.hEvent);
		if (drv->hhDriver) CloseHandle(drv->hhDriver);
		drv->hhDriver = NULL;
		drv->ovl.hEvent = NULL;
		if (ControlService(drv->scDriver, SERVICE_CONTROL_STOP, &srvStatus)){
			if (wait_for_service_state(drv->scDriver, SERVICE_STOPPED, &srvStatus)){
				DeleteService(drv->scDriver);
			}
		}
	}
	return 0;
}

/* endif defined (_WIN32) */

#else /* Unsupported OS */
/* On others OS (i.e., Darwin), we still do not support RDMSR, so supply dummy struct
   and functions */

#define RDMSR_UNSUPPORTED_OS

struct msr_driver_t { int dummy; };
struct msr_driver_t* cpu_msr_driver_open(void)
{
	set_error(ERR_NOT_IMP);
	return NULL;
}

struct msr_driver_t* cpu_msr_driver_open_core(unsigned core_num)
{
	set_error(ERR_NOT_IMP);
	return NULL;
}

int cpu_rdmsr(struct msr_driver_t* driver, uint32_t msr_index, uint64_t* result)
{
	return set_error(ERR_NOT_IMP);
}

int cpu_msr_driver_close(struct msr_driver_t* driver)
{
	return set_error(ERR_NOT_IMP);
}

int cpu_rdmsr_range(struct msr_driver_t* handle, uint32_t msr_index, uint8_t highbit,
                    uint8_t lowbit, uint64_t* result)
{
	return set_error(ERR_NOT_IMP);
}

int cpu_msrinfo(struct msr_driver_t* driver, cpu_msrinfo_request_t which)
{
	return set_error(ERR_NOT_IMP);
}

#endif /* Unsupported OS */

#ifndef RDMSR_UNSUPPORTED_OS

/* Useful links for hackers:
- AMD MSRs:
  AMD BIOS and Kernel Developer’s Guide (BKDG)
  * AMD Family 10h Processors
  http://support.amd.com/TechDocs/31116.pdf
  * AMD Family 11h Processors
  http://support.amd.com/TechDocs/41256.pdf
  * AMD Family 12h Processors
  http://support.amd.com/TechDocs/41131.pdf
  * AMD Family 14h Processors
  http://support.amd.com/TechDocs/43170_14h_Mod_00h-0Fh_BKDG.pdf
  * AMD Family 15h Processors
  http://support.amd.com/TechDocs/42301_15h_Mod_00h-0Fh_BKDG.pdf
  http://support.amd.com/TechDocs/42300_15h_Mod_10h-1Fh_BKDG.pdf
  http://support.amd.com/TechDocs/49125_15h_Models_30h-3Fh_BKDG.pdf
  http://support.amd.com/TechDocs/50742_15h_Models_60h-6Fh_BKDG.pdf
  http://support.amd.com/TechDocs/49125_15h_Models_30h-3Fh_BKDG.pdf
  * AMD Family 16h Processors
  http://support.amd.com/TechDocs/48751_16h_bkdg.pdf
  http://support.amd.com/TechDocs/52740_16h_Models_30h-3Fh_BKDG.pdf

- Intel MSRs:
  Intel® 64 and IA-32 Architectures Software Developer’s Manual
  * Volume 3 (3A, 3B, 3C & 3D): System Programming Guide
  http://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-software-developer-system-programming-manual-325384.pdf
*/

/* AMD MSRs addresses */
#define MSR_PSTATE_L           0xC0010061
#define MSR_PSTATE_S           0xC0010063
#define MSR_PSTATE_0           0xC0010064
#define MSR_PSTATE_7           0xC001006B

/* Intel MSRs addresses */
#define IA32_MPERF             0xE7
#define IA32_APERF             0xE8
#define IA32_PERF_STATUS       0x198
#define IA32_THERM_STATUS      0x19C
#define MSR_EBL_CR_POWERON     0x2A
#define MSR_TURBO_RATIO_LIMIT  0x1AD
#define MSR_TEMPERATURE_TARGET 0x1A2
#define MSR_PERF_STATUS        0x198
#define MSR_PLATFORM_INFO      0xCE


static int rdmsr_supported(void)
{
	struct cpu_id_t* id = get_cached_cpuid();
	return id->flags[CPU_FEATURE_MSR];
}

static int perfmsr_measure(struct msr_driver_t* handle, int msr)
{
	int err;
	uint64_t a, b;
	uint64_t x, y;
	err = cpu_rdmsr(handle, msr, &x);
	if (err) return CPU_INVALID_VALUE;
	sys_precise_clock(&a);
	busy_loop_delay(10);
	cpu_rdmsr(handle, msr, &y);
	sys_precise_clock(&b);
	if (a >= b || x > y) return CPU_INVALID_VALUE;
	return (int) ((y - x) / (b - a));
}

static int get_amd_multipliers(struct msr_driver_t* handle, struct cpu_id_t *id,
                               struct internal_id_info_t *internal,
                               uint32_t pstate, uint64_t *multiplier)
{
	int err;
	static int clock = 0;
	uint64_t CpuFid, CpuDid, CpuDidLSD;
	double divisor;

	if (pstate < MSR_PSTATE_0 || MSR_PSTATE_7 < pstate)
		return 1;

	switch (id->ext_family) {
		case 0x11:
			/* BKDG 11h, page 236
			MSRC001_00[6B:64][8:6] is CpuDid
			MSRC001_00[6B:64][5:0] is CpuFid
			CPU COF is ((100 MHz * (CpuFid + 08h)) / (2^CpuDid)) */
			err  = cpu_rdmsr_range(handle, pstate, 8, 6, &CpuDid);
			err += cpu_rdmsr_range(handle, pstate, 5, 0, &CpuFid);
			*multiplier = (uint64_t) ((CpuFid + 0x8) / (1ull << CpuDid));
			break;
		case 0x12:
			/* BKDG 12h, page 469
			MSRC001_00[6B:64][8:4] is CpuFid
			MSRC001_00[6B:64][3:0] is CpuDid
			CPU COF is (100MHz * (CpuFid + 10h) / (divisor specified by CpuDid)) */
			err  = cpu_rdmsr_range(handle, pstate, 8, 4, &CpuFid);
			err += cpu_rdmsr_range(handle, pstate, 3, 0, &CpuDid);
			if (CpuDid == 0x0)
				divisor = 1;
			else if (CpuDid == 0x1)
				divisor = 1.5;
			else if (CpuDid == 0x2)
				divisor = 2;
			else if (CpuDid == 0x3)
				divisor = 3;
			else if (CpuDid == 0x4)
				divisor = 4;
			else if (CpuDid == 0x5)
				divisor = 6;
			else if (CpuDid == 0x6)
				divisor = 8;
			else if (CpuDid == 0x7)
				divisor = 12;
			else if (CpuDid == 0x8)
				divisor = 16;
			else
				divisor = 0;

			if (divisor > 0)
				*multiplier = (uint64_t) ((CpuFid + 0x10) / divisor);
			else
				err++;
			break;
		case 0x14:
			/* BKDG 14h, page 430
			MSRC001_00[6B:64][8:4] is CpuDidMSD
			MSRC001_00[6B:64][3:0] is CpuDidLSD
			PLL COF is (100 MHz * (D18F3xD4[MainPllOpFreqId] + 10h))
			Divisor is (CpuDidMSD + (CpuDidLSD * 0.25) + 1)
			CPU COF is (main PLL frequency specified by D18F3xD4[MainPllOpFreqId]) / (core clock divisor specified by CpuDidMSD and CpuDidLSD) */
			err  = cpu_rdmsr_range(handle, pstate, 8, 4, &CpuDid);
			err += cpu_rdmsr_range(handle, pstate, 3, 0, &CpuDidLSD);
			if (clock == 0)
				clock = cpu_clock_measure(100, 1) + 5; // Fake round
			*multiplier = (uint64_t) ((clock / 100 + 0x10) / (CpuDid + CpuDidLSD * 0.25 + 1));
			break;
		case 0x10:
			/* BKDG 10h, page 429
			MSRC001_00[6B:64][8:6] is CpuDid
			MSRC001_00[6B:64][5:0] is CpuFid
			CPU COF is (100 MHz * (CpuFid + 10h) / (2^CpuDid)) */
		case 0x15:
			/* BKDG 15h, page 570/580/635/692 (00h-0Fh/10h-1Fh/30h-3Fh/60h-6Fh)
			MSRC001_00[6B:64][8:6] is CpuDid
			MSRC001_00[6B:64][5:0] is CpuFid
			CoreCOF is (100 * (MSRC001_00[6B:64][CpuFid] + 10h) / (2^MSRC001_00[6B:64][CpuDid])) */
		case 0x16:
			/* BKDG 16h, page 549/611 (00h-0Fh/30h-3Fh)
			MSRC001_00[6B:64][8:6] is CpuDid
			MSRC001_00[6B:64][5:0] is CpuFid
			CoreCOF is (100 * (MSRC001_00[6B:64][CpuFid] + 10h) / (2^MSRC001_00[6B:64][CpuDid])) */
			err  = cpu_rdmsr_range(handle, pstate, 8, 6, &CpuDid);
			err += cpu_rdmsr_range(handle, pstate, 5, 0, &CpuFid);
			*multiplier = (uint64_t) ((CpuFid + 0x10) / (1ull << CpuDid));
			break;
		default:
			err = 1;
			break;
	}

	return err;
}

static double get_info_min_multiplier(struct msr_driver_t* handle, struct cpu_id_t *id,
                                      struct internal_id_info_t *internal)
{
	int err;
	uint64_t reg;

	if(id->vendor == VENDOR_INTEL) {
		/* Refer links above
		Table 35-12.  MSRs in Next Generation Intel Atom Processors Based on the Goldmont Microarchitecture
		Table 35-13.  MSRs in Processors Based on Intel® Microarchitecture Code Name Nehalem
		Table 35-18.  MSRs Supported by Intel® Processors based on Intel® microarchitecture code name Sandy Bridge (Contd.)
		Table 35-23.  Additional MSRs Supported by 3rd Generation Intel® Core™ Processors (based on Intel® microarchitecture code name Ivy Bridge)
		Table 35-24.  MSRs Supported by Intel® Xeon® Processors E5 v2 Product Family (based on Ivy Bridge-E microarchitecture)
		Table 35-27.  Additional MSRs Supported by Processors based on the Haswell or Haswell-E microarchitectures
		Table 35-34.  Additional MSRs Common to Intel® Xeon® Processor D and Intel Xeon Processors E5 v4 Family Based on the Broadwell Microarchitecture
		Table 35-40.  Selected MSRs Supported by Next Generation Intel® Xeon Phi™ Processors with DisplayFamily_DisplayModel Signature 06_57H
		MSR_PLATFORM_INFO[47:40] is Maximum Efficiency Ratio
		Maximum Efficiency Ratio is the minimum ratio that the processor can operates */
		err = cpu_rdmsr_range(handle, MSR_PLATFORM_INFO, 47, 40, &reg);
		if (!err) return (double) reg;
	}
	else if(id->vendor == VENDOR_AMD) {
		/* Refer links above
		MSRC001_0061[6:4] is PstateMaxVal
		PstateMaxVal is the lowest-performance non-boosted P-state */
		err  = cpu_rdmsr_range(handle, MSR_PSTATE_L, 6, 4, &reg);
		err += get_amd_multipliers(handle, id, internal, MSR_PSTATE_0 + (uint32_t) reg, &reg);
		if (!err) return (double) reg;
	}

	return (double) CPU_INVALID_VALUE / 100;
}

static double get_info_cur_multiplier(struct msr_driver_t* handle, struct cpu_id_t *id,
                                      struct internal_id_info_t *internal)
{
	int err;
	uint64_t reg;

	if(id->vendor == VENDOR_INTEL && internal->code.intel == PENTIUM) {
		err = cpu_rdmsr(handle, MSR_EBL_CR_POWERON, &reg);
		if (!err) return (double) ((reg>>22) & 0x1f);
	}
	else if(id->vendor == VENDOR_INTEL && internal->code.intel != PENTIUM) {
		/* Refer links above
		Table 35-2.  IA-32 Architectural MSRs (Contd.)
		IA32_PERF_STATUS[15:0] is Current performance State Value
		[7:0] is 0x0, [15:8] looks like current ratio */
		err = cpu_rdmsr_range(handle, IA32_PERF_STATUS, 15, 8, &reg);
		if (!err) return (double) reg;
	}
	else if(id->vendor == VENDOR_AMD) {
		/* Refer links above
		MSRC001_0063[2:0] is CurPstate */
		err  = cpu_rdmsr_range(handle, MSR_PSTATE_S, 2, 0, &reg);
		err += get_amd_multipliers(handle, id, internal, MSR_PSTATE_0 + (uint32_t) reg, &reg);
		if (!err) return (double) reg;
	}

	return (double) CPU_INVALID_VALUE / 100;
}

static double get_info_max_multiplier(struct msr_driver_t* handle, struct cpu_id_t *id,
                                      struct internal_id_info_t *internal)
{
	int err;
	uint64_t reg;

	if(id->vendor == VENDOR_INTEL && internal->code.intel == PENTIUM) {
		err = cpu_rdmsr(handle, IA32_PERF_STATUS, &reg);
		if (!err) return (double) ((reg >> 40) & 0x1f);
	}
	else if(id->vendor == VENDOR_INTEL && internal->code.intel != PENTIUM) {
		/* Refer links above
		Table 35-10.  Specific MSRs Supported by Intel® Atom™ Processor C2000 Series with CPUID Signature 06_4DH
		Table 35-12.  MSRs in Next Generation Intel Atom Processors Based on the Goldmont Microarchitecture (Contd.)
		Table 35-13.  MSRs in Processors Based on Intel® Microarchitecture Code Name Nehalem (Contd.)
		Table 35-14.  Additional MSRs in Intel® Xeon® Processor 5500 and 3400 Series
		Table 35-16.  Additional MSRs Supported by Intel Processors (Based on Intel® Microarchitecture Code Name Westmere)
		Table 35-19.  MSRs Supported by 2nd Generation Intel® Core™ Processors (Intel® microarchitecture code name Sandy Bridge)
		Table 35-21.  Selected MSRs Supported by Intel® Xeon® Processors E5 Family (based on Sandy Bridge microarchitecture)
		Table 35-28.  MSRs Supported by 4th Generation Intel® Core™ Processors (Haswell microarchitecture) (Contd.)
		Table 35-30.  Additional MSRs Supported by Intel® Xeon® Processor E5 v3 Family
		Table 35-33.  Additional MSRs Supported by Intel® Core™ M Processors and 5th Generation Intel® Core™ Processors
		Table 35-34.  Additional MSRs Common to Intel® Xeon® Processor D and Intel Xeon Processors E5 v4 Family Based on the Broadwell Microarchitecture
		Table 35-37.  Additional MSRs Supported by 6th Generation Intel® Core™ Processors Based on Skylake Microarchitecture
		Table 35-40.  Selected MSRs Supported by Next Generation Intel® Xeon Phi™ Processors with DisplayFamily_DisplayModel Signature 06_57H
		MSR_TURBO_RATIO_LIMIT[7:0] is Maximum Ratio Limit for 1C */
		err = cpu_rdmsr_range(handle, MSR_TURBO_RATIO_LIMIT, 7, 0, &reg);
		if (!err) return (double) reg;
	}
	else if(id->vendor == VENDOR_AMD) {
		/* Refer links above
		MSRC001_0064 is Pb0
		Pb0 is the highest-performance boosted P-state */
		err = get_amd_multipliers(handle, id, internal, MSR_PSTATE_0, &reg);
		if (!err) return (double) reg;
	}

	return (double) CPU_INVALID_VALUE / 100;
}

static int get_info_temperature(struct msr_driver_t* handle, struct cpu_id_t *id,
                                struct internal_id_info_t *internal)
{
	int err;
	uint64_t DigitalReadout, ReadingValid, TemperatureTarget;

	if(id->vendor == VENDOR_INTEL) {
		/* Refer links above
		Table 35-2.   IA-32 Architectural MSRs
		IA32_THERM_STATUS[22:16] is Digital Readout
		IA32_THERM_STATUS[31]    is Reading Valid

		Table 35-6.   MSRs Common to the Silvermont Microarchitecture and Newer Microarchitectures for Intel® Atom
		Table 35-13.  MSRs in Processors Based on Intel® Microarchitecture Code Name Nehalem (Contd.)
		Table 35-18.  MSRs Supported by Intel® Processors based on Intel® microarchitecture code name Sandy Bridge (Contd.)
		Table 35-24.  MSRs Supported by Intel® Xeon® Processors E5 v2 Product Family (based on Ivy Bridge-E microarchitecture) (Contd.)
		Table 35-34.  Additional MSRs Common to Intel® Xeon® Processor D and Intel Xeon Processors E5 v4 Family Based on the Broadwell Microarchitecture
		Table 35-40.  Selected MSRs Supported by Next Generation Intel® Xeon Phi™ Processors with DisplayFamily_DisplayModel Signature 06_57H
		MSR_TEMPERATURE_TARGET[23:16] is Temperature Target */
		err  = cpu_rdmsr_range(handle, IA32_THERM_STATUS,      22, 16, &DigitalReadout);
		err += cpu_rdmsr_range(handle, IA32_THERM_STATUS,      31, 31, &ReadingValid);
		err += cpu_rdmsr_range(handle, MSR_TEMPERATURE_TARGET, 23, 16, &TemperatureTarget);
		if(!err && ReadingValid) return (int) (TemperatureTarget - DigitalReadout);
	}

	return CPU_INVALID_VALUE;
}

static double get_info_voltage(struct msr_driver_t* handle, struct cpu_id_t *id,
                               struct internal_id_info_t *internal)
{
	int err;
	uint64_t reg, CpuVid;

	if(id->vendor == VENDOR_INTEL) {
		/* Refer links above
		Table 35-18.  MSRs Supported by Intel® Processors based on Intel® microarchitecture code name Sandy Bridge (Contd.)
		MSR_PERF_STATUS[47:32] is Core Voltage
		P-state core voltage can be computed by MSR_PERF_STATUS[37:32] * (float) 1/(2^13). */
		err = cpu_rdmsr_range(handle, MSR_PERF_STATUS, 47, 32, &reg);
		if (!err) return (double) reg / (1 << 13);
	}
	else if(id->vendor == VENDOR_AMD) {
		/* Refer links above
		MSRC001_00[6B:64][15:9] is CpuVid
		MSRC001_0063[2:0] is P-state Status
		2.4.1.6.3 Serial VID (SVI) Encodings: voltage = 1.550V - 0.0125V * SviVid[6:0] */
		err  = cpu_rdmsr_range(handle, MSR_PSTATE_S,                   2, 0, &reg);
		err += cpu_rdmsr_range(handle, MSR_PSTATE_0 + (uint32_t) reg, 15, 9, &CpuVid);
		if (!err && MSR_PSTATE_0 + (uint32_t) reg <= MSR_PSTATE_7) return 1.550 - 0.0125 * CpuVid;
	}

	return (double) CPU_INVALID_VALUE / 100;
}

static double get_info_bus_clock(struct msr_driver_t* handle, struct cpu_id_t *id,
                                 struct internal_id_info_t *internal)
{
	int err;
	static int clock = 0;
	uint64_t reg;

	if(clock == 0)
		clock = cpu_clock_measure(100, 1);

	if(id->vendor == VENDOR_INTEL) {
		/* Refer links above
		Table 35-12.  MSRs in Next Generation Intel Atom Processors Based on the Goldmont Microarchitecture
		Table 35-13.  MSRs in Processors Based on Intel® Microarchitecture Code Name Nehalem
		Table 35-18.  MSRs Supported by Intel® Processors based on Intel® microarchitecture code name Sandy Bridge (Contd.)
		Table 35-23.  Additional MSRs Supported by 3rd Generation Intel® Core™ Processors (based on Intel® microarchitecture code name Ivy Bridge)
		Table 35-24.  MSRs Supported by Intel® Xeon® Processors E5 v2 Product Family (based on Ivy Bridge-E microarchitecture)
		Table 35-27.  Additional MSRs Supported by Processors based on the Haswell or Haswell-E microarchitectures
		Table 35-40.  Selected MSRs Supported by Next Generation Intel® Xeon Phi™ Processors with DisplayFamily_DisplayModel Signature 06_57H
		MSR_PLATFORM_INFO[15:8] is Maximum Non-Turbo Ratio */
		err = cpu_rdmsr_range(handle, MSR_PLATFORM_INFO, 15, 8, &reg);
		if (!err) return (double) clock / reg;
	}
	else if(id->vendor == VENDOR_AMD) {
		/* Refer links above
		MSRC001_0061[2:0] is CurPstateLimit
		CurPstateLimit is the highest-performance non-boosted P-state */
		err  = cpu_rdmsr_range(handle, MSR_PSTATE_L, 2, 0, &reg);
		err += get_amd_multipliers(handle, id, internal, MSR_PSTATE_0 + (uint32_t) reg, &reg);
		if (!err) return (double) clock / reg;
	}

	return (double) CPU_INVALID_VALUE / 100;
}

int cpu_rdmsr_range(struct msr_driver_t* handle, uint32_t msr_index, uint8_t highbit,
                    uint8_t lowbit, uint64_t* result)
{
	int err;
	const uint8_t bits = highbit - lowbit + 1;

	if(highbit > 63 || lowbit > highbit)
		return set_error(ERR_INVRANGE);

	err = cpu_rdmsr(handle, msr_index, result);

	if(!err && bits < 64) {
		/* Show only part of register */
		*result >>= lowbit;
		*result &= (1ULL << bits) - 1;
	}

	return err;
}

int cpu_msrinfo(struct msr_driver_t* handle, cpu_msrinfo_request_t which)
{
	struct cpu_raw_data_t raw;
	static struct cpu_id_t id;
	static struct internal_id_info_t internal;
	internal.score = -1;

	if (handle == NULL)
		return set_error(ERR_HANDLE);

	if (internal.score == -1) {
		cpuid_get_raw_data(&raw);
		cpu_ident_internal(&raw, &id, &internal);
	}

	switch (which) {
		case INFO_MPERF:
			return perfmsr_measure(handle, IA32_MPERF);
		case INFO_APERF:
			return perfmsr_measure(handle, IA32_APERF);
		case INFO_MIN_MULTIPLIER:
			return (int) (get_info_min_multiplier(handle, &id, &internal) * 100);
		case INFO_CUR_MULTIPLIER:
			return (int) (get_info_cur_multiplier(handle, &id, &internal) * 100);
		case INFO_MAX_MULTIPLIER:
			return (int) (get_info_max_multiplier(handle, &id, &internal) * 100);
		case INFO_TEMPERATURE:
			return get_info_temperature(handle, &id, &internal);
		case INFO_THROTTLING:
			return CPU_INVALID_VALUE;
		case INFO_VOLTAGE:
			return (int) (get_info_voltage(handle, &id, &internal) * 100);
		case INFO_BCLK:
		case INFO_BUS_CLOCK:
			return (int) (get_info_bus_clock(handle, &id, &internal) * 100);
		default:
			return CPU_INVALID_VALUE;
	}
}

#endif // RDMSR_UNSUPPORTED_OS
