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
#ifndef __LIBCPUID_H__
#define __LIBCPUID_H__
/**
 * \file     libcpuid.h
 * \author   Veselin Georgiev
 * \date     Oct 2008
 * \version  0.4.0
 *
 * Version history:
 *
 * * 0.1.0 (2008-10-15): initial adaptation from wxfractgui sources
 * * 0.1.1 (2009-07-06): Added intel_fn11 fields to cpu_raw_data_t to handle
 *                       new processor topology enumeration required on Core i7
 * * 0.1.2 (2009-09-26): Added support for MSR reading through self-extracting
 *                       kernel driver on Win32.
 * * 0.1.3 (2010-04-20): Added support for greater more accurate CPU clock
 *                       measurements with cpu_clock_by_ic()
 * * 0.2.0 (2011-10-11): Support for AMD Bulldozer CPUs, 128-bit SSE unit size
 *                       checking. A backwards-incompatible change, since the
 *                       sizeof cpu_id_t is now different.
 * * 0.2.1 (2012-05-26): Support for Ivy Bridge, and detecting the presence of
 *                       the RdRand instruction.
 * * 0.2.2 (2015-11-04): Support for newer processors up to Haswell and Vishera.
 *                       Fix clock detection in cpu_clock_by_ic() for Bulldozer.
 *                       More entries supported in cpu_msrinfo().
 *                       *BSD and Solaris support (unofficial).
 * * 0.3.0 (2016-07-09): Support for Skylake; MSR ops in FreeBSD; INFO_VOLTAGE
 *                       for AMD CPUs. Level 4 cache support for Crystalwell
 *                       (a backwards-incompatible change since the sizeof
 *                        cpu_raw_data_t is now different).
 * * 0.4.0 (2016-09-30): Better detection of AMD clock multiplier with msrinfo.
 *                       Support for Intel SGX detection
 *                       (a backwards-incompatible change since the sizeof
 *                        cpu_raw_data_t and cpu_id_t is now different).
 */

/** @mainpage A simple libcpuid introduction
 *
 * LibCPUID provides CPU identification and access to the CPUID and RDTSC
 * instructions on the x86.
 * <p>
 * To execute CPUID, use \ref cpu_exec_cpuid <br>
 * To execute RDTSC, use \ref cpu_rdtsc <br>
 * To fetch the CPUID info needed for CPU identification, use
 *   \ref cpuid_get_raw_data <br>
 * To make sense of that data (decode, extract features), use \ref cpu_identify <br>
 * To detect the CPU speed, use either \ref cpu_clock, \ref cpu_clock_by_os,
 * \ref cpu_tsc_mark + \ref cpu_tsc_unmark + \ref cpu_clock_by_mark,
 * \ref cpu_clock_measure or \ref cpu_clock_by_ic.
 * Read carefully for pros/cons of each method. <br>
 * 
 * To read MSRs, use \ref cpu_msr_driver_open to get a handle, and then
 * \ref cpu_rdmsr for querying abilities. Some MSR decoding is available on recent
 * CPUs, and can be queried through \ref cpu_msrinfo; the various types of queries
 * are described in \ref cpu_msrinfo_request_t.
 * </p>
 */

/** @defgroup libcpuid LibCPUID
 @{ */

/* Include some integer type specifications: */
#include "libcpuid_types.h"

/* Some limits and other constants */
#include "libcpuid_constants.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief CPU vendor, as guessed from the Vendor String.
 */
typedef enum {
	VENDOR_INTEL = 0,  /*!< Intel CPU */
	VENDOR_AMD,        /*!< AMD CPU */
	VENDOR_CYRIX,      /*!< Cyrix CPU */
	VENDOR_NEXGEN,     /*!< NexGen CPU */
	VENDOR_TRANSMETA,  /*!< Transmeta CPU */
	VENDOR_UMC,        /*!< x86 CPU by UMC */
	VENDOR_CENTAUR,    /*!< x86 CPU by IDT */
	VENDOR_RISE,       /*!< x86 CPU by Rise Technology */
	VENDOR_SIS,        /*!< x86 CPU by SiS */
	VENDOR_NSC,        /*!< x86 CPU by National Semiconductor */
	
	NUM_CPU_VENDORS,   /*!< Valid CPU vendor ids: 0..NUM_CPU_VENDORS - 1 */
	VENDOR_UNKNOWN = -1,
} cpu_vendor_t;
#define NUM_CPU_VENDORS NUM_CPU_VENDORS

/**
 * @brief Contains just the raw CPUID data.
 *
 * This contains only the most basic CPU data, required to do identification
 * and feature recognition. Every processor should be identifiable using this
 * data only.
 */
struct cpu_raw_data_t {
	/** contains results of CPUID for eax = 0, 1, ...*/
	uint32_t basic_cpuid[MAX_CPUID_LEVEL][4];

	/** contains results of CPUID for eax = 0x80000000, 0x80000001, ...*/
	uint32_t ext_cpuid[MAX_EXT_CPUID_LEVEL][4];
	
	/** when the CPU is intel and it supports deterministic cache
	    information: this contains the results of CPUID for eax = 4
	    and ecx = 0, 1, ... */
	uint32_t intel_fn4[MAX_INTELFN4_LEVEL][4];
	
	/** when the CPU is intel and it supports leaf 0Bh (Extended Topology
	    enumeration leaf), this stores the result of CPUID with 
	    eax = 11 and ecx = 0, 1, 2... */
	uint32_t intel_fn11[MAX_INTELFN11_LEVEL][4];
	
	/** when the CPU is intel and supports leaf 12h (SGX enumeration leaf),
	 *  this stores the result of CPUID with eax = 0x12 and
	 *  ecx = 0, 1, 2... */
	uint32_t intel_fn12h[MAX_INTELFN12H_LEVEL][4];

	/** when the CPU is intel and supports leaf 14h (Intel Processor Trace
	 *  capabilities leaf).
	 *  this stores the result of CPUID with eax = 0x12 and
	 *  ecx = 0, 1, 2... */
	uint32_t intel_fn14h[MAX_INTELFN14H_LEVEL][4];
};

/**
 * @brief This contains information about SGX features of the processor
 * Example usage:
 * @code
 * ...
 * struct cpu_raw_data_t raw;
 * struct cpu_id_t id;
 * 
 * if (cpuid_get_raw_data(&raw) == 0 && cpu_identify(&raw, &id) == 0 && id.sgx.present) {
 *   printf("SGX is present.\n");
 *   printf("SGX1 instructions: %s.\n", id.sgx.flags[INTEL_SGX1] ? "present" : "absent");
 *   printf("SGX2 instructions: %s.\n", id.sgx.flags[INTEL_SGX2] ? "present" : "absent");
 *   printf("Max 32-bit enclave size: 2^%d bytes.\n", id.sgx.max_enclave_32bit);
 *   printf("Max 64-bit enclave size: 2^%d bytes.\n", id.sgx.max_enclave_64bit);
 *   for (int i = 0; i < id.sgx.num_epc_sections; i++) {
 *     struct cpu_epc_t epc = cpuid_get_epc(i, NULL);
 *     printf("EPC section #%d: address = %x, size = %d bytes.\n", epc.address, epc.size);
 *   }
 * } else {
 *   printf("SGX is not present.\n");
 * }
 * @endcode
 */ 
struct cpu_sgx_t {
	/** Whether SGX is present (boolean) */
	uint32_t present;
	
	/** Max enclave size in 32-bit mode. This is a power-of-two value:
	 *  if it is "31", then the max enclave size is 2^31 bytes (2 GiB).
	 */
	uint8_t max_enclave_32bit;
	
	/** Max enclave size in 64-bit mode. This is a power-of-two value:
	 *  if it is "36", then the max enclave size is 2^36 bytes (64 GiB).
	 */
	uint8_t max_enclave_64bit;
	
	/**
	 * contains SGX feature flags. See the \ref cpu_sgx_feature_t
	 * "INTEL_SGX*" macros below.
	 */
	uint8_t flags[SGX_FLAGS_MAX];
	
	/** number of Enclave Page Cache (EPC) sections. Info for each
	 *  section is available through the \ref cpuid_get_epc() function
	 */
	int num_epc_sections;
	
	/** bit vector of the supported extended  features that can be written
	 *  to the MISC region of the SSA (Save State Area)
	 */ 
	uint32_t misc_select;
	
	/** a bit vector of the attributes that can be set to SECS.ATTRIBUTES
	 *  via ECREATE. Corresponds to bits 0-63 (incl.) of SECS.ATTRIBUTES.
	 */ 
	uint64_t secs_attributes;
	
	/** a bit vector of the bits that can be set in the XSAVE feature
	 *  request mask; Corresponds to bits 64-127 of SECS.ATTRIBUTES.
	 */
	uint64_t secs_xfrm;
};

/**
 * @brief This contains the recognized CPU features/info
 */
struct cpu_id_t {
	/** contains the CPU vendor string, e.g. "GenuineIntel" */
	char vendor_str[VENDOR_STR_MAX];
	
	/** contains the brand string, e.g. "Intel(R) Xeon(TM) CPU 2.40GHz" */
	char brand_str[BRAND_STR_MAX];
	
	/** contains the recognized CPU vendor */
	cpu_vendor_t vendor;
	
	/**
	 * contain CPU flags. Used to test for features. See
	 * the \ref cpu_feature_t "CPU_FEATURE_*" macros below.
	 * @see Features
	 */
	uint8_t flags[CPU_FLAGS_MAX];
	
	/** CPU family */
	int32_t family;
	
	/** CPU model */
	int32_t model;
	
	/** CPU stepping */
	int32_t stepping;
	
	/** CPU extended family */
	int32_t ext_family;
	
	/** CPU extended model */
	int32_t ext_model;
	
	/** Number of CPU cores on the current processor */
	int32_t num_cores;
	
	/**
	 * Number of logical processors on the current processor.
	 * Could be more than the number of physical cores,
	 * e.g. when the processor has HyperThreading.
	 */
	int32_t num_logical_cpus;
	
	/**
	 * The total number of logical processors.
	 * The same value is availabe through \ref cpuid_get_total_cpus.
	 *
	 * This is num_logical_cpus * {total physical processors in the system}
	 * (but only on a real system, under a VM this number may be lower).
	 *
	 * If you're writing a multithreaded program and you want to run it on
	 * all CPUs, this is the number of threads you need.
	 *
	 * @note in a VM, this will exactly match the number of CPUs set in
	 *       the VM's configuration.
	 *
	 */
	int32_t total_logical_cpus;
	
	/**
	 * L1 data cache size in KB. Could be zero, if the CPU lacks cache.
	 * If the size cannot be determined, it will be -1.
	 */
	int32_t l1_data_cache;
	
	/**
	 * L1 instruction cache size in KB. Could be zero, if the CPU lacks
	 * cache. If the size cannot be determined, it will be -1.
	 * @note On some Intel CPUs, whose instruction cache is in fact
	 * a trace cache, the size will be expressed in K uOps.
	 */
	int32_t l1_instruction_cache;
	
	/**
	 * L2 cache size in KB. Could be zero, if the CPU lacks L2 cache.
	 * If the size of the cache could not be determined, it will be -1
	 */
	int32_t l2_cache;
	
	/** L3 cache size in KB. Zero on most systems */
	int32_t l3_cache;

	/** L4 cache size in KB. Zero on most systems */
	int32_t l4_cache;
	
	/** Cache associativity for the L1 data cache. -1 if undetermined */
	int32_t l1_assoc;
	
	/** Cache associativity for the L2 cache. -1 if undetermined */
	int32_t l2_assoc;
	
	/** Cache associativity for the L3 cache. -1 if undetermined */
	int32_t l3_assoc;

	/** Cache associativity for the L4 cache. -1 if undetermined */
	int32_t l4_assoc;
	
	/** Cache-line size for L1 data cache. -1 if undetermined */
	int32_t l1_cacheline;
	
	/** Cache-line size for L2 cache. -1 if undetermined */
	int32_t l2_cacheline;
	
	/** Cache-line size for L3 cache. -1 if undetermined */
	int32_t l3_cacheline;
	
	/** Cache-line size for L4 cache. -1 if undetermined */
	int32_t l4_cacheline;

	/**
	 * The brief and human-friendly CPU codename, which was recognized.<br>
	 * Examples:
	 * @code
	 * +--------+--------+-------+-------+-------+---------------------------------------+-----------------------+
	 * | Vendor | Family | Model | Step. | Cache |       Brand String                    | cpu_id_t.cpu_codename |
	 * +--------+--------+-------+-------+-------+---------------------------------------+-----------------------+
	 * | AMD    |      6 |     8 |     0 |   256 | (not available - will be ignored)     | "K6-2"                |
	 * | Intel  |     15 |     2 |     5 |   512 | "Intel(R) Xeon(TM) CPU 2.40GHz"       | "Xeon (Prestonia)"    |
	 * | Intel  |      6 |    15 |    11 |  4096 | "Intel(R) Core(TM)2 Duo CPU E6550..." | "Conroe (Core 2 Duo)" |
	 * | AMD    |     15 |    35 |     2 |  1024 | "Dual Core AMD Opteron(tm) Proces..." | "Opteron (Dual Core)" |
	 * +--------+--------+-------+-------+-------+---------------------------------------+-----------------------+
	 * @endcode
	 */
	char cpu_codename[64];
	
	/** SSE execution unit size (64 or 128; -1 if N/A) */
	int32_t sse_size;
	
	/**
	 * contain miscellaneous detection information. Used to test about specifics of
	 * certain detected features. See \ref cpu_hint_t "CPU_HINT_*" macros below.
	 * @see Hints
	 */
	uint8_t detection_hints[CPU_HINTS_MAX];
	
	/** contains information about SGX features if the processor, if present */
	struct cpu_sgx_t sgx;
};

/**
 * @brief CPU feature identifiers
 *
 * Usage:
 * @code
 * ...
 * struct cpu_raw_data_t raw;
 * struct cpu_id_t id;
 * if (cpuid_get_raw_data(&raw) == 0 && cpu_identify(&raw, &id) == 0) {
 *     if (id.flags[CPU_FEATURE_SSE2]) {
 *         // The CPU has SSE2...
 *         ...
 *     } else {
 *         // no SSE2
 *     }
 * } else {
 *   // processor cannot be determined.
 * }
 * @endcode
 */
typedef enum {
	CPU_FEATURE_FPU = 0,	/*!< Floating point unit */
	CPU_FEATURE_VME,	/*!< Virtual mode extension */
	CPU_FEATURE_DE,		/*!< Debugging extension */
	CPU_FEATURE_PSE,	/*!< Page size extension */
	CPU_FEATURE_TSC,	/*!< Time-stamp counter */
	CPU_FEATURE_MSR,	/*!< Model-specific regsisters, RDMSR/WRMSR supported */
	CPU_FEATURE_PAE,	/*!< Physical address extension */
	CPU_FEATURE_MCE,	/*!< Machine check exception */
	CPU_FEATURE_CX8,	/*!< CMPXCHG8B instruction supported */
	CPU_FEATURE_APIC,	/*!< APIC support */
	CPU_FEATURE_MTRR,	/*!< Memory type range registers */
	CPU_FEATURE_SEP,	/*!< SYSENTER / SYSEXIT instructions supported */
	CPU_FEATURE_PGE,	/*!< Page global enable */
	CPU_FEATURE_MCA,	/*!< Machine check architecture */
	CPU_FEATURE_CMOV,	/*!< CMOVxx instructions supported */
	CPU_FEATURE_PAT,	/*!< Page attribute table */
	CPU_FEATURE_PSE36,	/*!< 36-bit page address extension */
	CPU_FEATURE_PN,		/*!< Processor serial # implemented (Intel P3 only) */
	CPU_FEATURE_CLFLUSH,	/*!< CLFLUSH instruction supported */
	CPU_FEATURE_DTS,	/*!< Debug store supported */
	CPU_FEATURE_ACPI,	/*!< ACPI support (power states) */
	CPU_FEATURE_MMX,	/*!< MMX instruction set supported */
	CPU_FEATURE_FXSR,	/*!< FXSAVE / FXRSTOR supported */
	CPU_FEATURE_SSE,	/*!< Streaming-SIMD Extensions (SSE) supported */
	CPU_FEATURE_SSE2,	/*!< SSE2 instructions supported */
	CPU_FEATURE_SS,		/*!< Self-snoop */
	CPU_FEATURE_HT,		/*!< Hyper-threading supported (but might be disabled) */
	CPU_FEATURE_TM,		/*!< Thermal monitor */
	CPU_FEATURE_IA64,	/*!< IA64 supported (Itanium only) */
	CPU_FEATURE_PBE,	/*!< Pending-break enable */
	CPU_FEATURE_PNI,	/*!< PNI (SSE3) instructions supported */
	CPU_FEATURE_PCLMUL,	/*!< PCLMULQDQ instruction supported */
	CPU_FEATURE_DTS64,	/*!< 64-bit Debug store supported */
	CPU_FEATURE_MONITOR,	/*!< MONITOR / MWAIT supported */
	CPU_FEATURE_DS_CPL,	/*!< CPL Qualified Debug Store */
	CPU_FEATURE_VMX,	/*!< Virtualization technology supported */
	CPU_FEATURE_SMX,	/*!< Safer mode exceptions */
	CPU_FEATURE_EST,	/*!< Enhanced SpeedStep */
	CPU_FEATURE_TM2,	/*!< Thermal monitor 2 */
	CPU_FEATURE_SSSE3,	/*!< SSSE3 instructionss supported (this is different from SSE3!) */
	CPU_FEATURE_CID,	/*!< Context ID supported */
	CPU_FEATURE_CX16,	/*!< CMPXCHG16B instruction supported */
	CPU_FEATURE_XTPR,	/*!< Send Task Priority Messages disable */
	CPU_FEATURE_PDCM,	/*!< Performance capabilities MSR supported */
	CPU_FEATURE_DCA,	/*!< Direct cache access supported */
	CPU_FEATURE_SSE4_1,	/*!< SSE 4.1 instructions supported */
	CPU_FEATURE_SSE4_2,	/*!< SSE 4.2 instructions supported */
	CPU_FEATURE_SYSCALL,	/*!< SYSCALL / SYSRET instructions supported */
	CPU_FEATURE_XD,		/*!< Execute disable bit supported */
	CPU_FEATURE_MOVBE,	/*!< MOVBE instruction supported */
	CPU_FEATURE_POPCNT,	/*!< POPCNT instruction supported */
	CPU_FEATURE_AES,	/*!< AES* instructions supported */
	CPU_FEATURE_XSAVE,	/*!< XSAVE/XRSTOR/etc instructions supported */
	CPU_FEATURE_OSXSAVE,	/*!< non-privileged copy of OSXSAVE supported */
	CPU_FEATURE_AVX,	/*!< Advanced vector extensions supported */
	CPU_FEATURE_MMXEXT,	/*!< AMD MMX-extended instructions supported */
	CPU_FEATURE_3DNOW,	/*!< AMD 3DNow! instructions supported */
	CPU_FEATURE_3DNOWEXT,	/*!< AMD 3DNow! extended instructions supported */
	CPU_FEATURE_NX,		/*!< No-execute bit supported */
	CPU_FEATURE_FXSR_OPT,	/*!< FFXSR: FXSAVE and FXRSTOR optimizations */
	CPU_FEATURE_RDTSCP,	/*!< RDTSCP instruction supported (AMD-only) */
	CPU_FEATURE_LM,		/*!< Long mode (x86_64/EM64T) supported */
	CPU_FEATURE_LAHF_LM,	/*!< LAHF/SAHF supported in 64-bit mode */
	CPU_FEATURE_CMP_LEGACY,	/*!< core multi-processing legacy mode */
	CPU_FEATURE_SVM,	/*!< AMD Secure virtual machine */
	CPU_FEATURE_ABM,	/*!< LZCNT instruction support */
	CPU_FEATURE_MISALIGNSSE,/*!< Misaligned SSE supported */
	CPU_FEATURE_SSE4A,	/*!< SSE 4a from AMD */
	CPU_FEATURE_3DNOWPREFETCH,	/*!< PREFETCH/PREFETCHW support */
	CPU_FEATURE_OSVW,	/*!< OS Visible Workaround (AMD) */
	CPU_FEATURE_IBS,	/*!< Instruction-based sampling */
	CPU_FEATURE_SSE5,	/*!< SSE 5 instructions supported (deprecated, will never be 1) */
	CPU_FEATURE_SKINIT,	/*!< SKINIT / STGI supported */
	CPU_FEATURE_WDT,	/*!< Watchdog timer support */
	CPU_FEATURE_TS,		/*!< Temperature sensor */
	CPU_FEATURE_FID,	/*!< Frequency ID control */
	CPU_FEATURE_VID,	/*!< Voltage ID control */
	CPU_FEATURE_TTP,	/*!< THERMTRIP */
	CPU_FEATURE_TM_AMD,	/*!< AMD-specified hardware thermal control */
	CPU_FEATURE_STC,	/*!< Software thermal control */
	CPU_FEATURE_100MHZSTEPS,/*!< 100 MHz multiplier control */
	CPU_FEATURE_HWPSTATE,	/*!< Hardware P-state control */
	CPU_FEATURE_CONSTANT_TSC,	/*!< TSC ticks at constant rate */
	CPU_FEATURE_XOP,	/*!< The XOP instruction set (same as the old CPU_FEATURE_SSE5) */
	CPU_FEATURE_FMA3,	/*!< The FMA3 instruction set */
	CPU_FEATURE_FMA4,	/*!< The FMA4 instruction set */
	CPU_FEATURE_TBM,	/*!< Trailing bit manipulation instruction support */
	CPU_FEATURE_F16C,	/*!< 16-bit FP convert instruction support */
	CPU_FEATURE_RDRAND,     /*!< RdRand instruction */
	CPU_FEATURE_X2APIC,     /*!< x2APIC, APIC_BASE.EXTD, MSRs 0000_0800h...0000_0BFFh 64-bit ICR (+030h but not +031h), no DFR (+00Eh), SELF_IPI (+040h) also see standard level 0000_000Bh */
	CPU_FEATURE_CPB,	/*!< Core performance boost */
	CPU_FEATURE_APERFMPERF,	/*!< MPERF/APERF MSRs support */
	CPU_FEATURE_PFI,	/*!< Processor Feedback Interface support */
	CPU_FEATURE_PA,		/*!< Processor accumulator */
	CPU_FEATURE_AVX2,	/*!< AVX2 instructions */
	CPU_FEATURE_BMI1,	/*!< BMI1 instructions */
	CPU_FEATURE_BMI2,	/*!< BMI2 instructions */
	CPU_FEATURE_HLE,	/*!< Hardware Lock Elision prefixes */
	CPU_FEATURE_RTM,	/*!< Restricted Transactional Memory instructions */
	CPU_FEATURE_AVX512F,	/*!< AVX-512 Foundation */
	CPU_FEATURE_AVX512DQ,	/*!< AVX-512 Double/Quad granular insns */
	CPU_FEATURE_AVX512PF,	/*!< AVX-512 Prefetch */
	CPU_FEATURE_AVX512ER,	/*!< AVX-512 Exponential/Reciprocal */
	CPU_FEATURE_AVX512CD,	/*!< AVX-512 Conflict detection */
	CPU_FEATURE_SHA_NI,	/*!< SHA-1/SHA-256 instructions */
	CPU_FEATURE_AVX512BW,	/*!< AVX-512 Byte/Word granular insns */
	CPU_FEATURE_AVX512VL,	/*!< AVX-512 128/256 vector length extensions */
	CPU_FEATURE_SGX,	/*!< SGX extensions. Non-autoritative, check cpu_id_t::sgx::present to verify presence */
	CPU_FEATURE_RDSEED,	/*!< RDSEED instruction */
	CPU_FEATURE_ADX,	/*!< ADX extensions (arbitrary precision) */
	/* termination: */
	NUM_CPU_FEATURES,
} cpu_feature_t;

/**
 * @brief CPU detection hints identifiers
 *
 * Usage: similar to the flags usage
 */
typedef enum {
	CPU_HINT_SSE_SIZE_AUTH = 0,	/*!< SSE unit size is authoritative (not only a Family/Model guesswork, but based on an actual CPUID bit) */
	/* termination */
	NUM_CPU_HINTS,
} cpu_hint_t;

/**
 * @brief SGX features flags
 * \see cpu_sgx_t
 *
 * Usage:
 * @code
 * ...
 * struct cpu_raw_data_t raw;
 * struct cpu_id_t id;
 * if (cpuid_get_raw_data(&raw) == 0 && cpu_identify(&raw, &id) == 0 && id.sgx.present) {
 *     if (id.sgx.flags[INTEL_SGX1])
 *         // The CPU has SGX1 instructions support...
 *         ...
 *     } else {
 *         // no SGX
 *     }
 * } else {
 *   // processor cannot be determined.
 * }
 * @endcode
 */
 
typedef enum {
	INTEL_SGX1,		/*!< SGX1 instructions support */
	INTEL_SGX2,		/*!< SGX2 instructions support */
	
	/* termination: */
	NUM_SGX_FEATURES,
} cpu_sgx_feature_t;

/**
 * @brief Describes common library error codes
 */
typedef enum {
	ERR_OK       =  0,	/*!< "No error" */
	ERR_NO_CPUID = -1,	/*!< "CPUID instruction is not supported" */
	ERR_NO_RDTSC = -2,	/*!< "RDTSC instruction is not supported" */
	ERR_NO_MEM   = -3,	/*!< "Memory allocation failed" */
	ERR_OPEN     = -4,	/*!< "File open operation failed" */
	ERR_BADFMT   = -5,	/*!< "Bad file format" */
	ERR_NOT_IMP  = -6,	/*!< "Not implemented" */
	ERR_CPU_UNKN = -7,	/*!< "Unsupported processor" */
	ERR_NO_RDMSR = -8,	/*!< "RDMSR instruction is not supported" */
	ERR_NO_DRIVER= -9,	/*!< "RDMSR driver error (generic)" */
	ERR_NO_PERMS = -10,	/*!< "No permissions to install RDMSR driver" */
	ERR_EXTRACT  = -11,	/*!< "Cannot extract RDMSR driver (read only media?)" */
	ERR_HANDLE   = -12,	/*!< "Bad handle" */
	ERR_INVMSR   = -13,	/*!< "Invalid MSR" */
	ERR_INVCNB   = -14,	/*!< "Invalid core number" */
	ERR_HANDLE_R = -15,	/*!< "Error on handle read" */
	ERR_INVRANGE = -16,	/*!< "Invalid given range" */
} cpu_error_t;

/**
 * @brief Internal structure, used in cpu_tsc_mark, cpu_tsc_unmark and
 *        cpu_clock_by_mark
 */
struct cpu_mark_t {
	uint64_t tsc;		/*!< Time-stamp from RDTSC */
	uint64_t sys_clock;	/*!< In microsecond resolution */
};

/**
 * @brief Returns the total number of logical CPU threads (even if CPUID is not present).
 *
 * Under VM, this number (and total_logical_cpus, since they are fetched with the same code)
 * may be nonsensical, i.e. might not equal NumPhysicalCPUs*NumCoresPerCPU*HyperThreading.
 * This is because no matter how many logical threads the host machine has, you may limit them
 * in the VM to any number you like. **This** is the number returned by cpuid_get_total_cpus().
 *
 * @returns Number of logical CPU threads available. Equals the \ref cpu_id_t::total_logical_cpus.
 */
int cpuid_get_total_cpus(void);

/**
 * @brief Checks if the CPUID instruction is supported
 * @retval 1 if CPUID is present
 * @retval 0 the CPU doesn't have CPUID.
 */
int cpuid_present(void);

/**
 * @brief Executes the CPUID instruction
 * @param eax - the value of the EAX register when executing CPUID
 * @param regs - the results will be stored here. regs[0] = EAX, regs[1] = EBX, ...
 * @note CPUID will be executed with EAX set to the given value and EBX, ECX,
 *       EDX set to zero.
 */
void cpu_exec_cpuid(uint32_t eax, uint32_t* regs);

/**
 * @brief Executes the CPUID instruction with the given input registers
 * @note This is just a bit more generic version of cpu_exec_cpuid - it allows
 *       you to control all the registers.
 * @param regs - Input/output. Prior to executing CPUID, EAX, EBX, ECX and
 *               EDX will be set to regs[0], regs[1], regs[2] and regs[3].
 *               After CPUID, this array will contain the results.
 */
void cpu_exec_cpuid_ext(uint32_t* regs);

/**
 * @brief Obtains the raw CPUID data from the current CPU
 * @param data - a pointer to cpu_raw_data_t structure
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpuid_get_raw_data(struct cpu_raw_data_t* data);

/**
 * @brief Writes the raw CPUID data to a text file
 * @param data - a pointer to cpu_raw_data_t structure
 * @param filename - the path of the file, where the serialized data should be
 *                   written. If empty, stdout will be used.
 * @note This is intended primarily for debugging. On some processor, which is
 *       not currently supported or not completely recognized by cpu_identify,
 *       one can still successfully get the raw data and write it to a file.
 *       libcpuid developers can later import this file and debug the detection
 *       code as if running on the actual hardware.
 *       The file is simple text format of "something=value" pairs. Version info
 *       is also written, but the format is not intended to be neither backward-
 *       nor forward compatible.
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpuid_serialize_raw_data(struct cpu_raw_data_t* data, const char* filename);

/**
 * @brief Reads raw CPUID data from file
 * @param data - a pointer to cpu_raw_data_t structure. The deserialized data will
 *               be written here.
 * @param filename - the path of the file, containing the serialized raw data.
 *                   If empty, stdin will be used.
 * @note This function may fail, if the file is created by different version of
 *       the library. Also, see the notes on cpuid_serialize_raw_data.
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
*/
int cpuid_deserialize_raw_data(struct cpu_raw_data_t* data, const char* filename);

/**
 * @brief Identifies the CPU
 * @param raw - Input - a pointer to the raw CPUID data, which is obtained
 *              either by cpuid_get_raw_data or cpuid_deserialize_raw_data.
 *              Can also be NULL, in which case the functions calls
 *              cpuid_get_raw_data itself.
 * @param data - Output - the decoded CPU features/info is written here.
 * @note The function will not fail, even if some of the information
 *       cannot be obtained. Even when the CPU is new and thus unknown to
 *       libcpuid, some generic info, such as "AMD K9 family CPU" will be
 *       written to data.cpu_codename, and most other things, such as the
 *       CPU flags, cache sizes, etc. should be detected correctly anyway.
 *       However, the function CAN fail, if the CPU is completely alien to
 *       libcpuid.
 * @note While cpu_identify() and cpuid_get_raw_data() are fast for most
 *       purposes, running them several thousand times per second can hamper
 *       performance significantly. Specifically, avoid writing "cpu feature
 *       checker" wrapping function, which calls cpu_identify and returns the
 *       value of some flag, if that function is going to be called frequently.
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpu_identify(struct cpu_raw_data_t* raw, struct cpu_id_t* data);

/**
 * @brief Returns the short textual representation of a CPU flag
 * @param feature - the feature, whose textual representation is wanted.
 * @returns a constant string like "fpu", "tsc", "sse2", etc.
 * @note the names of the returned flags are compatible with those from
 *       /proc/cpuinfo in Linux, with the exception of `tm_amd'
 */
const char* cpu_feature_str(cpu_feature_t feature);

/**
 * @brief Returns textual description of the last error
 *
 * libcpuid stores an `errno'-style error status, whose description
 * can be obtained with this function.
 * @note This function is not thread-safe
 * @see cpu_error_t
 */
const char* cpuid_error(void);

/**
 * @brief Executes RDTSC
 *
 * The RDTSC (ReaD Time Stamp Counter) instruction gives access to an
 * internal 64-bit counter, which usually increments at each clock cycle.
 * This can be used for various timing routines, and as a very precise
 * clock source. It is set to zero on system startup. Beware that may not
 * increment at the same frequency as the CPU. Consecutive calls of RDTSC
 * are, however, guaranteed to return monotonically-increasing values.
 *
 * @param result - a pointer to a 64-bit unsigned integer, where the TSC value
 *                 will be stored
 *
 * @note  If 100% compatibility is a concern, you must first check if the
 * RDTSC instruction is present (if it is not, your program will crash
 * with "invalid opcode" exception). Only some very old processors (i486,
 * early AMD K5 and some Cyrix CPUs) lack that instruction - they should
 * have become exceedingly rare these days. To verify RDTSC presence,
 * run cpu_identify() and check flags[CPU_FEATURE_TSC].
 *
 * @note The monotonically increasing nature of the TSC may be violated
 * on SMP systems, if their TSC clocks run at different rate. If the OS
 * doesn't account for that, the TSC drift may become arbitrary large.
 */
void cpu_rdtsc(uint64_t* result);

/**
 * @brief Store TSC and timing info
 *
 * This function stores the current TSC value and current
 * time info from a precise OS-specific clock source in the cpu_mark_t
 * structure. The sys_clock field contains time with microsecond resolution.
 * The values can later be used to measure time intervals, number of clocks,
 * FPU frequency, etc.
 * @see cpu_rdtsc
 *
 * @param mark [out] - a pointer to a cpu_mark_t structure
 */
void cpu_tsc_mark(struct cpu_mark_t* mark);

/**
 * @brief Calculate TSC and timing difference
 *
 * @param mark - input/output: a pointer to a cpu_mark_t sturcture, which has
 *               already been initialized by cpu_tsc_mark. The difference in
 *               TSC and time will be written here.
 *
 * This function calculates the TSC and time difference, by obtaining the
 * current TSC and timing values and subtracting the contents of the `mark'
 * structure from them. Results are written in the same structure.
 *
 * Example:
 * @code
 * ...
 * struct cpu_mark_t mark;
 * cpu_tsc_mark(&mark);
 * foo();
 * cpu_tsc_unmark(&mark);
 * printf("Foo finished. Executed in %llu cycles and %llu usecs\n",
 *        mark.tsc, mark.sys_clock);
 * ...
 * @endcode
 */
void cpu_tsc_unmark(struct cpu_mark_t* mark);

/**
 * @brief Calculates the CPU clock
 *
 * @param mark - pointer to a cpu_mark_t structure, which has been initialized
 *   with cpu_tsc_mark and later `stopped' with cpu_tsc_unmark.
 *
 * @note For reliable results, the marked time interval should be at least about
 * 10 ms.
 *
 * @returns the CPU clock frequency, in MHz. Due to measurement error, it will
 * differ from the true value in a few least-significant bits. Accuracy depends
 * on the timing interval - the more, the better. If the timing interval is
 * insufficient, the result is -1. Also, see the comment on cpu_clock_measure
 * for additional issues and pitfalls in using RDTSC for CPU frequency
 * measurements.
 */
int cpu_clock_by_mark(struct cpu_mark_t* mark);

/**
 * @brief Returns the CPU clock, as reported by the OS
 *
 * This function uses OS-specific functions to obtain the CPU clock. It may
 * differ from the true clock for several reasons:<br><br>
 *
 * i) The CPU might be in some power saving state, while the OS reports its
 *    full-power frequency, or vice-versa.<br>
 * ii) In some cases you can raise or lower the CPU frequency with overclocking
 *     utilities and the OS will not notice.
 *
 * @returns the CPU clock frequency in MHz. If the OS is not (yet) supported
 * or lacks the necessary reporting machinery, the return value is -1
 */
int cpu_clock_by_os(void);

/**
 * @brief Measure the CPU clock frequency
 *
 * @param millis - How much time to waste in the busy-wait cycle. In millisecs.
 *                 Useful values 10 - 1000
 * @param quad_check - Do a more thorough measurement if nonzero
 *                     (see the explanation).
 *
 * The function performs a busy-wait cycle for the given time and calculates
 * the CPU frequency by the difference of the TSC values. The accuracy of the
 * calculation depends on the length of the busy-wait cycle: more is better,
 * but 100ms should be enough for most purposes.
 *
 * While this will calculate the CPU frequency correctly in most cases, there are
 * several reasons why it might be incorrect:<br>
 *
 * i) RDTSC doesn't guarantee it will run at the same clock as the CPU.
 *    Apparently there aren't CPUs at the moment, but still, there's no
 *    guarantee.<br>
 * ii) The CPU might be in a low-frequency power saving mode, and the CPU
 *     might be switched to higher frequency at any time. If this happens
 *     during the measurement, the result can be anywhere between the
 *     low and high frequencies. Also, if you're interested in the
 *     high frequency value only, this function might return the low one
 *     instead.<br>
 * iii) On SMP systems exhibiting TSC drift (see \ref cpu_rdtsc)
 *
 * the quad_check option will run four consecutive measurements and
 * then return the average of the two most-consistent results. The total
 * runtime of the function will still be `millis' - consider using
 * a bit more time for the timing interval.
 *
 * Finally, for benchmarking / CPU intensive applications, the best strategy is
 * to use the cpu_tsc_mark() / cpu_tsc_unmark() / cpu_clock_by_mark() method.
 * Begin by mark()-ing about one second after application startup (allowing the
 * power-saving manager to kick in and rise the frequency during that time),
 * then unmark() just before application finishing. The result will most
 * acurately represent at what frequency your app was running.
 *
 * @returns the CPU clock frequency in MHz (within some measurement error
 * margin). If RDTSC is not supported, the result is -1.
 */
int cpu_clock_measure(int millis, int quad_check);

/**
 * @brief Measure the CPU clock frequency using instruction-counting
 *
 * @param millis - how much time to allocate for each run, in milliseconds
 * @param runs - how many runs to perform
 *
 * The function performs a busy-wait cycle using a known number of "heavy" (SSE)
 * instructions. These instructions run at (more or less guaranteed) 1 IPC rate,
 * so by running a busy loop for a fixed amount of time, and measuring the
 * amount of instructions done, the CPU clock is accurately measured.
 *
 * Of course, this function is still affected by the power-saving schemes, so
 * the warnings as of cpu_clock_measure() still apply. However, this function is
 * immune to problems with detection, related to the Intel Nehalem's "Turbo"
 * mode, where the internal clock is raised, but the RDTSC rate is unaffected.
 *
 * The function will run for about (millis * runs) milliseconds.
 * You can make only a single busy-wait run (runs == 1); however, this can
 * be affected by task scheduling (which will break the counting), so allowing
 * more than one run is recommended. As run length is not imperative for
 * accurate readings (e.g., 50ms is sufficient), you can afford a lot of short
 * runs, e.g. 10 runs of 50ms or 20 runs of 25ms.
 *
 * Recommended values - millis = 50, runs = 4. For more robustness,
 * increase the number of runs.
 * 
 * NOTE: on Bulldozer and later CPUs, the busy-wait cycle runs at 1.4 IPC, thus
 * the results are skewed. This is corrected internally by dividing the resulting
 * value by 1.4.
 * However, this only occurs if the thread is executed on a single CMT
 * module - if there are other threads competing for resources, the results are
 * unpredictable. Make sure you run cpu_clock_by_ic() on a CPU that is free from
 * competing threads, or if there are such threads, they shouldn't exceed the
 * number of modules. On a Bulldozer X8, that means 4 threads.
 *
 * @returns the CPU clock frequency in MHz (within some measurement error
 * margin). If SSE is not supported, the result is -1. If the input parameters
 * are incorrect, or some other internal fault is detected, the result is -2.
 */
int cpu_clock_by_ic(int millis, int runs);

/**
 * @brief Get the CPU clock frequency (all-in-one method)
 *
 * This is an all-in-one method for getting the CPU clock frequency.
 * It tries to use the OS for that. If the OS doesn't have this info, it
 * uses cpu_clock_measure with 200ms time interval and quadruple checking.
 *
 * @returns the CPU clock frequency in MHz. If every possible method fails,
 * the result is -1.
 */
int cpu_clock(void);


/**
 * @brief The return value of cpuid_get_epc().
 * @details
 * Describes an EPC (Enclave Page Cache) layout (physical address and size).
 * A CPU may have one or more EPC areas, and information about each is
 * fetched via \ref cpuid_get_epc.
 */ 
struct cpu_epc_t {
	uint64_t start_addr;
	uint64_t length;
};

/**
 * @brief Fetches information about an EPC (Enclave Page Cache) area.
 * @param index - zero-based index, valid range [0..cpu_id_t.egx.num_epc_sections)
 * @param raw   - a pointer to fetched raw CPUID data. Needed only for testing,
 *                you can safely pass NULL here (if you pass a real structure,
 *                it will be used for fetching the leaf 12h data if index < 2;
 *                otherwise the real CPUID instruction will be used).
 * @returns the requested data. If the CPU doesn't support SGX, or if
 *          index >= cpu_id_t.egx.num_epc_sections, both fields of the returned
 *          structure will be zeros.
 */
struct cpu_epc_t cpuid_get_epc(int index, const struct cpu_raw_data_t* raw);

/**
 * @brief Returns the libcpuid version
 *
 * @returns the string representation of the libcpuid version, like "0.1.1"
 */
const char* cpuid_lib_version(void);

typedef void (*libcpuid_warn_fn_t) (const char *msg);
/**
 * @brief Sets the warning print function
 *
 * In some cases, the internal libcpuid machinery would like to emit useful
 * debug warnings. By default, these warnings are written to stderr. However,
 * you can set a custom function that will receive those warnings.
 *
 * @param warn_fun - the warning function you want to set. If NULL, warnings
 *                   are disabled. The function takes const char* argument.
 *
 * @returns the current warning function. You can use the return value to
 * keep the previous warning function and restore it at your discretion.
 */
libcpuid_warn_fn_t cpuid_set_warn_function(libcpuid_warn_fn_t warn_fun);

/**
 * @brief Sets the verbosiness level
 *
 * When the verbosiness level is above zero, some functions might print
 * diagnostic information about what are they doing. The higher the level is,
 * the more detail is printed. Level zero is guaranteed to omit all such
 * output. The output is written using the same machinery as the warnings,
 * @see cpuid_set_warn_function()
 *
 * @param level the desired verbosiness level. Useful values 0..2 inclusive
 */
void cpuid_set_verbosiness_level(int level);


/**
 * @brief Obtains the CPU vendor from CPUID from the current CPU
 * @note The result is cached.
 * @returns VENDOR_UNKNOWN if failed, otherwise the CPU vendor type.
 *          @see cpu_vendor_t
 */
cpu_vendor_t cpuid_get_vendor(void);

/**
 * @brief a structure that holds a list of processor names
 */
struct cpu_list_t {
	/** Number of entries in the list */
	int num_entries;
	/** Pointers to names. There will be num_entries of them */
	char **names;
};

/**
 * @brief Gets a list of all known CPU names from a specific vendor.
 *
 * This function compiles a list of all known CPU (code)names
 * (i.e. the possible values of cpu_id_t::cpu_codename) for the given vendor.
 *
 * There are about 100 entries for Intel and AMD, and a few for the other
 * vendors. The list is written out in approximate chronological introduction
 * order of the parts.
 *
 * @param vendor the vendor to be queried
 * @param list [out] the resulting list will be written here.
 * NOTE: As the memory is dynamically allocated, be sure to call
 *       cpuid_free_cpu_list() after you're done with the data
 * @see cpu_list_t
 */
void cpuid_get_cpu_list(cpu_vendor_t vendor, struct cpu_list_t* list);

/**
 * @brief Frees a CPU list
 *
 * This function deletes all the memory associated with a CPU list, as obtained
 * by cpuid_get_cpu_list()
 *
 * @param list - the list to be free()'d.
 */
void cpuid_free_cpu_list(struct cpu_list_t* list);

struct msr_driver_t;
/**
 * @brief Starts/opens a driver, needed to read MSRs (Model Specific Registers)
 *
 * On systems that support it, this function will create a temporary
 * system driver, that has privileges to execute the RDMSR instruction.
 * After the driver is created, you can read MSRs by calling \ref cpu_rdmsr
 *
 * @returns a handle to the driver on success, and NULL on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
struct msr_driver_t* cpu_msr_driver_open(void);

/**
 * @brief Similar to \ref cpu_msr_driver_open, but accept one parameter
 *
 * This function works on certain operating systems (GNU/Linux, FreeBSD)
 *
 * @param core_num specify the core number for MSR.
 *          The first core number is 0.
 *          The last core number is \ref cpuid_get_total_cpus - 1.
 *
 * @returns a handle to the driver on success, and NULL on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
struct msr_driver_t* cpu_msr_driver_open_core(unsigned core_num);

/**
 * @brief Reads a Model-Specific Register (MSR)
 *
 * If the CPU has MSRs (as indicated by the CPU_FEATURE_MSR flag), you can
 * read a MSR with the given index by calling this function.
 *
 * There are several prerequisites you must do before reading MSRs:
 * 1) You must ensure the CPU has RDMSR. Check the CPU_FEATURE_MSR flag
 *    in cpu_id_t::flags
 * 2) You must ensure that the CPU implements the specific MSR you intend to
 *    read.
 * 3) You must open a MSR-reader driver. RDMSR is a privileged instruction and
 *    needs ring-0 access in order to work. This temporary driver is created
 *    by calling \ref cpu_msr_driver_open
 *
 * @param handle - a handle to the MSR reader driver, as created by
 *                 cpu_msr_driver_open
 * @param msr_index - the numeric ID of the MSR you want to read
 * @param result - a pointer to a 64-bit integer, where the MSR value is stored
 *
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpu_rdmsr(struct msr_driver_t* handle, uint32_t msr_index, uint64_t* result);


typedef enum {
	INFO_MPERF,                /*!< Maximum performance frequency clock. This
                                    is a counter, which increments as a
                                    proportion of the actual processor speed. */
	INFO_APERF,                /*!< Actual performance frequency clock. This
                                    accumulates the core clock counts when the
                                    core is active. */
	INFO_MIN_MULTIPLIER,       /*!< Minimum CPU:FSB ratio for this CPU,
                                    multiplied by 100. */
	INFO_CUR_MULTIPLIER,       /*!< Current CPU:FSB ratio, multiplied by 100.
                                    e.g., a CPU:FSB value of 18.5 reads as
                                    "1850". */
	INFO_MAX_MULTIPLIER,       /*!< Maximum CPU:FSB ratio for this CPU,
                                    multiplied by 100. */
	INFO_TEMPERATURE,          /*!< The current core temperature in Celsius. */
	INFO_THROTTLING,           /*!< 1 if the current logical processor is
                                    throttling. 0 if it is running normally. */
	INFO_VOLTAGE,              /*!< The current core voltage in Volt,
	                            multiplied by 100. */
	INFO_BCLK,                 /*!< See \ref INFO_BUS_CLOCK. */
	INFO_BUS_CLOCK,            /*!< The main bus clock in MHz,
	                            e.g., FSB/QPI/DMI/HT base clock,
	                            multiplied by 100. */
} cpu_msrinfo_request_t;

/**
 * @brief Similar to \ref cpu_rdmsr, but extract a range of bits
 *
 * @param handle - a handle to the MSR reader driver, as created by
 *                 cpu_msr_driver_open
 * @param msr_index - the numeric ID of the MSR you want to read
 * @param highbit - the high bit in range, must be inferior to 64
 * @param lowbit - the low bit in range, must be equal or superior to 0
 * @param result - a pointer to a 64-bit integer, where the MSR value is stored
 *
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpu_rdmsr_range(struct msr_driver_t* handle, uint32_t msr_index, uint8_t highbit,
                    uint8_t lowbit, uint64_t* result);

/**
 * @brief Reads extended CPU information from Model-Specific Registers.
 * @param handle - a handle to an open MSR driver, @see cpu_msr_driver_open
 * @param which - which info field should be returned. A list of
 *                available information entities is listed in the
 *                cpu_msrinfo_request_t enum.
 * @retval - if the requested information is available for the current
 *           processor model, the respective value is returned.
 *           if no information is available, or the CPU doesn't support
 *           the query, the special value CPU_INVALID_VALUE is returned
 */
int cpu_msrinfo(struct msr_driver_t* handle, cpu_msrinfo_request_t which);
#define CPU_INVALID_VALUE 0x3fffffff

/**
 * @brief Closes an open MSR driver
 *
 * This function unloads the MSR driver opened by cpu_msr_driver_open and
 * frees any resources associated with it.
 *
 * @param handle - a handle to the MSR reader driver, as created by
 *                 cpu_msr_driver_open
 *
 * @returns zero if successful, and some negative number on error.
 *          The error message can be obtained by calling \ref cpuid_error.
 *          @see cpu_error_t
 */
int cpu_msr_driver_close(struct msr_driver_t* handle);

#ifdef __cplusplus
}; /* extern "C" */
#endif


/** @} */

#endif /* __LIBCPUID_H__ */
