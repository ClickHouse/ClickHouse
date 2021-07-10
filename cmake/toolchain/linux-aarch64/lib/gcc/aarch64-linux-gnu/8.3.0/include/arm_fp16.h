/* ARM FP16 scalar intrinsics include file.

   Copyright (C) 2016-2018 Free Software Foundation, Inc.
   Contributed by ARM Ltd.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published
   by the Free Software Foundation; either version 3, or (at your
   option) any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   Under Section 7 of GPL version 3, you are granted additional
   permissions described in the GCC Runtime Library Exception, version
   3.1, as published by the Free Software Foundation.

   You should have received a copy of the GNU General Public License and
   a copy of the GCC Runtime Library Exception along with this program;
   see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _AARCH64_FP16_H_
#define _AARCH64_FP16_H_

#include <stdint.h>

#pragma GCC push_options
#pragma GCC target ("arch=armv8.2-a+fp16")

typedef __fp16 float16_t;

/* ARMv8.2-A FP16 one operand scalar intrinsics.  */

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vabsh_f16 (float16_t __a)
{
  return __builtin_aarch64_abshf (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vceqzh_f16 (float16_t __a)
{
  return __builtin_aarch64_cmeqhf_uss (__a, 0.0f);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcgezh_f16 (float16_t __a)
{
  return __builtin_aarch64_cmgehf_uss (__a, 0.0f);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcgtzh_f16 (float16_t __a)
{
  return __builtin_aarch64_cmgthf_uss (__a, 0.0f);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vclezh_f16 (float16_t __a)
{
  return __builtin_aarch64_cmlehf_uss (__a, 0.0f);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcltzh_f16 (float16_t __a)
{
  return __builtin_aarch64_cmlthf_uss (__a, 0.0f);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_s16 (int16_t __a)
{
  return __builtin_aarch64_floathihf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_s32 (int32_t __a)
{
  return __builtin_aarch64_floatsihf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_s64 (int64_t __a)
{
  return __builtin_aarch64_floatdihf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_u16 (uint16_t __a)
{
  return __builtin_aarch64_floatunshihf_us (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_u32 (uint32_t __a)
{
  return __builtin_aarch64_floatunssihf_us (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_f16_u64 (uint64_t __a)
{
  return __builtin_aarch64_floatunsdihf_us (__a);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvth_s16_f16 (float16_t __a)
{
  return __builtin_aarch64_fix_trunchfhi (__a);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvth_s32_f16 (float16_t __a)
{
  return __builtin_aarch64_fix_trunchfsi (__a);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvth_s64_f16 (float16_t __a)
{
  return __builtin_aarch64_fix_trunchfdi (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvth_u16_f16 (float16_t __a)
{
  return __builtin_aarch64_fixuns_trunchfhi_us (__a);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvth_u32_f16 (float16_t __a)
{
  return __builtin_aarch64_fixuns_trunchfsi_us (__a);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvth_u64_f16 (float16_t __a)
{
  return __builtin_aarch64_fixuns_trunchfdi_us (__a);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvtah_s16_f16 (float16_t __a)
{
  return __builtin_aarch64_lroundhfhi (__a);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvtah_s32_f16 (float16_t __a)
{
  return __builtin_aarch64_lroundhfsi (__a);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvtah_s64_f16 (float16_t __a)
{
  return __builtin_aarch64_lroundhfdi (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvtah_u16_f16 (float16_t __a)
{
  return __builtin_aarch64_lrounduhfhi_us (__a);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvtah_u32_f16 (float16_t __a)
{
  return __builtin_aarch64_lrounduhfsi_us (__a);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvtah_u64_f16 (float16_t __a)
{
  return __builtin_aarch64_lrounduhfdi_us (__a);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvtmh_s16_f16 (float16_t __a)
{
  return __builtin_aarch64_lfloorhfhi (__a);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvtmh_s32_f16 (float16_t __a)
{
  return __builtin_aarch64_lfloorhfsi (__a);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvtmh_s64_f16 (float16_t __a)
{
  return __builtin_aarch64_lfloorhfdi (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvtmh_u16_f16 (float16_t __a)
{
  return __builtin_aarch64_lflooruhfhi_us (__a);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvtmh_u32_f16 (float16_t __a)
{
  return __builtin_aarch64_lflooruhfsi_us (__a);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvtmh_u64_f16 (float16_t __a)
{
  return __builtin_aarch64_lflooruhfdi_us (__a);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvtnh_s16_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnhfhi (__a);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvtnh_s32_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnhfsi (__a);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvtnh_s64_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnhfdi (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvtnh_u16_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnuhfhi_us (__a);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvtnh_u32_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnuhfsi_us (__a);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvtnh_u64_f16 (float16_t __a)
{
  return __builtin_aarch64_lfrintnuhfdi_us (__a);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvtph_s16_f16 (float16_t __a)
{
  return __builtin_aarch64_lceilhfhi (__a);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvtph_s32_f16 (float16_t __a)
{
  return __builtin_aarch64_lceilhfsi (__a);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvtph_s64_f16 (float16_t __a)
{
  return __builtin_aarch64_lceilhfdi (__a);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvtph_u16_f16 (float16_t __a)
{
  return __builtin_aarch64_lceiluhfhi_us (__a);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvtph_u32_f16 (float16_t __a)
{
  return __builtin_aarch64_lceiluhfsi_us (__a);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvtph_u64_f16 (float16_t __a)
{
  return __builtin_aarch64_lceiluhfdi_us (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vnegh_f16 (float16_t __a)
{
  return __builtin_aarch64_neghf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrecpeh_f16 (float16_t __a)
{
  return __builtin_aarch64_frecpehf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrecpxh_f16 (float16_t __a)
{
  return __builtin_aarch64_frecpxhf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndh_f16 (float16_t __a)
{
  return __builtin_aarch64_btrunchf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndah_f16 (float16_t __a)
{
  return __builtin_aarch64_roundhf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndih_f16 (float16_t __a)
{
  return __builtin_aarch64_nearbyinthf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndmh_f16 (float16_t __a)
{
  return __builtin_aarch64_floorhf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndnh_f16 (float16_t __a)
{
  return __builtin_aarch64_frintnhf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndph_f16 (float16_t __a)
{
  return __builtin_aarch64_ceilhf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrndxh_f16 (float16_t __a)
{
  return __builtin_aarch64_rinthf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrsqrteh_f16 (float16_t __a)
{
  return __builtin_aarch64_rsqrtehf (__a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vsqrth_f16 (float16_t __a)
{
  return __builtin_aarch64_sqrthf (__a);
}

/* ARMv8.2-A FP16 two operands scalar intrinsics.  */

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vaddh_f16 (float16_t __a, float16_t __b)
{
  return __a + __b;
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vabdh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fabdhf (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcageh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_facgehf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcagth_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_facgthf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcaleh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_faclehf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcalth_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_faclthf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vceqh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_cmeqhf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcgeh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_cmgehf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcgth_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_cmgthf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcleh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_cmlehf_uss (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vclth_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_cmlthf_uss (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_s16 (int16_t __a, const int __b)
{
  return __builtin_aarch64_scvtfhi (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_s32 (int32_t __a, const int __b)
{
  return __builtin_aarch64_scvtfsihf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_s64 (int64_t __a, const int __b)
{
  return __builtin_aarch64_scvtfdihf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_u16 (uint16_t __a, const int __b)
{
  return __builtin_aarch64_ucvtfhi_sus (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_u32 (uint32_t __a, const int __b)
{
  return __builtin_aarch64_ucvtfsihf_sus (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vcvth_n_f16_u64 (uint64_t __a, const int __b)
{
  return __builtin_aarch64_ucvtfdihf_sus (__a, __b);
}

__extension__ static __inline int16_t __attribute__ ((__always_inline__))
vcvth_n_s16_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzshf (__a, __b);
}

__extension__ static __inline int32_t __attribute__ ((__always_inline__))
vcvth_n_s32_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzshfsi (__a, __b);
}

__extension__ static __inline int64_t __attribute__ ((__always_inline__))
vcvth_n_s64_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzshfdi (__a, __b);
}

__extension__ static __inline uint16_t __attribute__ ((__always_inline__))
vcvth_n_u16_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzuhf_uss (__a, __b);
}

__extension__ static __inline uint32_t __attribute__ ((__always_inline__))
vcvth_n_u32_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzuhfsi_uss (__a, __b);
}

__extension__ static __inline uint64_t __attribute__ ((__always_inline__))
vcvth_n_u64_f16 (float16_t __a, const int __b)
{
  return __builtin_aarch64_fcvtzuhfdi_uss (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vdivh_f16 (float16_t __a, float16_t __b)
{
  return __a / __b;
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vmaxh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fmaxhf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vmaxnmh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fmaxhf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vminh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fminhf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vminnmh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fminhf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vmulh_f16 (float16_t __a, float16_t __b)
{
  return __a * __b;
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vmulxh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_fmulxhf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrecpsh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_frecpshf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vrsqrtsh_f16 (float16_t __a, float16_t __b)
{
  return __builtin_aarch64_rsqrtshf (__a, __b);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vsubh_f16 (float16_t __a, float16_t __b)
{
  return __a - __b;
}

/* ARMv8.2-A FP16 three operands scalar intrinsics.  */

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vfmah_f16 (float16_t __a, float16_t __b, float16_t __c)
{
  return __builtin_aarch64_fmahf (__b, __c, __a);
}

__extension__ static __inline float16_t __attribute__ ((__always_inline__))
vfmsh_f16 (float16_t __a, float16_t __b, float16_t __c)
{
  return __builtin_aarch64_fnmahf (__b, __c, __a);
}

#pragma GCC pop_options

#endif
