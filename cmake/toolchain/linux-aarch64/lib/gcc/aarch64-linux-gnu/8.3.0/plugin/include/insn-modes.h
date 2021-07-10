/* Generated automatically from machmode.def and config/aarch64/aarch64-modes.def
   by genmodes.  */

#ifndef GCC_INSN_MODES_H
#define GCC_INSN_MODES_H

enum machine_mode
{
  E_VOIDmode,              /* machmode.def:189 */
#define HAVE_VOIDmode
#ifdef USE_ENUM_MODES
#define VOIDmode E_VOIDmode
#else
#define VOIDmode ((void) 0, E_VOIDmode)
#endif
  E_BLKmode,               /* machmode.def:193 */
#define HAVE_BLKmode
#ifdef USE_ENUM_MODES
#define BLKmode E_BLKmode
#else
#define BLKmode ((void) 0, E_BLKmode)
#endif
  E_CCmode,                /* machmode.def:231 */
#define HAVE_CCmode
#ifdef USE_ENUM_MODES
#define CCmode E_CCmode
#else
#define CCmode ((void) 0, E_CCmode)
#endif
  E_CCFPmode,              /* config/aarch64/aarch64-modes.def:21 */
#define HAVE_CCFPmode
#ifdef USE_ENUM_MODES
#define CCFPmode E_CCFPmode
#else
#define CCFPmode ((void) 0, E_CCFPmode)
#endif
  E_CCFPEmode,             /* config/aarch64/aarch64-modes.def:22 */
#define HAVE_CCFPEmode
#ifdef USE_ENUM_MODES
#define CCFPEmode E_CCFPEmode
#else
#define CCFPEmode ((void) 0, E_CCFPEmode)
#endif
  E_CC_SWPmode,            /* config/aarch64/aarch64-modes.def:23 */
#define HAVE_CC_SWPmode
#ifdef USE_ENUM_MODES
#define CC_SWPmode E_CC_SWPmode
#else
#define CC_SWPmode ((void) 0, E_CC_SWPmode)
#endif
  E_CC_NZmode,             /* config/aarch64/aarch64-modes.def:24 */
#define HAVE_CC_NZmode
#ifdef USE_ENUM_MODES
#define CC_NZmode E_CC_NZmode
#else
#define CC_NZmode ((void) 0, E_CC_NZmode)
#endif
  E_CC_Zmode,              /* config/aarch64/aarch64-modes.def:25 */
#define HAVE_CC_Zmode
#ifdef USE_ENUM_MODES
#define CC_Zmode E_CC_Zmode
#else
#define CC_Zmode ((void) 0, E_CC_Zmode)
#endif
  E_CC_Cmode,              /* config/aarch64/aarch64-modes.def:26 */
#define HAVE_CC_Cmode
#ifdef USE_ENUM_MODES
#define CC_Cmode E_CC_Cmode
#else
#define CC_Cmode ((void) 0, E_CC_Cmode)
#endif
  E_BImode,                /* machmode.def:196 */
#define HAVE_BImode
#ifdef USE_ENUM_MODES
#define BImode E_BImode
#else
#define BImode (scalar_int_mode ((scalar_int_mode::from_int) E_BImode))
#endif
  E_QImode,                /* machmode.def:204 */
#define HAVE_QImode
#ifdef USE_ENUM_MODES
#define QImode E_QImode
#else
#define QImode (scalar_int_mode ((scalar_int_mode::from_int) E_QImode))
#endif
  E_HImode,                /* machmode.def:205 */
#define HAVE_HImode
#ifdef USE_ENUM_MODES
#define HImode E_HImode
#else
#define HImode (scalar_int_mode ((scalar_int_mode::from_int) E_HImode))
#endif
  E_SImode,                /* machmode.def:206 */
#define HAVE_SImode
#ifdef USE_ENUM_MODES
#define SImode E_SImode
#else
#define SImode (scalar_int_mode ((scalar_int_mode::from_int) E_SImode))
#endif
  E_DImode,                /* machmode.def:207 */
#define HAVE_DImode
#ifdef USE_ENUM_MODES
#define DImode E_DImode
#else
#define DImode (scalar_int_mode ((scalar_int_mode::from_int) E_DImode))
#endif
  E_TImode,                /* machmode.def:208 */
#define HAVE_TImode
#ifdef USE_ENUM_MODES
#define TImode E_TImode
#else
#define TImode (scalar_int_mode ((scalar_int_mode::from_int) E_TImode))
#endif
  E_OImode,                /* config/aarch64/aarch64-modes.def:57 */
#define HAVE_OImode
#ifdef USE_ENUM_MODES
#define OImode E_OImode
#else
#define OImode (scalar_int_mode ((scalar_int_mode::from_int) E_OImode))
#endif
  E_CImode,                /* config/aarch64/aarch64-modes.def:61 */
#define HAVE_CImode
#ifdef USE_ENUM_MODES
#define CImode E_CImode
#else
#define CImode (scalar_int_mode ((scalar_int_mode::from_int) E_CImode))
#endif
  E_XImode,                /* config/aarch64/aarch64-modes.def:62 */
#define HAVE_XImode
#ifdef USE_ENUM_MODES
#define XImode E_XImode
#else
#define XImode (scalar_int_mode ((scalar_int_mode::from_int) E_XImode))
#endif
  E_QQmode,                /* machmode.def:234 */
#define HAVE_QQmode
#ifdef USE_ENUM_MODES
#define QQmode E_QQmode
#else
#define QQmode (scalar_mode ((scalar_mode::from_int) E_QQmode))
#endif
  E_HQmode,                /* machmode.def:235 */
#define HAVE_HQmode
#ifdef USE_ENUM_MODES
#define HQmode E_HQmode
#else
#define HQmode (scalar_mode ((scalar_mode::from_int) E_HQmode))
#endif
  E_SQmode,                /* machmode.def:236 */
#define HAVE_SQmode
#ifdef USE_ENUM_MODES
#define SQmode E_SQmode
#else
#define SQmode (scalar_mode ((scalar_mode::from_int) E_SQmode))
#endif
  E_DQmode,                /* machmode.def:237 */
#define HAVE_DQmode
#ifdef USE_ENUM_MODES
#define DQmode E_DQmode
#else
#define DQmode (scalar_mode ((scalar_mode::from_int) E_DQmode))
#endif
  E_TQmode,                /* machmode.def:238 */
#define HAVE_TQmode
#ifdef USE_ENUM_MODES
#define TQmode E_TQmode
#else
#define TQmode (scalar_mode ((scalar_mode::from_int) E_TQmode))
#endif
  E_UQQmode,               /* machmode.def:240 */
#define HAVE_UQQmode
#ifdef USE_ENUM_MODES
#define UQQmode E_UQQmode
#else
#define UQQmode (scalar_mode ((scalar_mode::from_int) E_UQQmode))
#endif
  E_UHQmode,               /* machmode.def:241 */
#define HAVE_UHQmode
#ifdef USE_ENUM_MODES
#define UHQmode E_UHQmode
#else
#define UHQmode (scalar_mode ((scalar_mode::from_int) E_UHQmode))
#endif
  E_USQmode,               /* machmode.def:242 */
#define HAVE_USQmode
#ifdef USE_ENUM_MODES
#define USQmode E_USQmode
#else
#define USQmode (scalar_mode ((scalar_mode::from_int) E_USQmode))
#endif
  E_UDQmode,               /* machmode.def:243 */
#define HAVE_UDQmode
#ifdef USE_ENUM_MODES
#define UDQmode E_UDQmode
#else
#define UDQmode (scalar_mode ((scalar_mode::from_int) E_UDQmode))
#endif
  E_UTQmode,               /* machmode.def:244 */
#define HAVE_UTQmode
#ifdef USE_ENUM_MODES
#define UTQmode E_UTQmode
#else
#define UTQmode (scalar_mode ((scalar_mode::from_int) E_UTQmode))
#endif
  E_HAmode,                /* machmode.def:246 */
#define HAVE_HAmode
#ifdef USE_ENUM_MODES
#define HAmode E_HAmode
#else
#define HAmode (scalar_mode ((scalar_mode::from_int) E_HAmode))
#endif
  E_SAmode,                /* machmode.def:247 */
#define HAVE_SAmode
#ifdef USE_ENUM_MODES
#define SAmode E_SAmode
#else
#define SAmode (scalar_mode ((scalar_mode::from_int) E_SAmode))
#endif
  E_DAmode,                /* machmode.def:248 */
#define HAVE_DAmode
#ifdef USE_ENUM_MODES
#define DAmode E_DAmode
#else
#define DAmode (scalar_mode ((scalar_mode::from_int) E_DAmode))
#endif
  E_TAmode,                /* machmode.def:249 */
#define HAVE_TAmode
#ifdef USE_ENUM_MODES
#define TAmode E_TAmode
#else
#define TAmode (scalar_mode ((scalar_mode::from_int) E_TAmode))
#endif
  E_UHAmode,               /* machmode.def:251 */
#define HAVE_UHAmode
#ifdef USE_ENUM_MODES
#define UHAmode E_UHAmode
#else
#define UHAmode (scalar_mode ((scalar_mode::from_int) E_UHAmode))
#endif
  E_USAmode,               /* machmode.def:252 */
#define HAVE_USAmode
#ifdef USE_ENUM_MODES
#define USAmode E_USAmode
#else
#define USAmode (scalar_mode ((scalar_mode::from_int) E_USAmode))
#endif
  E_UDAmode,               /* machmode.def:253 */
#define HAVE_UDAmode
#ifdef USE_ENUM_MODES
#define UDAmode E_UDAmode
#else
#define UDAmode (scalar_mode ((scalar_mode::from_int) E_UDAmode))
#endif
  E_UTAmode,               /* machmode.def:254 */
#define HAVE_UTAmode
#ifdef USE_ENUM_MODES
#define UTAmode E_UTAmode
#else
#define UTAmode (scalar_mode ((scalar_mode::from_int) E_UTAmode))
#endif
  E_HFmode,                /* config/aarch64/aarch64-modes.def:29 */
#define HAVE_HFmode
#ifdef USE_ENUM_MODES
#define HFmode E_HFmode
#else
#define HFmode (scalar_float_mode ((scalar_float_mode::from_int) E_HFmode))
#endif
  E_SFmode,                /* machmode.def:226 */
#define HAVE_SFmode
#ifdef USE_ENUM_MODES
#define SFmode E_SFmode
#else
#define SFmode (scalar_float_mode ((scalar_float_mode::from_int) E_SFmode))
#endif
  E_DFmode,                /* machmode.def:227 */
#define HAVE_DFmode
#ifdef USE_ENUM_MODES
#define DFmode E_DFmode
#else
#define DFmode (scalar_float_mode ((scalar_float_mode::from_int) E_DFmode))
#endif
  E_TFmode,                /* config/aarch64/aarch64-modes.def:96 */
#define HAVE_TFmode
#ifdef USE_ENUM_MODES
#define TFmode E_TFmode
#else
#define TFmode (scalar_float_mode ((scalar_float_mode::from_int) E_TFmode))
#endif
  E_SDmode,                /* machmode.def:267 */
#define HAVE_SDmode
#ifdef USE_ENUM_MODES
#define SDmode E_SDmode
#else
#define SDmode (scalar_float_mode ((scalar_float_mode::from_int) E_SDmode))
#endif
  E_DDmode,                /* machmode.def:268 */
#define HAVE_DDmode
#ifdef USE_ENUM_MODES
#define DDmode E_DDmode
#else
#define DDmode (scalar_float_mode ((scalar_float_mode::from_int) E_DDmode))
#endif
  E_TDmode,                /* machmode.def:269 */
#define HAVE_TDmode
#ifdef USE_ENUM_MODES
#define TDmode E_TDmode
#else
#define TDmode (scalar_float_mode ((scalar_float_mode::from_int) E_TDmode))
#endif
  E_CQImode,               /* machmode.def:262 */
#define HAVE_CQImode
#ifdef USE_ENUM_MODES
#define CQImode E_CQImode
#else
#define CQImode (complex_mode ((complex_mode::from_int) E_CQImode))
#endif
  E_CHImode,               /* machmode.def:262 */
#define HAVE_CHImode
#ifdef USE_ENUM_MODES
#define CHImode E_CHImode
#else
#define CHImode (complex_mode ((complex_mode::from_int) E_CHImode))
#endif
  E_CSImode,               /* machmode.def:262 */
#define HAVE_CSImode
#ifdef USE_ENUM_MODES
#define CSImode E_CSImode
#else
#define CSImode (complex_mode ((complex_mode::from_int) E_CSImode))
#endif
  E_CDImode,               /* machmode.def:262 */
#define HAVE_CDImode
#ifdef USE_ENUM_MODES
#define CDImode E_CDImode
#else
#define CDImode (complex_mode ((complex_mode::from_int) E_CDImode))
#endif
  E_CTImode,               /* machmode.def:262 */
#define HAVE_CTImode
#ifdef USE_ENUM_MODES
#define CTImode E_CTImode
#else
#define CTImode (complex_mode ((complex_mode::from_int) E_CTImode))
#endif
  E_COImode,               /* machmode.def:262 */
#define HAVE_COImode
#ifdef USE_ENUM_MODES
#define COImode E_COImode
#else
#define COImode (complex_mode ((complex_mode::from_int) E_COImode))
#endif
  E_CCImode,               /* machmode.def:262 */
#define HAVE_CCImode
#ifdef USE_ENUM_MODES
#define CCImode E_CCImode
#else
#define CCImode (complex_mode ((complex_mode::from_int) E_CCImode))
#endif
  E_CXImode,               /* machmode.def:262 */
#define HAVE_CXImode
#ifdef USE_ENUM_MODES
#define CXImode E_CXImode
#else
#define CXImode (complex_mode ((complex_mode::from_int) E_CXImode))
#endif
  E_HCmode,                /* machmode.def:264 */
#define HAVE_HCmode
#ifdef USE_ENUM_MODES
#define HCmode E_HCmode
#else
#define HCmode (complex_mode ((complex_mode::from_int) E_HCmode))
#endif
  E_SCmode,                /* machmode.def:264 */
#define HAVE_SCmode
#ifdef USE_ENUM_MODES
#define SCmode E_SCmode
#else
#define SCmode (complex_mode ((complex_mode::from_int) E_SCmode))
#endif
  E_DCmode,                /* machmode.def:264 */
#define HAVE_DCmode
#ifdef USE_ENUM_MODES
#define DCmode E_DCmode
#else
#define DCmode (complex_mode ((complex_mode::from_int) E_DCmode))
#endif
  E_TCmode,                /* machmode.def:264 */
#define HAVE_TCmode
#ifdef USE_ENUM_MODES
#define TCmode E_TCmode
#else
#define TCmode (complex_mode ((complex_mode::from_int) E_TCmode))
#endif
  E_VNx16BImode,           /* config/aarch64/aarch64-modes.def:34 */
#define HAVE_VNx16BImode
#ifdef USE_ENUM_MODES
#define VNx16BImode E_VNx16BImode
#else
#define VNx16BImode ((void) 0, E_VNx16BImode)
#endif
  E_VNx8BImode,            /* config/aarch64/aarch64-modes.def:35 */
#define HAVE_VNx8BImode
#ifdef USE_ENUM_MODES
#define VNx8BImode E_VNx8BImode
#else
#define VNx8BImode ((void) 0, E_VNx8BImode)
#endif
  E_VNx4BImode,            /* config/aarch64/aarch64-modes.def:36 */
#define HAVE_VNx4BImode
#ifdef USE_ENUM_MODES
#define VNx4BImode E_VNx4BImode
#else
#define VNx4BImode ((void) 0, E_VNx4BImode)
#endif
  E_VNx2BImode,            /* config/aarch64/aarch64-modes.def:37 */
#define HAVE_VNx2BImode
#ifdef USE_ENUM_MODES
#define VNx2BImode E_VNx2BImode
#else
#define VNx2BImode ((void) 0, E_VNx2BImode)
#endif
  E_V8QImode,              /* config/aarch64/aarch64-modes.def:49 */
#define HAVE_V8QImode
#ifdef USE_ENUM_MODES
#define V8QImode E_V8QImode
#else
#define V8QImode ((void) 0, E_V8QImode)
#endif
  E_V4HImode,              /* config/aarch64/aarch64-modes.def:49 */
#define HAVE_V4HImode
#ifdef USE_ENUM_MODES
#define V4HImode E_V4HImode
#else
#define V4HImode ((void) 0, E_V4HImode)
#endif
  E_V2SImode,              /* config/aarch64/aarch64-modes.def:49 */
#define HAVE_V2SImode
#ifdef USE_ENUM_MODES
#define V2SImode E_V2SImode
#else
#define V2SImode ((void) 0, E_V2SImode)
#endif
  E_V16QImode,             /* config/aarch64/aarch64-modes.def:50 */
#define HAVE_V16QImode
#ifdef USE_ENUM_MODES
#define V16QImode E_V16QImode
#else
#define V16QImode ((void) 0, E_V16QImode)
#endif
  E_VNx16QImode,           /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx16QImode
#ifdef USE_ENUM_MODES
#define VNx16QImode E_VNx16QImode
#else
#define VNx16QImode ((void) 0, E_VNx16QImode)
#endif
  E_V8HImode,              /* config/aarch64/aarch64-modes.def:50 */
#define HAVE_V8HImode
#ifdef USE_ENUM_MODES
#define V8HImode E_V8HImode
#else
#define V8HImode ((void) 0, E_V8HImode)
#endif
  E_VNx8HImode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx8HImode
#ifdef USE_ENUM_MODES
#define VNx8HImode E_VNx8HImode
#else
#define VNx8HImode ((void) 0, E_VNx8HImode)
#endif
  E_V4SImode,              /* config/aarch64/aarch64-modes.def:50 */
#define HAVE_V4SImode
#ifdef USE_ENUM_MODES
#define V4SImode E_V4SImode
#else
#define V4SImode ((void) 0, E_V4SImode)
#endif
  E_VNx4SImode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx4SImode
#ifdef USE_ENUM_MODES
#define VNx4SImode E_VNx4SImode
#else
#define VNx4SImode ((void) 0, E_VNx4SImode)
#endif
  E_V2DImode,              /* config/aarch64/aarch64-modes.def:50 */
#define HAVE_V2DImode
#ifdef USE_ENUM_MODES
#define V2DImode E_V2DImode
#else
#define V2DImode ((void) 0, E_V2DImode)
#endif
  E_VNx2DImode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx2DImode
#ifdef USE_ENUM_MODES
#define VNx2DImode E_VNx2DImode
#else
#define VNx2DImode ((void) 0, E_VNx2DImode)
#endif
  E_VNx32QImode,           /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx32QImode
#ifdef USE_ENUM_MODES
#define VNx32QImode E_VNx32QImode
#else
#define VNx32QImode ((void) 0, E_VNx32QImode)
#endif
  E_VNx16HImode,           /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx16HImode
#ifdef USE_ENUM_MODES
#define VNx16HImode E_VNx16HImode
#else
#define VNx16HImode ((void) 0, E_VNx16HImode)
#endif
  E_VNx8SImode,            /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx8SImode
#ifdef USE_ENUM_MODES
#define VNx8SImode E_VNx8SImode
#else
#define VNx8SImode ((void) 0, E_VNx8SImode)
#endif
  E_VNx4DImode,            /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx4DImode
#ifdef USE_ENUM_MODES
#define VNx4DImode E_VNx4DImode
#else
#define VNx4DImode ((void) 0, E_VNx4DImode)
#endif
  E_VNx2TImode,            /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx2TImode
#ifdef USE_ENUM_MODES
#define VNx2TImode E_VNx2TImode
#else
#define VNx2TImode ((void) 0, E_VNx2TImode)
#endif
  E_VNx48QImode,           /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx48QImode
#ifdef USE_ENUM_MODES
#define VNx48QImode E_VNx48QImode
#else
#define VNx48QImode ((void) 0, E_VNx48QImode)
#endif
  E_VNx24HImode,           /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx24HImode
#ifdef USE_ENUM_MODES
#define VNx24HImode E_VNx24HImode
#else
#define VNx24HImode ((void) 0, E_VNx24HImode)
#endif
  E_VNx12SImode,           /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx12SImode
#ifdef USE_ENUM_MODES
#define VNx12SImode E_VNx12SImode
#else
#define VNx12SImode ((void) 0, E_VNx12SImode)
#endif
  E_VNx6DImode,            /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx6DImode
#ifdef USE_ENUM_MODES
#define VNx6DImode E_VNx6DImode
#else
#define VNx6DImode ((void) 0, E_VNx6DImode)
#endif
  E_VNx3TImode,            /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx3TImode
#ifdef USE_ENUM_MODES
#define VNx3TImode E_VNx3TImode
#else
#define VNx3TImode ((void) 0, E_VNx3TImode)
#endif
  E_VNx64QImode,           /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx64QImode
#ifdef USE_ENUM_MODES
#define VNx64QImode E_VNx64QImode
#else
#define VNx64QImode ((void) 0, E_VNx64QImode)
#endif
  E_VNx32HImode,           /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx32HImode
#ifdef USE_ENUM_MODES
#define VNx32HImode E_VNx32HImode
#else
#define VNx32HImode ((void) 0, E_VNx32HImode)
#endif
  E_VNx16SImode,           /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx16SImode
#ifdef USE_ENUM_MODES
#define VNx16SImode E_VNx16SImode
#else
#define VNx16SImode ((void) 0, E_VNx16SImode)
#endif
  E_VNx8DImode,            /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx8DImode
#ifdef USE_ENUM_MODES
#define VNx8DImode E_VNx8DImode
#else
#define VNx8DImode ((void) 0, E_VNx8DImode)
#endif
  E_VNx4TImode,            /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx4TImode
#ifdef USE_ENUM_MODES
#define VNx4TImode E_VNx4TImode
#else
#define VNx4TImode ((void) 0, E_VNx4TImode)
#endif
  E_VNx2OImode,            /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx2OImode
#ifdef USE_ENUM_MODES
#define VNx2OImode E_VNx2OImode
#else
#define VNx2OImode ((void) 0, E_VNx2OImode)
#endif
  E_V2HFmode,              /* config/aarch64/aarch64-modes.def:54 */
#define HAVE_V2HFmode
#ifdef USE_ENUM_MODES
#define V2HFmode E_V2HFmode
#else
#define V2HFmode ((void) 0, E_V2HFmode)
#endif
  E_V4HFmode,              /* config/aarch64/aarch64-modes.def:51 */
#define HAVE_V4HFmode
#ifdef USE_ENUM_MODES
#define V4HFmode E_V4HFmode
#else
#define V4HFmode ((void) 0, E_V4HFmode)
#endif
  E_V2SFmode,              /* config/aarch64/aarch64-modes.def:51 */
#define HAVE_V2SFmode
#ifdef USE_ENUM_MODES
#define V2SFmode E_V2SFmode
#else
#define V2SFmode ((void) 0, E_V2SFmode)
#endif
  E_V1DFmode,              /* config/aarch64/aarch64-modes.def:53 */
#define HAVE_V1DFmode
#ifdef USE_ENUM_MODES
#define V1DFmode E_V1DFmode
#else
#define V1DFmode ((void) 0, E_V1DFmode)
#endif
  E_V8HFmode,              /* config/aarch64/aarch64-modes.def:52 */
#define HAVE_V8HFmode
#ifdef USE_ENUM_MODES
#define V8HFmode E_V8HFmode
#else
#define V8HFmode ((void) 0, E_V8HFmode)
#endif
  E_VNx8HFmode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx8HFmode
#ifdef USE_ENUM_MODES
#define VNx8HFmode E_VNx8HFmode
#else
#define VNx8HFmode ((void) 0, E_VNx8HFmode)
#endif
  E_V4SFmode,              /* config/aarch64/aarch64-modes.def:52 */
#define HAVE_V4SFmode
#ifdef USE_ENUM_MODES
#define V4SFmode E_V4SFmode
#else
#define V4SFmode ((void) 0, E_V4SFmode)
#endif
  E_VNx4SFmode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx4SFmode
#ifdef USE_ENUM_MODES
#define VNx4SFmode E_VNx4SFmode
#else
#define VNx4SFmode ((void) 0, E_VNx4SFmode)
#endif
  E_V2DFmode,              /* config/aarch64/aarch64-modes.def:52 */
#define HAVE_V2DFmode
#ifdef USE_ENUM_MODES
#define V2DFmode E_V2DFmode
#else
#define V2DFmode ((void) 0, E_V2DFmode)
#endif
  E_VNx2DFmode,            /* config/aarch64/aarch64-modes.def:90 */
#define HAVE_VNx2DFmode
#ifdef USE_ENUM_MODES
#define VNx2DFmode E_VNx2DFmode
#else
#define VNx2DFmode ((void) 0, E_VNx2DFmode)
#endif
  E_VNx16HFmode,           /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx16HFmode
#ifdef USE_ENUM_MODES
#define VNx16HFmode E_VNx16HFmode
#else
#define VNx16HFmode ((void) 0, E_VNx16HFmode)
#endif
  E_VNx8SFmode,            /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx8SFmode
#ifdef USE_ENUM_MODES
#define VNx8SFmode E_VNx8SFmode
#else
#define VNx8SFmode ((void) 0, E_VNx8SFmode)
#endif
  E_VNx4DFmode,            /* config/aarch64/aarch64-modes.def:91 */
#define HAVE_VNx4DFmode
#ifdef USE_ENUM_MODES
#define VNx4DFmode E_VNx4DFmode
#else
#define VNx4DFmode ((void) 0, E_VNx4DFmode)
#endif
  E_VNx24HFmode,           /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx24HFmode
#ifdef USE_ENUM_MODES
#define VNx24HFmode E_VNx24HFmode
#else
#define VNx24HFmode ((void) 0, E_VNx24HFmode)
#endif
  E_VNx12SFmode,           /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx12SFmode
#ifdef USE_ENUM_MODES
#define VNx12SFmode E_VNx12SFmode
#else
#define VNx12SFmode ((void) 0, E_VNx12SFmode)
#endif
  E_VNx6DFmode,            /* config/aarch64/aarch64-modes.def:92 */
#define HAVE_VNx6DFmode
#ifdef USE_ENUM_MODES
#define VNx6DFmode E_VNx6DFmode
#else
#define VNx6DFmode ((void) 0, E_VNx6DFmode)
#endif
  E_VNx32HFmode,           /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx32HFmode
#ifdef USE_ENUM_MODES
#define VNx32HFmode E_VNx32HFmode
#else
#define VNx32HFmode ((void) 0, E_VNx32HFmode)
#endif
  E_VNx16SFmode,           /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx16SFmode
#ifdef USE_ENUM_MODES
#define VNx16SFmode E_VNx16SFmode
#else
#define VNx16SFmode ((void) 0, E_VNx16SFmode)
#endif
  E_VNx8DFmode,            /* config/aarch64/aarch64-modes.def:93 */
#define HAVE_VNx8DFmode
#ifdef USE_ENUM_MODES
#define VNx8DFmode E_VNx8DFmode
#else
#define VNx8DFmode ((void) 0, E_VNx8DFmode)
#endif
  MAX_MACHINE_MODE,

  MIN_MODE_RANDOM = E_VOIDmode,
  MAX_MODE_RANDOM = E_BLKmode,

  MIN_MODE_CC = E_CCmode,
  MAX_MODE_CC = E_CC_Cmode,

  MIN_MODE_INT = E_QImode,
  MAX_MODE_INT = E_XImode,

  MIN_MODE_PARTIAL_INT = E_VOIDmode,
  MAX_MODE_PARTIAL_INT = E_VOIDmode,

  MIN_MODE_POINTER_BOUNDS = E_VOIDmode,
  MAX_MODE_POINTER_BOUNDS = E_VOIDmode,

  MIN_MODE_FRACT = E_QQmode,
  MAX_MODE_FRACT = E_TQmode,

  MIN_MODE_UFRACT = E_UQQmode,
  MAX_MODE_UFRACT = E_UTQmode,

  MIN_MODE_ACCUM = E_HAmode,
  MAX_MODE_ACCUM = E_TAmode,

  MIN_MODE_UACCUM = E_UHAmode,
  MAX_MODE_UACCUM = E_UTAmode,

  MIN_MODE_FLOAT = E_HFmode,
  MAX_MODE_FLOAT = E_TFmode,

  MIN_MODE_DECIMAL_FLOAT = E_SDmode,
  MAX_MODE_DECIMAL_FLOAT = E_TDmode,

  MIN_MODE_COMPLEX_INT = E_CQImode,
  MAX_MODE_COMPLEX_INT = E_CXImode,

  MIN_MODE_COMPLEX_FLOAT = E_HCmode,
  MAX_MODE_COMPLEX_FLOAT = E_TCmode,

  MIN_MODE_VECTOR_BOOL = E_VNx16BImode,
  MAX_MODE_VECTOR_BOOL = E_VNx2BImode,

  MIN_MODE_VECTOR_INT = E_V8QImode,
  MAX_MODE_VECTOR_INT = E_VNx2OImode,

  MIN_MODE_VECTOR_FRACT = E_VOIDmode,
  MAX_MODE_VECTOR_FRACT = E_VOIDmode,

  MIN_MODE_VECTOR_UFRACT = E_VOIDmode,
  MAX_MODE_VECTOR_UFRACT = E_VOIDmode,

  MIN_MODE_VECTOR_ACCUM = E_VOIDmode,
  MAX_MODE_VECTOR_ACCUM = E_VOIDmode,

  MIN_MODE_VECTOR_UACCUM = E_VOIDmode,
  MAX_MODE_VECTOR_UACCUM = E_VOIDmode,

  MIN_MODE_VECTOR_FLOAT = E_V2HFmode,
  MAX_MODE_VECTOR_FLOAT = E_VNx8DFmode,

  NUM_MACHINE_MODES = MAX_MACHINE_MODE
};

#define CONST_MODE_NUNITS
#define CONST_MODE_PRECISION
#define CONST_MODE_SIZE
#define CONST_MODE_UNIT_SIZE const
#define CONST_MODE_BASE_ALIGN
#define CONST_MODE_IBIT const
#define CONST_MODE_FBIT const

#define BITS_PER_UNIT (8)
#define MAX_BITSIZE_MODE_ANY_INT (64*BITS_PER_UNIT)
#define MAX_BITSIZE_MODE_ANY_MODE 8192
#define NUM_INT_N_ENTS 1
#define NUM_POLY_INT_COEFFS 2

#endif /* insn-modes.h */
