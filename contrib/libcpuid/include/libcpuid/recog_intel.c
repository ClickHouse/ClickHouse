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
#include <string.h>
#include <ctype.h>
#include "libcpuid.h"
#include "libcpuid_util.h"
#include "libcpuid_internal.h"
#include "recog_intel.h"

const struct intel_bcode_str { intel_code_t code; char *str; } intel_bcode_str[] = {
	#define CODE(x) { x, #x }
	#define CODE2(x, y) CODE(x)
	#include "intel_code_t.h"
	#undef CODE
};

enum _intel_model_t {
	UNKNOWN = -1,
	_3000 = 100,
	_3100,
	_3200,
	X3200,
	_3300,
	X3300,
	_5100,
	_5200,
	_5300,
	_5400,
	_2xxx, /* Core i[357] 2xxx */
	_3xxx, /* Core i[357] 3xxx */
};
typedef enum _intel_model_t intel_model_t;

const struct match_entry_t cpudb_intel[] = {
	{ -1, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Intel CPU"       },
	
	/* i486 */
	{  4, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown i486"            },
	{  4,  0, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX-25/33"           },
	{  4,  1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX-50"              },
	{  4,  2, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 SX"                 },
	{  4,  3, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX2"                },
	{  4,  4, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 SL"                 },
	{  4,  5, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 SX2"                },
	{  4,  7, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX2 WriteBack"      },
	{  4,  8, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX4"                },
	{  4,  9, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "i486 DX4 WriteBack"      },
	
	/* All Pentia:
	   Pentium 1 */
	{  5, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Pentium"         },
	{  5,  0, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium A-Step"          },
	{  5,  1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 1 (0.8u)"        },
	{  5,  2, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 1 (0.35u)"       },
	{  5,  3, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium OverDrive"       },
	{  5,  4, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 1 (0.35u)"       },
	{  5,  7, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 1 (0.35u)"       },
	{  5,  8, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium MMX (0.25u)"     },
	
	/* Pentium 2 / 3 / M / Conroe / whatsnext - all P6 based. */
	{  6, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown P6"              },
	{  6,  0, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium Pro"             },
	{  6,  1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium Pro"             },
	{  6,  3, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium II (Klamath)"    },
	{  6,  5, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium II (Deschutes)"  },
	{  6,  5, -1, -1, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile Pentium II (Tonga)"},
	{  6,  6, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium II (Dixon)"      },
	
	{  6,  3, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon (Klamath)"     },
	{  6,  5, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon (Drake)"       },
	{  6,  6, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon (Dixon)"       },
		
	{  6,  5, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-II Celeron (Covingtons" },
	{  6,  6, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-II Celeron (Mendocino)" },
	
	/* -------------------------------------------------- */
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Katmai)"    },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Coppermine)"},
	{  6, 10, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Coppermine)"},
	{  6, 11, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Tualatin)"  },
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon (Tanner)"     },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon (Cascades)"   },
	{  6, 10, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon (Cascades)"   },
	{  6, 11, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon (Tualatin)"   },
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron (Katmai)"     },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron (Coppermine)" },
	{  6, 10, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron (Coppermine)" },
	{  6, 11, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron (Tualatin)"   },
	
	/* Netburst based (Pentium 4 and later)
	   classic P4s */
	{ 15, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Pentium 4"       },
	{ 15, -1, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "Unknown P-4 Celeron"     },
	{ 15, -1, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Unknown Xeon"            },
	
	{ 15,  0, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Willamette)"  },
	{ 15,  1, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Willamette)"  },
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Northwood)"   },
	{ 15,  3, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Prescott)"    },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Prescott)"    },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium 4 (Cedar Mill)"  },
	{ 15,  0, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Willamette)" },
	{ 15,  1, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Willamette)" },
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Northwood)"  },
	{ 15,  3, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Prescott)"   },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Prescott)"   },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, MOBILE_PENTIUM    ,     0, "Mobile P-4 (Cedar Mill)" },
	
	/* server CPUs */
	{ 15,  0, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Foster)"           },
	{ 15,  1, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Foster)"           },
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Prestonia)"        },
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, XEONMP            ,     0, "Xeon (Gallatin)"         },
	{ 15,  3, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Nocona)"           },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Nocona)"           },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, XEON_IRWIN        ,     0, "Xeon (Irwindale)"        },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, XEONMP            ,     0, "Xeon (Cranford)"         },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, XEON_POTOMAC      ,     0, "Xeon (Potomac)"          },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, XEON              ,     0, "Xeon (Dempsey)"          },
	
	/* Pentium Ds */
	{ 15,  4,  4, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium D (SmithField)"  },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, PENTIUM_D         ,     0, "Pentium D (SmithField)"  },
	{ 15,  4,  7, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium D (SmithField)"  },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, PENTIUM_D         ,     0, "Pentium D (Presler)"     },

	/* Celeron and Celeron Ds */
	{ 15,  1, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron (Willamette)"   },
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron (Northwood)"    },
	{ 15,  3, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron D (Prescott)"   },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron D (Prescott)"   },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron D (Cedar Mill)" },
	
	/* -------------------------------------------------- */
	/* Intel Core microarchitecture - P6-based */
	
	{  6,  9, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Pentium M"          },
	{  6,  9, -1, -1, -1,   1,    -1,    -1, MOBILE_PENTIUM_M  ,     0, "Unknown Pentium M"          },
	{  6,  9, -1, -1, -1,   1,    -1,    -1, PENTIUM           ,     0, "Pentium M (Banias)"         },
	{  6,  9, -1, -1, -1,   1,    -1,    -1, MOBILE_PENTIUM_M  ,     0, "Pentium M (Banias)"         },
	{  6,  9, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "Celeron M"                  },
	{  6, 13, -1, -1, -1,   1,    -1,    -1, PENTIUM           ,     0, "Pentium M (Dothan)"         },
	{  6, 13, -1, -1, -1,   1,    -1,    -1, MOBILE_PENTIUM_M  ,     0, "Pentium M (Dothan)"         },
	{  6, 13, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "Celeron M"                  },
	
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM_UNKNOWN      ,     0, "Unknown Atom"               },
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM_DIAMONDVILLE ,     0, "Atom (Diamondville)"        },
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM_SILVERTHORNE ,     0, "Atom (Silverthorne)"        },
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM_CEDARVIEW    ,     0, "Atom (Cedarview)"           },
	{  6,  6, -1, -1, -1,  -1,    -1,    -1, ATOM_CEDARVIEW    ,     0, "Atom (Cedarview)"           },
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM_PINEVIEW     ,     0, "Atom (Pineview)"            },
	
	/* -------------------------------------------------- */
	
	{  6, 14, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Yonah"             },
	{  6, 14, -1, -1, -1,   1,    -1,    -1, CORE_SOLO         ,     0, "Yonah (Core Solo)"         },
	{  6, 14, -1, -1, -1,   2,    -1,    -1, CORE_DUO          ,     0, "Yonah (Core Duo)"          },
	{  6, 14, -1, -1, -1,   1,    -1,    -1, MOBILE_CORE_SOLO  ,     0, "Yonah (Core Solo)"         },
	{  6, 14, -1, -1, -1,   2,    -1,    -1, MOBILE_CORE_DUO   ,     0, "Yonah (Core Duo)"          },
	{  6, 14, -1, -1, -1,   1,    -1,    -1, CORE_SOLO         ,     0, "Yonah (Core Solo)"         },
	
	{  6, 15, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Core 2"            },
	{  6, 15, -1, -1, -1,   2,  4096,    -1, CORE_DUO          ,     0, "Conroe (Core 2 Duo)"       },
	{  6, 15, -1, -1, -1,   2,  1024,    -1, CORE_DUO          ,     0, "Conroe (Core 2 Duo) 1024K" },
	{  6, 15, -1, -1, -1,   2,   512,    -1, CORE_DUO          ,     0, "Conroe (Core 2 Duo) 512K"  },
	{  6, 15, -1, -1, -1,   4,    -1,    -1, QUAD_CORE         ,     0, "Kentsfield (Core 2 Quad)"  },
	{  6, 15, -1, -1, -1,   4,  4096,    -1, QUAD_CORE         ,     0, "Kentsfield (Core 2 Quad)"  },
	{  6, 15, -1, -1, -1, 400,    -1,    -1, MORE_THAN_QUADCORE,     0, "More than quad-core"       },
	{  6, 15, -1, -1, -1,   2,  2048,    -1, CORE_DUO          ,     0, "Allendale (Core 2 Duo)"    },
	{  6, 15, -1, -1, -1,   2,    -1,    -1, MOBILE_CORE_DUO   ,     0, "Merom (Core 2 Duo)"        },
	{  6, 15, -1, -1, -1,   2,  2048,    -1, MEROM             ,     0, "Merom (Core 2 Duo) 2048K"  },
	{  6, 15, -1, -1, -1,   2,  4096,    -1, MEROM             ,     0, "Merom (Core 2 Duo) 4096K"  },
	
	{  6, 15, -1, -1, 15,   1,    -1,    -1, CELERON           ,     0, "Conroe-L (Celeron)"        },
	{  6,  6, -1, -1, 22,   1,    -1,    -1, CELERON           ,     0, "Conroe-L (Celeron)"        },
	{  6, 15, -1, -1, 15,   2,    -1,    -1, CELERON           ,     0, "Conroe-L (Allendale)"      },
	{  6,  6, -1, -1, 22,   2,    -1,    -1, CELERON           ,     0, "Conroe-L (Allendale)"      },
	
	
	{  6,  6, -1, -1, 22,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Core ?"           },
	{  6,  7, -1, -1, 23,   1,    -1,    -1, NO_CODE           ,     0, "Unknown Core ?"           },
	{  6,  6, -1, -1, 22, 400,    -1,    -1, MORE_THAN_QUADCORE,     0, "More than quad-core"      },
	{  6,  7, -1, -1, 23, 400,    -1,    -1, MORE_THAN_QUADCORE,     0, "More than quad-core"      },
	
	{  6,  7, -1, -1, 23,   1,    -1,    -1, CORE_SOLO         ,     0, "Unknown Core 45nm"        },
	{  6,  7, -1, -1, 23,   1,    -1,    -1, CORE_DUO          ,     0, "Unknown Core 45nm"        },
	{  6,  7, -1, -1, 23,   2,  1024,    -1, WOLFDALE          ,     0, "Celeron Wolfdale 1M" },
	{  6,  7, -1, -1, 23,   2,  2048,    -1, WOLFDALE          ,     0, "Wolfdale (Core 2 Duo) 2M" },
	{  6,  7, -1, -1, 23,   2,  3072,    -1, WOLFDALE          ,     0, "Wolfdale (Core 2 Duo) 3M" },
	{  6,  7, -1, -1, 23,   2,  6144,    -1, WOLFDALE          ,     0, "Wolfdale (Core 2 Duo) 6M" },
	{  6,  7, -1, -1, 23,   1,    -1,    -1, MOBILE_CORE_DUO   ,     0, "Penryn (Core 2 Duo)"      },
	{  6,  7, -1, -1, 23,   2,  1024,    -1, PENRYN            ,     0, "Penryn (Core 2 Duo)"      },
	{  6,  7, -1, -1, 23,   2,  3072,    -1, PENRYN            ,     0, "Penryn (Core 2 Duo) 3M"   },
	{  6,  7, -1, -1, 23,   2,  6144,    -1, PENRYN            ,     0, "Penryn (Core 2 Duo) 6M"   },
	{  6,  7, -1, -1, 23,   4,  2048,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 2M"},
	{  6,  7, -1, -1, 23,   4,  3072,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 3M"},
	{  6,  7, -1, -1, 23,   4,  6144,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 6M"},
	
	/* Core microarchitecture-based Xeons: */
	{  6, 14, -1, -1, 14,   1,    -1,    -1, XEON              ,     0, "Xeon LV"                  },
	{  6, 15, -1, -1, 15,   2,  4096,    -1, XEON              , _5100, "Xeon (Woodcrest)"         },
	{  6, 15, -1, -1, 15,   2,  2048,    -1, XEON              , _3000, "Xeon (Conroe/2M)"         },
	{  6, 15, -1, -1, 15,   2,  4096,    -1, XEON              , _3000, "Xeon (Conroe/4M)"         },
	{  6, 15, -1, -1, 15,   4,  4096,    -1, XEON              , X3200, "Xeon (Kentsfield)"        },
	{  6, 15, -1, -1, 15,   4,  4096,    -1, XEON              , _5300, "Xeon (Clovertown)"        },
	{  6,  7, -1, -1, 23,   2,  6144,    -1, XEON              , _3100, "Xeon (Wolfdale)"          },
	{  6,  7, -1, -1, 23,   2,  6144,    -1, XEON              , _5200, "Xeon (Wolfdale DP)"       },
	{  6,  7, -1, -1, 23,   4,  6144,    -1, XEON              , _5400, "Xeon (Harpertown)"        },
	{  6,  7, -1, -1, 23,   4,  3072,    -1, XEON              , X3300, "Xeon (Yorkfield/3M)"      },
	{  6,  7, -1, -1, 23,   4,  6144,    -1, XEON              , X3300, "Xeon (Yorkfield/6M)"      },

	/* Nehalem CPUs (45nm): */
	{  6, 10, -1, -1, 26,   4,    -1,    -1, XEON_GAINESTOWN   ,     0, "Gainestown (Xeon)"        },
	{  6, 10, -1, -1, 26,   4,    -1,  4096, XEON_GAINESTOWN   ,     0, "Gainestown 4M (Xeon)"     },
	{  6, 10, -1, -1, 26,   4,    -1,  8192, XEON_GAINESTOWN   ,     0, "Gainestown 8M (Xeon)"     },
	{  6, 10, -1, -1, 26,   4,    -1,    -1, XEON_I7           ,     0, "Bloomfield (Xeon)"        },
	{  6, 10, -1, -1, 26,   4,    -1,    -1, CORE_I7           ,     0, "Bloomfield (Core i7)"     },
	{  6, 10, -1, -1, 30,   4,    -1,    -1, CORE_I7           ,     0, "Lynnfield (Core i7)"      },
	{  6,  5, -1, -1, 37,   4,    -1,  8192, CORE_I5           ,     0, "Lynnfield (Core i5)"      },

	/* Westmere CPUs (32nm): */
	{  6,  5, -1, -1, 37,   2,    -1,    -1, NO_CODE           ,     0, "Unknown Core i3/i5"       },
	{  6, 12, -1, -1, 44,  -1,    -1,    -1, XEON_WESTMERE     ,     0, "Westmere (Xeon)"          },
	{  6, 12, -1, -1, 44,  -1,    -1, 12288, XEON_WESTMERE     ,     0, "Gulftown (Xeon)"          },
	{  6, 12, -1, -1, 44,   4,    -1, 12288, CORE_I7           ,     0, "Gulftown (Core i7)"       },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I5           ,     0, "Clarkdale (Core i5)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I3           ,     0, "Clarkdale (Core i3)"      },
	{  6,  5, -1, -1, 37,   2,    -1,    -1, PENTIUM           ,     0, "Arrandale"                },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I7           ,     0, "Arrandale (Core i7)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  3072, CORE_I5           ,     0, "Arrandale (Core i5)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  3072, CORE_I3           ,     0, "Arrandale (Core i3)"      },

	/* Sandy Bridge CPUs (32nm): */
	{  6, 10, -1, -1, 42,  -1,    -1,    -1, NO_CODE           ,     0, "Unknown Sandy Bridge"     },
	{  6, 10, -1, -1, 42,  -1,    -1,    -1, XEON              ,     0, "Sandy Bridge (Xeon)"      },
	{  6, 10, -1, -1, 42,  -1,    -1,    -1, CORE_I7           ,     0, "Sandy Bridge (Core i7)"   },
	{  6, 10, -1, -1, 42,   4,    -1,    -1, CORE_I7           ,     0, "Sandy Bridge (Core i7)"   },
	{  6, 10, -1, -1, 42,   4,    -1,    -1, CORE_I5           ,     0, "Sandy Bridge (Core i5)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, CORE_I3           ,     0, "Sandy Bridge (Core i3)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, PENTIUM           ,     0, "Sandy Bridge (Pentium)"   },
	{  6, 10, -1, -1, 42,   1,    -1,    -1, CELERON           ,     0, "Sandy Bridge (Celeron)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, CELERON           ,     0, "Sandy Bridge (Celeron)"   },
	{  6, 13, -1, -1, 45,  -1,    -1,    -1, NO_CODE           ,     0, "Sandy Bridge-E"           },
	{  6, 13, -1, -1, 45,  -1,    -1,    -1, XEON              ,     0, "Sandy Bridge-E (Xeon)"    },

	/* Ivy Bridge CPUs (22nm): */
	{  6, 10, -1, -1, 58,  -1,    -1,    -1, XEON              ,     0, "Ivy Bridge (Xeon)"        },
	{  6, 10, -1, -1, 58,   4,    -1,    -1, CORE_IVY7         ,     0, "Ivy Bridge (Core i7)"   },
	{  6, 10, -1, -1, 58,   4,    -1,    -1, CORE_IVY5         ,     0, "Ivy Bridge (Core i5)"   },
	{  6, 10, -1, -1, 58,   2,    -1,    -1, CORE_IVY3         ,     0, "Ivy Bridge (Core i3)"   },
	{  6, 10, -1, -1, 58,   2,    -1,    -1, PENTIUM           ,     0, "Ivy Bridge (Pentium)"     },
	{  6, 10, -1, -1, 58,   1,    -1,    -1, CELERON           ,     0, "Ivy Bridge (Celeron)"     },
	{  6, 10, -1, -1, 58,   2,    -1,    -1, CELERON           ,     0, "Ivy Bridge (Celeron)"     },
	{  6, 14, -1, -1, 62,  -1,    -1,    -1, NO_CODE           ,     0, "Ivy Bridge-E"             },
	
	/* Haswell CPUs (22nm): */
	{  6, 12, -1, -1, 60,  -1,    -1,    -1, XEON              ,     0, "Haswell (Xeon)"           },
	{  6, 12, -1, -1, 60,   4,    -1,    -1, CORE_HASWELL7     ,     0, "Haswell (Core i7)"   },
	{  6,  5, -1, -1, 69,   4,    -1,    -1, CORE_HASWELL7     ,     0, "Haswell (Core i7)"   },
	{  6, 12, -1, -1, 60,   4,    -1,    -1, CORE_HASWELL5     ,     0, "Haswell (Core i5)"        },
	{  6,  5, -1, -1, 69,   4,    -1,    -1, CORE_HASWELL5     ,     0, "Haswell (Core i5)"        },
	{  6, 12, -1, -1, 60,   2,    -1,    -1, CORE_HASWELL3     ,     0, "Haswell (Core i3)"        },
	{  6,  5, -1, -1, 69,   2,    -1,    -1, CORE_HASWELL3     ,     0, "Haswell (Core i3)"   },
	{  6, 12, -1, -1, 60,   2,    -1,    -1, PENTIUM           ,     0, "Haswell (Pentium)"        },
	{  6, 12, -1, -1, 60,   2,    -1,    -1, CELERON           ,     0, "Haswell (Celeron)"        },
	{  6, 12, -1, -1, 60,   1,    -1,    -1, CELERON           ,     0, "Haswell (Celeron)"        },
	{  6, 15, -1, -1, 63,  -1,    -1,    -1, NO_CODE           ,     0, "Haswell-E"                },

	/* Broadwell CPUs (14nm): */
	{  6,  7, -1, -1, 71,   4,    -1,    -1, CORE_BROADWELL7   ,     0, "Broadwell (Core i7)"      },
	{  6,  7, -1, -1, 71,   4,    -1,    -1, CORE_BROADWELL5   ,     0, "Broadwell (Core i5)"      },
	{  6, 13, -1, -1, 61,   4,    -1,    -1, CORE_BROADWELL7   ,     0, "Broadwell-U (Core i7)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, CORE_BROADWELL7   ,     0, "Broadwell-U (Core i7)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, CORE_BROADWELL5   ,     0, "Broadwell-U (Core i5)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, CORE_BROADWELL3   ,     0, "Broadwell-U (Core i3)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, PENTIUM           ,     0, "Broadwell-U (Pentium)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, CELERON           ,     0, "Broadwell-U (Celeron)"    },
	{  6, 13, -1, -1, 61,   2,    -1,    -1, NA                ,     0, "Broadwell-U (Core M)"     },
	{  6, 15, -1, -1, 79,   2,    -1,    -1, CORE_BROADWELL3   ,     0, "Broadwell-E (Core i3)"    },
	{  6, 15, -1, -1, 79,   2,    -1,    -1, CORE_BROADWELL5   ,     0, "Broadwell-E (Core i5)"    },
	{  6, 15, -1, -1, 79,   4,    -1,    -1, CORE_BROADWELL5   ,     0, "Broadwell-E (Core i5)"    },
	{  6, 15, -1, -1, 79,   2,    -1,    -1, CORE_BROADWELL7   ,     0, "Broadwell-E (Core i7)"    },
	{  6, 15, -1, -1, 79,   4,    -1,    -1, CORE_BROADWELL7   ,     0, "Broadwell-E (Core i7)"    },
	
	/* Skylake CPUs (14nm): */
	{  6, 14, -1, -1, 94,   4,    -1,    -1, CORE_BROADWELL7   ,     0, "Skylake (Core i7)"        },
	{  6, 14, -1, -1, 94,   4,    -1,    -1, CORE_BROADWELL5   ,     0, "Skylake (Core i5)"        },
	{  6, 14, -1, -1, 94,   4,    -1,    -1, CORE_BROADWELL3   ,     0, "Skylake (Core i3)"        },
	{  6, 14, -1, -1, 94,   4,    -1,    -1, PENTIUM           ,     0, "Skylake (Pentium)"        },

	/* Itaniums */
	{  7, -1, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Itanium"                 },
	{ 15, -1, -1, 16, -1,   1,    -1,    -1, NO_CODE           ,     0, "Itanium 2"               },
	
};


static void load_intel_features(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	const struct feature_map_t matchtable_edx1[] = {
		{ 18, CPU_FEATURE_PN },
		{ 21, CPU_FEATURE_DTS },
		{ 22, CPU_FEATURE_ACPI },
		{ 27, CPU_FEATURE_SS },
		{ 29, CPU_FEATURE_TM },
		{ 30, CPU_FEATURE_IA64 },
		{ 31, CPU_FEATURE_PBE },
	};
	const struct feature_map_t matchtable_ecx1[] = {
		{  2, CPU_FEATURE_DTS64 },
		{  4, CPU_FEATURE_DS_CPL },
		{  5, CPU_FEATURE_VMX },
		{  6, CPU_FEATURE_SMX },
		{  7, CPU_FEATURE_EST },
		{  8, CPU_FEATURE_TM2 },
		{ 10, CPU_FEATURE_CID },
		{ 14, CPU_FEATURE_XTPR },
		{ 15, CPU_FEATURE_PDCM },
		{ 18, CPU_FEATURE_DCA },
		{ 21, CPU_FEATURE_X2APIC },
	};
	const struct feature_map_t matchtable_edx81[] = {
		{ 20, CPU_FEATURE_XD },
	};
	const struct feature_map_t matchtable_ebx7[] = {
		{  2, CPU_FEATURE_SGX },
		{  4, CPU_FEATURE_HLE },
		{ 11, CPU_FEATURE_RTM },
		{ 16, CPU_FEATURE_AVX512F },
		{ 17, CPU_FEATURE_AVX512DQ },
		{ 18, CPU_FEATURE_RDSEED },
		{ 19, CPU_FEATURE_ADX },
		{ 26, CPU_FEATURE_AVX512PF },
		{ 27, CPU_FEATURE_AVX512ER },
		{ 28, CPU_FEATURE_AVX512CD },
		{ 29, CPU_FEATURE_SHA_NI },
		{ 30, CPU_FEATURE_AVX512BW },
		{ 31, CPU_FEATURE_AVX512VL },
	};
	if (raw->basic_cpuid[0][0] >= 1) {
		match_features(matchtable_edx1, COUNT_OF(matchtable_edx1), raw->basic_cpuid[1][3], data);
		match_features(matchtable_ecx1, COUNT_OF(matchtable_ecx1), raw->basic_cpuid[1][2], data);
	}
	if (raw->ext_cpuid[0][0] >= 1) {
		match_features(matchtable_edx81, COUNT_OF(matchtable_edx81), raw->ext_cpuid[1][3], data);
	}
	// detect TSX/AVX512:
	if (raw->basic_cpuid[0][0] >= 7) {
		match_features(matchtable_ebx7, COUNT_OF(matchtable_ebx7), raw->basic_cpuid[7][1], data);
	}
}

enum _cache_type_t {
	L1I,
	L1D,
	L2,
	L3,
	L4
};
typedef enum _cache_type_t cache_type_t;

static void check_case(uint8_t on, cache_type_t cache, int size, int assoc, int linesize, struct cpu_id_t* data)
{
	if (!on) return;
	switch (cache) {
		case L1I:
			data->l1_instruction_cache = size;
			break;
		case L1D:
			data->l1_data_cache = size;
			data->l1_assoc = assoc;
			data->l1_cacheline = linesize;
			break;
		case L2:
			data->l2_cache = size;
			data->l2_assoc = assoc;
			data->l2_cacheline = linesize;
			break;
		case L3:
			data->l3_cache = size;
			data->l3_assoc = assoc;
			data->l3_cacheline = linesize;
			break;
		case L4:
			data->l4_cache = size;
			data->l4_assoc = assoc;
			data->l4_cacheline = linesize;
			break;
		default:
			break;
	}
}

static void decode_intel_oldstyle_cache_info(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	uint8_t f[256] = {0};
	int reg, off;
	uint32_t x;
	for (reg = 0; reg < 4; reg++) {
		x = raw->basic_cpuid[2][reg];
		if (x & 0x80000000) continue;
		for (off = 0; off < 4; off++) {
			f[x & 0xff] = 1;
			x >>= 8;
		}
	}
	
	check_case(f[0x06], L1I,      8,  4,  32, data);
	check_case(f[0x08], L1I,     16,  4,  32, data);
	check_case(f[0x0A], L1D,      8,  2,  32, data);
	check_case(f[0x0C], L1D,     16,  4,  32, data);
	check_case(f[0x22],  L3,    512,  4,  64, data);
	check_case(f[0x23],  L3,   1024,  8,  64, data);
	check_case(f[0x25],  L3,   2048,  8,  64, data);
	check_case(f[0x29],  L3,   4096,  8,  64, data);
	check_case(f[0x2C], L1D,     32,  8,  64, data);
	check_case(f[0x30], L1I,     32,  8,  64, data);
	check_case(f[0x39],  L2,    128,  4,  64, data);
	check_case(f[0x3A],  L2,    192,  6,  64, data);
	check_case(f[0x3B],  L2,    128,  2,  64, data);
	check_case(f[0x3C],  L2,    256,  4,  64, data);
	check_case(f[0x3D],  L2,    384,  6,  64, data);
	check_case(f[0x3E],  L2,    512,  4,  64, data);
	check_case(f[0x41],  L2,    128,  4,  32, data);
	check_case(f[0x42],  L2,    256,  4,  32, data);
	check_case(f[0x43],  L2,    512,  4,  32, data);
	check_case(f[0x44],  L2,   1024,  4,  32, data);
	check_case(f[0x45],  L2,   2048,  4,  32, data);
	check_case(f[0x46],  L3,   4096,  4,  64, data);
	check_case(f[0x47],  L3,   8192,  8,  64, data);
	check_case(f[0x4A],  L3,   6144, 12,  64, data);
	check_case(f[0x4B],  L3,   8192, 16,  64, data);
	check_case(f[0x4C],  L3,  12288, 12,  64, data);
	check_case(f[0x4D],  L3,  16384, 16,  64, data);
	check_case(f[0x4E],  L2,   6144, 24,  64, data);
	check_case(f[0x60], L1D,     16,  8,  64, data);
	check_case(f[0x66], L1D,      8,  4,  64, data);
	check_case(f[0x67], L1D,     16,  4,  64, data);
	check_case(f[0x68], L1D,     32,  4,  64, data);
	/* The following four entries are trace cache. Intel does not
	 * specify a cache-line size, so we use -1 instead
	 */
	check_case(f[0x70], L1I,     12,  8,  -1, data);
	check_case(f[0x71], L1I,     16,  8,  -1, data);
	check_case(f[0x72], L1I,     32,  8,  -1, data);
	check_case(f[0x73], L1I,     64,  8,  -1, data);
	
	check_case(f[0x78],  L2,   1024,  4,  64, data);
	check_case(f[0x79],  L2,    128,  8,  64, data);
	check_case(f[0x7A],  L2,    256,  8,  64, data);
	check_case(f[0x7B],  L2,    512,  8,  64, data);
	check_case(f[0x7C],  L2,   1024,  8,  64, data);
	check_case(f[0x7D],  L2,   2048,  8,  64, data);
	check_case(f[0x7F],  L2,    512,  2,  64, data);
	check_case(f[0x82],  L2,    256,  8,  32, data);
	check_case(f[0x83],  L2,    512,  8,  32, data);
	check_case(f[0x84],  L2,   1024,  8,  32, data);
	check_case(f[0x85],  L2,   2048,  8,  32, data);
	check_case(f[0x86],  L2,    512,  4,  64, data);
	check_case(f[0x87],  L2,   1024,  8,  64, data);
	
	if (f[0x49]) {
		/* This flag is overloaded with two meanings. On Xeon MP
		 * (family 0xf, model 0x6) this means L3 cache. On all other
		 * CPUs (notably Conroe et al), this is L2 cache. In both cases
		 * it means 4MB, 16-way associative, 64-byte line size.
		 */
		if (data->family == 0xf && data->model == 0x6) {
			data->l3_cache = 4096;
			data->l3_assoc = 16;
			data->l3_cacheline = 64;
		} else {
			data->l2_cache = 4096;
			data->l2_assoc = 16;
			data->l2_cacheline = 64;
		}
	}
	if (f[0x40]) {
		/* Again, a special flag. It means:
		 * 1) If no L2 is specified, then CPU is w/o L2 (0 KB)
		 * 2) If L2 is specified by other flags, then, CPU is w/o L3.
		 */
		if (data->l2_cache == -1) {
			data->l2_cache = 0;
		} else {
			data->l3_cache = 0;
		}
	}
}

static void decode_intel_deterministic_cache_info(struct cpu_raw_data_t* raw,
                                                  struct cpu_id_t* data)
{
	int ecx;
	int ways, partitions, linesize, sets, size, level, typenumber;
	cache_type_t type;
	for (ecx = 0; ecx < MAX_INTELFN4_LEVEL; ecx++) {
		typenumber = raw->intel_fn4[ecx][0] & 0x1f;
		if (typenumber == 0) break;
		level = (raw->intel_fn4[ecx][0] >> 5) & 0x7;
		if (level == 1 && typenumber == 1)
			type = L1D;
		else if (level == 1 && typenumber == 2)
			type = L1I;
		else if (level == 2 && typenumber == 3)
			type = L2;
		else if (level == 3 && typenumber == 3)
			type = L3;
		else if (level == 4 && typenumber == 3)
			type = L4;
		else {
			warnf("deterministic_cache: unknown level/typenumber combo (%d/%d), cannot\n", level, typenumber);
			warnf("deterministic_cache: recognize cache type\n");
			continue;
		}
		ways = ((raw->intel_fn4[ecx][1] >> 22) & 0x3ff) + 1;
		partitions = ((raw->intel_fn4[ecx][1] >> 12) & 0x3ff) + 1;
		linesize = (raw->intel_fn4[ecx][1] & 0xfff) + 1;
		sets = raw->intel_fn4[ecx][2] + 1;
		size = ways * partitions * linesize * sets / 1024;
		check_case(1, type, size, ways, linesize, data);
	}
}

static int decode_intel_extended_topology(struct cpu_raw_data_t* raw,
                                           struct cpu_id_t* data)
{
	int i, level_type, num_smt = -1, num_core = -1;
	for (i = 0; i < MAX_INTELFN11_LEVEL; i++) {
		level_type = (raw->intel_fn11[i][2] & 0xff00) >> 8;
		switch (level_type) {
			case 0x01:
				num_smt = raw->intel_fn11[i][1] & 0xffff;
				break;
			case 0x02:
				num_core = raw->intel_fn11[i][1] & 0xffff;
				break;
			default:
				break;
		}
	}
	if (num_smt == -1 || num_core == -1) return 0;
	data->num_logical_cpus = num_core;
	data->num_cores = num_core / num_smt;
	// make sure num_cores is at least 1. In VMs, the CPUID instruction
	// is rigged and may give nonsensical results, but we should at least
	// avoid outputs like data->num_cores == 0.
	if (data->num_cores <= 0) data->num_cores = 1;
	return 1;
}

static void decode_intel_number_of_cores(struct cpu_raw_data_t* raw,
                                         struct cpu_id_t* data)
{
	int logical_cpus = -1, num_cores = -1;
	
	if (raw->basic_cpuid[0][0] >= 11) {
		if (decode_intel_extended_topology(raw, data)) return;
	}
	
	if (raw->basic_cpuid[0][0] >= 1) {
		logical_cpus = (raw->basic_cpuid[1][1] >> 16) & 0xff;
		if (raw->basic_cpuid[0][0] >= 4) {
			num_cores = 1 + ((raw->basic_cpuid[4][0] >> 26) & 0x3f);
		}
	}
	if (data->flags[CPU_FEATURE_HT]) {
		if (num_cores > 1) {
			data->num_cores = num_cores;
			data->num_logical_cpus = logical_cpus;
		} else {
			data->num_cores = 1;
			data->num_logical_cpus = (logical_cpus >= 1 ? logical_cpus : 1);
			if (data->num_logical_cpus == 1)
				data->flags[CPU_FEATURE_HT] = 0;
		}
	} else {
		data->num_cores = data->num_logical_cpus = 1;
	}
}

static intel_code_t get_brand_code(struct cpu_id_t* data)
{
	intel_code_t code = (intel_code_t) NO_CODE;
	int i, need_matchtable = 1, core_ix_base = 0;
	const char* bs = data->brand_str;
	const char* s;
	const struct { intel_code_t c; const char *search; } matchtable[] = {
		{ XEONMP, "Xeon MP" },
		{ XEONMP, "Xeon(TM) MP" },
		{ XEON, "Xeon" },
		{ CELERON, "Celeron" },
		{ MOBILE_PENTIUM_M, "Pentium(R) M" },
		{ CORE_SOLO, "Pentium(R) Dual  CPU" },
		{ CORE_SOLO, "Pentium(R) Dual-Core" },
		{ PENTIUM_D, "Pentium(R) D" },
		{ PENTIUM, "Pentium" },
		{ CORE_SOLO, "Genuine Intel(R) CPU" },
		{ CORE_SOLO, "Intel(R) Core(TM)" },
		{ ATOM_DIAMONDVILLE, "Atom(TM) CPU [N ][23]## " },
		{ ATOM_SILVERTHORNE, "Atom(TM) CPU Z" },
		{ ATOM_PINEVIEW, "Atom(TM) CPU [ND][45]## " },
		{ ATOM_CEDARVIEW, "Atom(TM) CPU [ND]#### " },
		{ ATOM_UNKNOWN,   "Atom(TM) CPU" },
	};

	if (strstr(bs, "Mobile")) {
		need_matchtable = 0;
		if (strstr(bs, "Celeron"))
			code = MOBILE_CELERON;
		else if (strstr(bs, "Pentium"))
			code = MOBILE_PENTIUM;
	}
	if ((i = match_pattern(bs, "Core(TM) i[357]")) != 0) {
		/* Core i3, Core i5 or Core i7 */
		need_matchtable = 0;
		
		core_ix_base = CORE_I3;
		
		/* if it has RdRand, then it is at least Ivy Bridge */
		if (data->flags[CPU_FEATURE_RDRAND])
			core_ix_base = CORE_IVY3;
		/* if it has FMA, then it is at least Haswell */
		if (data->flags[CPU_FEATURE_FMA3])
			core_ix_base = CORE_HASWELL3;
		/* if it has RTM, then it is at least a Broadwell-E or Skylake */
		if (data->flags[CPU_FEATURE_RDSEED])
			core_ix_base = CORE_BROADWELL3;
		
		switch (bs[i + 9]) {
			case '3': code = core_ix_base + 0; break;
			case '5': code = core_ix_base + 1; break;
			case '7': code = core_ix_base + 2; break;
		}
	}
	if (need_matchtable) {
		for (i = 0; i < COUNT_OF(matchtable); i++)
			if (match_pattern(bs, matchtable[i].search)) {
				code = matchtable[i].c;
				break;
			}
		debugf(2, "intel matchtable result is %d\n", code);
	}
	if (code == XEON) {
		if (match_pattern(bs, "W35##") || match_pattern(bs, "[ELXW]75##"))
			code = XEON_I7;
		else if (match_pattern(bs, "[ELXW]55##"))
			code = XEON_GAINESTOWN;
		else if (match_pattern(bs, "[ELXW]56##"))
			code = XEON_WESTMERE;
		else if (data->l3_cache > 0 && data->family == 16)
			/* restrict by family, since later Xeons also have L3 ... */
			code = XEON_IRWIN;
	}
	if (code == XEONMP && data->l3_cache > 0)
		code = XEON_POTOMAC;
	if (code == CORE_SOLO) {
		s = strstr(bs, "CPU");
		if (s) {
			s += 3;
			while (*s == ' ') s++;
			if (*s == 'T')
				code = (data->num_cores == 1) ? MOBILE_CORE_SOLO : MOBILE_CORE_DUO;
		}
	}
	if (code == CORE_SOLO) {
		switch (data->num_cores) {
			case 1: break;
			case 2:
			{
				code = CORE_DUO;
				if (data->num_logical_cpus > 2)
					code = DUAL_CORE_HT;
				break;
			}
			case 4:
			{
				code = QUAD_CORE;
				if (data->num_logical_cpus > 4)
					code = QUAD_CORE_HT;
				break;
			}
			default:
				code = MORE_THAN_QUADCORE; break;
		}
	}
	
	if (code == CORE_DUO && data->ext_model >= 23) {
		code = WOLFDALE;
	}
	if (code == PENTIUM_D && data->ext_model >= 23) {
		code = WOLFDALE;
	}
	if (code == MOBILE_CORE_DUO && data->model != 14) {
		if (data->ext_model < 23) {
			code = MEROM;
		} else {
			code = PENRYN;
		}
	}
	return code;
}

static intel_model_t get_model_code(struct cpu_id_t* data)
{
	int i = 0;
	int l = (int) strlen(data->brand_str);
	const char *bs = data->brand_str;
	int mod_flags = 0, model_no = 0, ndigs = 0;
	/* If the CPU is a Core ix, then just return the model number generation: */
	if ((i = match_pattern(bs, "Core(TM) i[357]")) != 0) {
		i += 11;
		if (i + 4 >= l) return UNKNOWN;
		if (bs[i] == '2') return _2xxx;
		if (bs[i] == '3') return _3xxx;
		return UNKNOWN;
	}
	
	/* For Core2-based Xeons: */
	while (i < l - 3) {
		if (bs[i] == 'C' && bs[i+1] == 'P' && bs[i+2] == 'U')
			break;
		i++;
	}
	if (i >= l - 3) return UNKNOWN;
	i += 3;
	while (i < l - 4 && bs[i] == ' ') i++;
	if (i >= l - 4) return UNKNOWN;
	while (i < l - 4 && !isdigit(bs[i])) {
		if (bs[i] >= 'A' && bs[i] <= 'Z')
			mod_flags |= (1 << (bs[i] - 'A'));
		i++;
	}
	if (i >= l - 4) return UNKNOWN;
	while (isdigit(bs[i])) {
		ndigs++;
		model_no = model_no * 10 + (int) (bs[i] - '0');
		i++;
	}
	if (ndigs != 4) return UNKNOWN;
#define HAVE(ch, flags) ((flags & (1 << ((int)(ch-'A')))) != 0)
	switch (model_no / 100) {
		case 30: return _3000;
		case 31: return _3100;
		case 32:
		{
			return (HAVE('X', mod_flags)) ? X3200 : _3200;
		}
		case 33:
		{
			return (HAVE('X', mod_flags)) ? X3300 : _3300;
		}
		case 51: return _5100;
		case 52: return _5200;
		case 53: return _5300;
		case 54: return _5400;
		default:
			return UNKNOWN;
	}
#undef HAVE
}

static void decode_intel_sgx_features(const struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	struct cpu_epc_t epc;
	int i;
	
	if (raw->basic_cpuid[0][0] < 0x12) return; // no 12h leaf
	if (raw->basic_cpuid[0x12][0] == 0) return; // no sub-leafs available, probably it's disabled by BIOS
	
	// decode sub-leaf 0:
	if (raw->basic_cpuid[0x12][0] & 1) data->sgx.flags[INTEL_SGX1] = 1;
	if (raw->basic_cpuid[0x12][0] & 2) data->sgx.flags[INTEL_SGX2] = 1;
	if (data->sgx.flags[INTEL_SGX1] || data->sgx.flags[INTEL_SGX2])
		data->sgx.present = 1;
	data->sgx.misc_select = raw->basic_cpuid[0x12][1];
	data->sgx.max_enclave_32bit = (raw->basic_cpuid[0x12][3]     ) & 0xff;
	data->sgx.max_enclave_64bit = (raw->basic_cpuid[0x12][3] >> 8) & 0xff;
	
	// decode sub-leaf 1:
	data->sgx.secs_attributes = raw->intel_fn12h[1][0] | (((uint64_t) raw->intel_fn12h[1][1]) << 32);
	data->sgx.secs_xfrm       = raw->intel_fn12h[1][2] | (((uint64_t) raw->intel_fn12h[1][3]) << 32);
	
	// decode higher-order subleafs, whenever present:
	data->sgx.num_epc_sections = -1;
	for (i = 0; i < 1000000; i++) {
		epc = cpuid_get_epc(i, raw);
		if (epc.length == 0) {
			debugf(2, "SGX: epc section request for %d returned null, no more EPC sections.\n", i);
			data->sgx.num_epc_sections = i;
			break;
		}
	}
	if (data->sgx.num_epc_sections == -1) {
		debugf(1, "SGX: warning: seems to be infinitude of EPC sections.\n");
		data->sgx.num_epc_sections = 1000000;
	}
}

struct cpu_epc_t cpuid_get_epc(int index, const struct cpu_raw_data_t* raw)
{
	uint32_t regs[4];
	struct cpu_epc_t retval = {0, 0};
	if (raw && index < MAX_INTELFN12H_LEVEL - 2) {
		// this was queried already, use the data:
		memcpy(regs, raw->intel_fn12h[2 + index], sizeof(regs));
	} else {
		// query this ourselves:
		regs[0] = 0x12;
		regs[2] = 2 + index;
		regs[1] = regs[3] = 0;
		cpu_exec_cpuid_ext(regs);
	}
	
	// decode values:
	if ((regs[0] & 0xf) == 0x1) {
		retval.start_addr |= (regs[0] & 0xfffff000); // bits [12, 32) -> bits [12, 32)
		retval.start_addr |= ((uint64_t) (regs[1] & 0x000fffff)) << 32; // bits [0, 20) -> bits [32, 52)
		retval.length     |= (regs[2] & 0xfffff000); // bits [12, 32) -> bits [12, 32)
		retval.length     |= ((uint64_t) (regs[3] & 0x000fffff)) << 32; // bits [0, 20) -> bits [32, 52)
	}
	return retval;
}

int cpuid_identify_intel(struct cpu_raw_data_t* raw, struct cpu_id_t* data, struct internal_id_info_t* internal)
{
	intel_code_t brand_code;
	intel_model_t model_code;
	int i;
	char* brand_code_str = NULL;

	load_intel_features(raw, data);
	if (raw->basic_cpuid[0][0] >= 4) {
		/* Deterministic way is preferred, being more generic */
		decode_intel_deterministic_cache_info(raw, data);
	} else if (raw->basic_cpuid[0][0] >= 2) {
		decode_intel_oldstyle_cache_info(raw, data);
	}
	decode_intel_number_of_cores(raw, data);

	brand_code  = get_brand_code(data);
	model_code = get_model_code(data);
	for (i = 0; i < COUNT_OF(intel_bcode_str); i++) {
		if (brand_code == intel_bcode_str[i].code) {
			brand_code_str = intel_bcode_str[i].str;
			break;
		}
	}
	if (brand_code_str)
		debugf(2, "Detected Intel brand code: %d (%s)\n", brand_code, brand_code_str);
	else
		debugf(2, "Detected Intel brand code: %d\n", brand_code);
	debugf(2, "Detected Intel model code: %d\n", model_code);
	
	internal->code.intel = brand_code;
	
	if (data->flags[CPU_FEATURE_SGX]) {
		debugf(2, "SGX seems to be present, decoding...\n");
		// if SGX is indicated by the CPU, verify its presence:
		decode_intel_sgx_features(raw, data);
	}

	internal->score = match_cpu_codename(cpudb_intel, COUNT_OF(cpudb_intel), data,
		brand_code, model_code);
	return 0;
}

void cpuid_get_list_intel(struct cpu_list_t* list)
{
	generic_get_cpu_list(cpudb_intel, COUNT_OF(cpudb_intel), list);
}
