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
#include "recog_intel.h"
#include "libcpuid_util.h"


enum _intel_code_t {
	NA,
	NO_CODE,
	PENTIUM = 10,
	MOBILE_PENTIUM,
	
	XEON = 20,
	XEON_IRWIN,
	XEONMP,
	XEON_POTOMAC,
	XEON_I7,
	XEON_GAINESTOWN,
	XEON_WESTMERE,
	
	MOBILE_PENTIUM_M = 30,
	CELERON,
	MOBILE_CELERON,
	NOT_CELERON,
	
	
	CORE_SOLO = 40,
	MOBILE_CORE_SOLO,
	CORE_DUO,
	MOBILE_CORE_DUO,
	
	WOLFDALE = 50,
	MEROM,
	PENRYN,
	QUAD_CORE,
	DUAL_CORE_HT,
	QUAD_CORE_HT,
	MORE_THAN_QUADCORE,
	PENTIUM_D,
	
	ATOM = 60,
	ATOM_SILVERTHORNE,
	ATOM_DIAMONDVILLE,
	ATOM_PINEVIEW,
	ATOM_CEDARVIEW,
	
	CORE_I3 = 70,
	CORE_I5,
	CORE_I7,
	CORE_IVY3, /* 22nm Core-iX */
	CORE_IVY5,
	CORE_IVY7,
	CORE_HASWELL3, /* 22nm Core-iX, Haswell */
	CORE_HASWELL5,
	CORE_HASWELL7,
};
typedef enum _intel_code_t intel_code_t;

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
	
	{  6,  3, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon"               },
	{  6,  5, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon"               },
	{  6,  6, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-II Xeon"               },
		
	{  6,  5, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-II Celeron (no L2)"    },
	{  6,  6, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-II Celeron (128K)"     },
	
	/* -------------------------------------------------- */
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Katmai)"    },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Coppermine)"},
	{  6, 10, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Coppermine)"},
	{  6, 11, -1, -1, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium III (Tualatin)"  },
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon"              },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon"              },
	{  6, 10, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon"              },
	{  6, 11, -1, -1, -1,   1,    -1,    -1, XEON              ,     0, "P-III Xeon"              },
	
	{  6,  7, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron"           },
	{  6,  8, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron"           },
	{  6, 10, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron"           },
	{  6, 11, -1, -1, -1,   1,    -1,    -1, CELERON           ,     0, "P-III Celeron"           },
	
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
	{ 15,  4,  4, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium D"               },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, PENTIUM_D         ,     0, "Pentium D"               },
	{ 15,  4,  7, 15, -1,   1,    -1,    -1, NO_CODE           ,     0, "Pentium D"               },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, PENTIUM_D         ,     0, "Pentium D"               },

	/* Celeron and Celeron Ds */
	{ 15,  1, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron (128K)"      }, 
	{ 15,  2, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "P-4 Celeron (128K)"      },
	{ 15,  3, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "Celeron D"               },
	{ 15,  4, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "Celeron D"               },
	{ 15,  6, -1, 15, -1,   1,    -1,    -1, CELERON           ,     0, "Celeron D"               },
	
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
	
	{  6, 12, -1, -1, -1,  -1,    -1,    -1, ATOM              ,     0, "Unknown Atom"               },
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
	{  6,  7, -1, -1, 23,   2,  3072,    -1, PENRYN            ,     0, "Penryn (Core 2 Duo) 3M"   },
	{  6,  7, -1, -1, 23,   2,  6144,    -1, PENRYN            ,     0, "Penryn (Core 2 Duo) 6M"   },
	{  6,  7, -1, -1, 23,   4,  2048,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 2M"},
	{  6,  7, -1, -1, 23,   4,  3072,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 3M"},
	{  6,  7, -1, -1, 23,   4,  6144,    -1, QUAD_CORE         ,     0, "Yorkfield (Core 2 Quad) 6M"},
	
	{  6,  5, -1, -1, 37,   2,    -1,    -1, NO_CODE           ,     0, "Unknown Core i3/i5 CPU"   },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I7           ,     0, "Arrandale (Core i7)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  3072, CORE_I5           ,     0, "Arrandale (Core i5)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I5           ,     0, "Clarkdale (Core i5)"      },
	{  6,  5, -1, -1, 37,   4,    -1,  8192, CORE_I5           ,     0, "Lynnfield (Core i5)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  3072, CORE_I3           ,     0, "Arrandale (Core i3)"      },
	{  6,  5, -1, -1, 37,   2,    -1,  4096, CORE_I3           ,     0, "Clarkdale (Core i3)"      },

	{  6, 10, -1, -1, 42,  -1,    -1,    -1, NO_CODE           ,     0, "Unknown Sandy Bridge"     },
	{  6, 10, -1, -1, 42,  -1,    -1,    -1, CORE_I7           ,     0, "Sandy Bridge i7"          },
	{  6, 10, -1, -1, 42,   4,    -1,    -1, CORE_I7           ,     0, "Sandy Bridge (Core i7)"   },
	{  6, 10, -1, -1, 42,   4,    -1,    -1, CORE_I5           ,     0, "Sandy Bridge (Core i5)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, CORE_I3           ,     0, "Sandy Bridge (Core i3)"   },
	{  6, 10, -1, -1, 42,   1,    -1,    -1, CELERON           ,     0, "Celeron (Sandy Bridge)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, CELERON           ,     0, "Celeron (Sandy Bridge)"   },
	{  6, 10, -1, -1, 42,   2,    -1,    -1, PENTIUM           ,     0, "Pentium (Sandy Bridge)"   },

	{  6, 10, -1, -1, 26,   1,    -1,    -1, CORE_I7           ,     0, "Intel Core i7"            },
	{  6, 10, -1, -1, 26,   4,    -1,    -1, CORE_I7           ,     0, "Bloomfield (Core i7)"     },
	{  6, 10, -1, -1, 30,   4,    -1,    -1, CORE_I7           ,     0, "Lynnfield (Core i7)"      },
	{  6, 10, -1, -1, 26,   4,    -1,    -1, XEON_I7           ,     0, "Xeon (Bloomfield)"        },

	{  6, 10, -1, -1, 26,   4,    -1,    -1, XEON_GAINESTOWN   ,     0, "Xeon (Gainestown)"        },
	{  6, 10, -1, -1, 26,   4,    -1,  4096, XEON_GAINESTOWN   ,     0, "Xeon (Gainestown) 4M"     },
	{  6, 10, -1, -1, 26,   4,    -1,  8192, XEON_GAINESTOWN   ,     0, "Xeon (Gainestown) 8M"     },
	
	{  6, 12, -1, -1, 44,  -1,    -1,    -1, XEON_WESTMERE     ,     0, "Xeon (Westmere-based)"    },
	{  6, 12, -1, -1, 44,   4,    -1, 12288, CORE_I7           ,     0, "Gulftown (Core i7)"       },
	{  6, 12, -1, -1, 44,  -1,    -1, 12288, XEON_WESTMERE     ,     0, "Xeon (Gulftown)"          },

	{  6, 13, -1, -1, 45,  -1,    -1,    -1, XEON              ,     0, "Xeon (Sandy Bridge)"      },
	
	{  6, 13, -1, -1, 45,  -1,    -1,    -1, CORE_I7           ,     0, "Sandy Bridge-E (Core i7)"   },
	{  6, 13, -1, -1, 45,  -1,    -1,    -1, CORE_I5           ,     0, "Sandy Bridge-E (Core i5)"   },
	{  6, 13, -1, -1, 45,  -1,    -1,    -1, CORE_I3           ,     0, "Sandy Bridge-E (Core i3)"   },

	{  6, 10, -1, -1, 58,   4,    -1,    -1, CORE_IVY7         ,     0, "Ivy Bridge (Core i7)"   },
	{  6, 10, -1, -1, 58,   4,    -1,    -1, CORE_IVY5         ,     0, "Ivy Bridge (Core i5)"   },
	{  6, 10, -1, -1, 58,   2,    -1,    -1, CORE_IVY3         ,     0, "Ivy Bridge (Core i3)"   },

	{  6, 12, -1, -1, 60,   4,    -1,    -1, CORE_HASWELL7     ,     0, "Haswell (Core i7)"   },
	{  6, 12, -1, -1, 60,   4,    -1,    -1, CORE_HASWELL5     ,     0, "Haswell (Core i5)"   },
	{  6, 12, -1, -1, 60,   2,    -1,    -1, CORE_HASWELL3     ,     0, "Haswell (Core i3)"   },

	
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
		{  1, CPU_FEATURE_PCLMUL },
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
		{ 20, CPU_FEATURE_SSE4_2 },
		{ 22, CPU_FEATURE_MOVBE },
		{ 25, CPU_FEATURE_AES },
		{ 26, CPU_FEATURE_XSAVE },
		{ 27, CPU_FEATURE_OSXSAVE },
		{ 28, CPU_FEATURE_AVX },
		{ 30, CPU_FEATURE_RDRAND },
	};
	const struct feature_map_t matchtable_ebx7[] = {
		{  5, CPU_FEATURE_AVX2 },
	};
	const struct feature_map_t matchtable_edx81[] = {
		{ 20, CPU_FEATURE_XD },
	};
	if (raw->basic_cpuid[0][0] >= 1) {
		match_features(matchtable_edx1, COUNT_OF(matchtable_edx1), raw->basic_cpuid[1][3], data);
		match_features(matchtable_ecx1, COUNT_OF(matchtable_ecx1), raw->basic_cpuid[1][2], data);
	}
	if (raw->basic_cpuid[0][0] >= 7) {
		match_features(matchtable_ebx7, COUNT_OF(matchtable_ebx7), raw->basic_cpuid[7][1], data);
	}
	if (raw->ext_cpuid[0][0] >= 1) {
		match_features(matchtable_edx81, COUNT_OF(matchtable_edx81), raw->ext_cpuid[1][3], data);
	}
}

enum _cache_type_t {
	L1I,
	L1D,
	L2,
	L3
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
	data->num_cores = num_core / num_smt;
	data->num_logical_cpus = num_core;
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
			data->num_logical_cpus = (logical_cpus >= 2 ? logical_cpus : 2);
		}
	} else {
		data->num_cores = data->num_logical_cpus = 1;
	}
}

static intel_code_t get_brand_code(struct cpu_id_t* data)
{
	intel_code_t code = NO_CODE;
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
		{ PENTIUM_D, "Pentium(R) D" },
		{ PENTIUM, "Pentium" },
		{ CORE_SOLO, "Genuine Intel(R) CPU" },
		{ CORE_SOLO, "Intel(R) Core(TM)" },
		{ ATOM_DIAMONDVILLE, "Atom(TM) CPU [N ][23]## " },
		{ ATOM_SILVERTHORNE, "Atom(TM) CPU Z" },
		{ ATOM_PINEVIEW, "Atom(TM) CPU D" },
		{ ATOM_CEDARVIEW, "Atom(TM) CPU N####" },
		{ ATOM,              "Atom(TM) CPU" },
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
		else if (data->l3_cache > 0)
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

int cpuid_identify_intel(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	load_intel_features(raw, data);
	if (raw->basic_cpuid[0][0] >= 4) {
		/* Deterministic way is preferred, being more generic */
		decode_intel_deterministic_cache_info(raw, data);
	} else if (raw->basic_cpuid[0][0] >= 2) {
		decode_intel_oldstyle_cache_info(raw, data);
	}
	decode_intel_number_of_cores(raw, data);
	match_cpu_codename(cpudb_intel, COUNT_OF(cpudb_intel), data,
		get_brand_code(data), get_model_code(data));
	return 0;
}

void cpuid_get_list_intel(struct cpu_list_t* list)
{
	generic_get_cpu_list(cpudb_intel, COUNT_OF(cpudb_intel), list);
}
