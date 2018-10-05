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
#include <ctype.h>
#include "libcpuid.h"
#include "libcpuid_util.h"
#include "libcpuid_internal.h"
#include "recog_amd.h"

const struct amd_code_str { amd_code_t code; char *str; } amd_code_str[] = {
	#define CODE(x) { x, #x }
	#define CODE2(x, y) CODE(x)
	#include "amd_code_t.h"
	#undef CODE
};

const struct match_entry_t cpudb_amd[] = {
	{ -1, -1, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown AMD CPU"               },
	
	/* 486 and the likes */
	{  4, -1, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown AMD 486"               },
	{  4,  3, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "AMD 486DX2"                    },
	{  4,  7, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "AMD 486DX2WB"                  },
	{  4,  8, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "AMD 486DX4"                    },
	{  4,  9, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "AMD 486DX4WB"                  },
	
	/* Pentia clones */
	{  5, -1, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown AMD 586"               },
	{  5,  0, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K5"                            },
	{  5,  1, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K5"                            },
	{  5,  2, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K5"                            },
	{  5,  3, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K5"                            },
	
	/* The K6 */
	{  5,  6, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K6"                            },
	{  5,  7, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K6"                            },
	
	{  5,  8, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K6-2"                          },
	{  5,  9, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K6-III"                        },
	{  5, 10, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown K6"                    },
	{  5, 11, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown K6"                    },
	{  5, 12, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown K6"                    },
	{  5, 13, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "K6-2+"                         },
	
	/* Athlon et al. */
	{  6,  1, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Athlon (Slot-A)"               },
	{  6,  2, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Athlon (Slot-A)"               },
	{  6,  3, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Duron (Spitfire)"              },
	{  6,  4, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Athlon (ThunderBird)"          },
	
	{  6,  6, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown Athlon"                },
	{  6,  6, -1, -1,   -1,   1,    -1,    -1, ATHLON                  ,     0, "Athlon (Palomino)"             },
	{  6,  6, -1, -1,   -1,   1,    -1,    -1, ATHLON_MP               ,     0, "Athlon MP (Palomino)"          },
	{  6,  6, -1, -1,   -1,   1,    -1,    -1, DURON                   ,     0, "Duron (Palomino)"              },
	{  6,  6, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP               ,     0, "Athlon XP"                     },
	
	{  6,  7, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown Athlon XP"             },
	{  6,  7, -1, -1,   -1,   1,    -1,    -1, DURON                   ,     0, "Duron (Morgan)"                },
	
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Athlon XP"                     },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, ATHLON                  ,     0, "Athlon XP (Thoroughbred)"      },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP               ,     0, "Athlon XP (Thoroughbred)"      },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, DURON                   ,     0, "Duron (Applebred)"             },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, SEMPRON                 ,     0, "Sempron (Thoroughbred)"        },
	{  6,  8, -1, -1,   -1,   1,   128,    -1, SEMPRON                 ,     0, "Sempron (Thoroughbred)"        },
	{  6,  8, -1, -1,   -1,   1,   256,    -1, SEMPRON                 ,     0, "Sempron (Thoroughbred)"        },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, ATHLON_MP               ,     0, "Athlon MP (Thoroughbred)"      },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP_M             ,     0, "Mobile Athlon (T-Bred)"        },
	{  6,  8, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP_M_LV          ,     0, "Mobile Athlon (T-Bred)"        },
	
	{  6, 10, -1, -1,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Athlon XP (Barton)"            },
	{  6, 10, -1, -1,   -1,   1,   512,    -1, ATHLON_XP               ,     0, "Athlon XP (Barton)"            },
	{  6, 10, -1, -1,   -1,   1,   512,    -1, SEMPRON                 ,     0, "Sempron (Barton)"              },
	{  6, 10, -1, -1,   -1,   1,   256,    -1, SEMPRON                 ,     0, "Sempron (Thorton)"             },
	{  6, 10, -1, -1,   -1,   1,   256,    -1, ATHLON_XP               ,     0, "Athlon XP (Thorton)"           },
	{  6, 10, -1, -1,   -1,   1,    -1,    -1, ATHLON_MP               ,     0, "Athlon MP (Barton)"            },
	{  6, 10, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP_M             ,     0, "Mobile Athlon (Barton)"        },
	{  6, 10, -1, -1,   -1,   1,    -1,    -1, ATHLON_XP_M_LV          ,     0, "Mobile Athlon (Barton)"        },
	
	/* K8 Architecture */
	{ 15, -1, -1, 15,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown K8"                    },
	{ 15, -1, -1, 16,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown K9"                    },
	
	{ 15, -1, -1, 15,   -1,   1,    -1,    -1, NO_CODE                 ,     0, "Unknown A64"                   },
	{ 15, -1, -1, 15,   -1,   1,    -1,    -1, OPTERON_SINGLE          ,     0, "Opteron"                       },
	{ 15, -1, -1, 15,   -1,   2,    -1,    -1, OPTERON_DUALCORE        ,     0, "Opteron (Dual Core)"           },
	{ 15,  3, -1, 15,   -1,   1,    -1,    -1, OPTERON_SINGLE          ,     0, "Opteron"                       },
	{ 15,  3, -1, 15,   -1,   2,    -1,    -1, OPTERON_DUALCORE        ,     0, "Opteron (Dual Core)"           },
	{ 15, -1, -1, 15,   -1,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (512K)"              },
	{ 15, -1, -1, 15,   -1,   1,  1024,    -1, ATHLON_64               ,     0, "Athlon 64 (1024K)"             },
	{ 15, -1, -1, 15,   -1,   1,    -1,    -1, ATHLON_FX               ,     0, "Athlon FX"                     },
	{ 15, -1, -1, 15,   -1,   1,    -1,    -1, ATHLON_64_FX            ,     0, "Athlon 64 FX"                  },
	{ 15,  3, -1, 15,   35,   2,    -1,    -1, ATHLON_64_FX            ,     0, "Athlon 64 FX X2 (Toledo)"      },
	{ 15, -1, -1, 15,   -1,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (512K)"           },
	{ 15, -1, -1, 15,   -1,   2,  1024,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (1024K)"          },
	{ 15, -1, -1, 15,   -1,   1,   512,    -1, TURION_64               ,     0, "Turion 64 (512K)"              },
	{ 15, -1, -1, 15,   -1,   1,  1024,    -1, TURION_64               ,     0, "Turion 64 (1024K)"             },
	{ 15, -1, -1, 15,   -1,   2,   512,    -1, TURION_X2               ,     0, "Turion 64 X2 (512K)"           },
	{ 15, -1, -1, 15,   -1,   2,  1024,    -1, TURION_X2               ,     0, "Turion 64 X2 (1024K)"          },
	{ 15, -1, -1, 15,   -1,   1,   128,    -1, SEMPRON                 ,     0, "A64 Sempron (128K)"            },
	{ 15, -1, -1, 15,   -1,   1,   256,    -1, SEMPRON                 ,     0, "A64 Sempron (256K)"            },
	{ 15, -1, -1, 15,   -1,   1,   512,    -1, SEMPRON                 ,     0, "A64 Sempron (512K)"            },
	{ 15, -1, -1, 15, 0x4f,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Orleans/512K)"      },
	{ 15, -1, -1, 15, 0x5f,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Orleans/512K)"      },
	{ 15, -1, -1, 15, 0x2f,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Venice/512K)"       },
	{ 15, -1, -1, 15, 0x2c,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Venice/512K)"       },
	{ 15, -1, -1, 15, 0x1f,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Winchester/512K)"   },
	{ 15, -1, -1, 15, 0x0c,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (Newcastle/512K)"    },
	{ 15, -1, -1, 15, 0x27,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (San Diego/512K)"    },
	{ 15, -1, -1, 15, 0x37,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (San Diego/512K)"    },
	{ 15, -1, -1, 15, 0x04,   1,   512,    -1, ATHLON_64               ,     0, "Athlon 64 (ClawHammer/512K)"   },
	
	{ 15, -1, -1, 15, 0x5f,   1,  1024,    -1, ATHLON_64               ,     0, "Athlon 64 (Orleans/1024K)"     },
	{ 15, -1, -1, 15, 0x27,   1,  1024,    -1, ATHLON_64               ,     0, "Athlon 64 (San Diego/1024K)"   },
	{ 15, -1, -1, 15, 0x04,   1,  1024,    -1, ATHLON_64               ,     0, "Athlon 64 (ClawHammer/1024K)"  },
	
	{ 15, -1, -1, 15, 0x4b,   2,   256,    -1, SEMPRON_DUALCORE        ,     0, "Athlon 64 X2 (Windsor/256K)"   },
	
	{ 15, -1, -1, 15, 0x23,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Toledo/512K)"    },
	{ 15, -1, -1, 15, 0x4b,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Windsor/512K)"   },
	{ 15, -1, -1, 15, 0x43,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Windsor/512K)"   },
	{ 15, -1, -1, 15, 0x6b,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Brisbane/512K)"  },
	{ 15, -1, -1, 15, 0x2b,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Manchester/512K)"},
	
	{ 15, -1, -1, 15, 0x23,   2,  1024,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Toledo/1024K)"   },
	{ 15, -1, -1, 15, 0x43,   2,  1024,    -1, ATHLON_64_X2            ,     0, "Athlon 64 X2 (Windsor/1024K)"  },
	
	{ 15, -1, -1, 15, 0x08,   1,   128,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Dublin/128K)"},
	{ 15, -1, -1, 15, 0x08,   1,   256,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Dublin/256K)"},
	{ 15, -1, -1, 15, 0x0c,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Paris)"            },
	{ 15, -1, -1, 15, 0x1c,   1,   128,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/128K)"     },
	{ 15, -1, -1, 15, 0x1c,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/256K)"     },
	{ 15, -1, -1, 15, 0x1c,   1,   128,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Sonora/128K)"},
	{ 15, -1, -1, 15, 0x1c,   1,   256,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Sonora/256K)"},
	{ 15, -1, -1, 15, 0x2c,   1,   128,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/128K)"     },
	{ 15, -1, -1, 15, 0x2c,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/256K)"     },
	{ 15, -1, -1, 15, 0x2c,   1,   128,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Albany/128K)"},
	{ 15, -1, -1, 15, 0x2c,   1,   256,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Albany/256K)"},
	{ 15, -1, -1, 15, 0x2f,   1,   128,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/128K)"     },
	{ 15, -1, -1, 15, 0x2f,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Palermo/256K)"     },
	{ 15, -1, -1, 15, 0x4f,   1,   128,    -1, SEMPRON                 ,     0, "Sempron 64 (Manila/128K)"      },
	{ 15, -1, -1, 15, 0x4f,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Manila/256K)"      },
	{ 15, -1, -1, 15, 0x5f,   1,   128,    -1, SEMPRON                 ,     0, "Sempron 64 (Manila/128K)"      },
	{ 15, -1, -1, 15, 0x5f,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Manila/256K)"      },
	{ 15, -1, -1, 15, 0x6b,   2,   256,    -1, SEMPRON                 ,     0, "Sempron 64 Dual (Sherman/256K)"},
	{ 15, -1, -1, 15, 0x6b,   2,   512,    -1, SEMPRON                 ,     0, "Sempron 64 Dual (Sherman/512K)"},
	{ 15, -1, -1, 15, 0x7f,   1,   256,    -1, SEMPRON                 ,     0, "Sempron 64 (Sparta/256K)"      },
	{ 15, -1, -1, 15, 0x7f,   1,   512,    -1, SEMPRON                 ,     0, "Sempron 64 (Sparta/512K)"      },
	{ 15, -1, -1, 15, 0x4c,   1,   256,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Keene/256K)"},
	{ 15, -1, -1, 15, 0x4c,   1,   512,    -1, M_SEMPRON               ,     0, "Mobile Sempron 64 (Keene/512K)"},
	{ 15, -1, -1, 15,   -1,   2,    -1,    -1, SEMPRON_DUALCORE        ,     0, "Sempron Dual Core"             },
	
	{ 15, -1, -1, 15, 0x24,   1,   512,    -1, TURION_64               ,     0, "Turion 64 (Lancaster/512K)"    },
	{ 15, -1, -1, 15, 0x24,   1,  1024,    -1, TURION_64               ,     0, "Turion 64 (Lancaster/1024K)"   },
	{ 15, -1, -1, 15, 0x48,   2,   256,    -1, TURION_X2               ,     0, "Turion X2 (Taylor)"            },
	{ 15, -1, -1, 15, 0x48,   2,   512,    -1, TURION_X2               ,     0, "Turion X2 (Trinidad)"          },
	{ 15, -1, -1, 15, 0x4c,   1,   512,    -1, TURION_64               ,     0, "Turion 64 (Richmond)"          },
	{ 15, -1, -1, 15, 0x68,   2,   256,    -1, TURION_X2               ,     0, "Turion X2 (Tyler/256K)"        },
	{ 15, -1, -1, 15, 0x68,   2,   512,    -1, TURION_X2               ,     0, "Turion X2 (Tyler/512K)"        },
	{ 15, -1, -1, 17,    3,   2,   512,    -1, TURION_X2               ,     0, "Turion X2 (Griffin/512K)"      },
	{ 15, -1, -1, 17,    3,   2,  1024,    -1, TURION_X2               ,     0, "Turion X2 (Griffin/1024K)"     },
	
	/* K9 Architecture */
	{ 15, -1, -1, 16,   -1,   1,    -1,    -1, PHENOM                  ,     0, "Unknown AMD Phenom"            },
	{ 15,  2, -1, 16,   -1,   1,    -1,    -1, PHENOM                  ,     0, "Phenom"                        },
	{ 15,  2, -1, 16,   -1,   3,    -1,    -1, PHENOM                  ,     0, "Phenom X3 (Toliman)"           },
	{ 15,  2, -1, 16,   -1,   4,    -1,    -1, PHENOM                  ,     0, "Phenom X4 (Agena)"             },
	{ 15,  2, -1, 16,   -1,   3,   512,    -1, PHENOM                  ,     0, "Phenom X3 (Toliman/256K)"      },
	{ 15,  2, -1, 16,   -1,   3,   512,    -1, PHENOM                  ,     0, "Phenom X3 (Toliman/512K)"      },
	{ 15,  2, -1, 16,   -1,   4,   128,    -1, PHENOM                  ,     0, "Phenom X4 (Agena/128K)"        },
	{ 15,  2, -1, 16,   -1,   4,   256,    -1, PHENOM                  ,     0, "Phenom X4 (Agena/256K)"        },
	{ 15,  2, -1, 16,   -1,   4,   512,    -1, PHENOM                  ,     0, "Phenom X4 (Agena/512K)"        },
	{ 15,  2, -1, 16,   -1,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon X2 (Kuma)"              },
	/* Phenom II derivates: */
	{ 15,  4, -1, 16,   -1,   4,    -1,    -1, NO_CODE                 ,     0, "Phenom (Deneb-based)"          },
	{ 15,  4, -1, 16,   -1,   1,  1024,    -1, SEMPRON                 ,     0, "Sempron (Sargas)"              },
	{ 15,  4, -1, 16,   -1,   2,   512,    -1, PHENOM2                 ,     0, "Phenom II X2 (Callisto)"       },
	{ 15,  4, -1, 16,   -1,   3,   512,    -1, PHENOM2                 ,     0, "Phenom II X3 (Heka)"           },
	{ 15,  4, -1, 16,   -1,   4,   512,    -1, PHENOM2                 ,     0, "Phenom II X4"                  },
	{ 15,  4, -1, 16,    4,   4,   512,    -1, PHENOM2                 ,     0, "Phenom II X4 (Deneb)"          },
	{ 15,  5, -1, 16,    5,   4,   512,    -1, PHENOM2                 ,     0, "Phenom II X4 (Deneb)"          },
	{ 15,  4, -1, 16,   10,   4,   512,    -1, PHENOM2                 ,     0, "Phenom II X4 (Zosma)"          },
	{ 15,  4, -1, 16,   10,   6,   512,    -1, PHENOM2                 ,     0, "Phenom II X6 (Thuban)"         },
	
	{ 15,  6, -1, 16,    6,   2,   512,    -1, ATHLON                  ,     0, "Athlon II (Champlain)"         },
	{ 15,  6, -1, 16,    6,   2,   512,    -1, ATHLON_64_X2            ,     0, "Athlon II X2 (Regor)"          },
	{ 15,  6, -1, 16,    6,   2,  1024,    -1, ATHLON_64_X2            ,     0, "Athlon II X2 (Regor)"          },
	{ 15,  5, -1, 16,    5,   3,   512,    -1, ATHLON_64_X3            ,     0, "Athlon II X3 (Rana)"           },
	{ 15,  5, -1, 16,    5,   4,   512,    -1, ATHLON_64_X4            ,     0, "Athlon II X4 (Propus)"         },

	/* 2011 CPUs: K10 architecture: Llano */
	{ 15,  1, -1, 18,    1,   2,   512,    -1, FUSION_EA               ,     0, "Llano X2"                      },
	{ 15,  1, -1, 18,    1,   2,  1024,    -1, FUSION_EA               ,     0, "Llano X2"                      },
	{ 15,  1, -1, 18,    1,   3,  1024,    -1, FUSION_EA               ,     0, "Llano X3"                      },
	{ 15,  1, -1, 18,    1,   4,  1024,    -1, FUSION_EA               ,     0, "Llano X4"                      },
	/* 2011 CPUs: Bobcat architecture: Ontario, Zacate, Desna, Hondo */
	{ 15,  2, -1, 20,   -1,   1,   512,    -1, FUSION_C                ,     0, "Brazos Ontario"                },
	{ 15,  2, -1, 20,   -1,   2,   512,    -1, FUSION_C                ,     0, "Brazos Ontario (Dual-core)"    },
	{ 15,  1, -1, 20,   -1,   1,   512,    -1, FUSION_E                ,     0, "Brazos Zacate"                 },
	{ 15,  1, -1, 20,   -1,   2,   512,    -1, FUSION_E                ,     0, "Brazos Zacate (Dual-core)"     },
	{ 15,  2, -1, 20,   -1,   2,   512,    -1, FUSION_Z                ,     0, "Brazos Desna (Dual-core)"      },
	/* 2012 CPUs: Piledriver architecture: Trinity and Richland */
	{ 15,  0, -1, 21,   10,   2,  1024,    -1, FUSION_A                ,     0, "Trinity X2"                    },
	{ 15,  0, -1, 21,   16,   2,  1024,    -1, FUSION_A                ,     0, "Trinity X2"                    },
	{ 15,  0, -1, 21,   10,   4,  1024,    -1, FUSION_A                ,     0, "Trinity X4"                    },
	{ 15,  0, -1, 21,   16,   4,  1024,    -1, FUSION_A                ,     0, "Trinity X4"                    },
	{ 15,  3, -1, 21,   13,   2,  1024,    -1, FUSION_A                ,     0, "Richland X2"                   },
	{ 15,  3, -1, 21,   13,   4,  1024,    -1, FUSION_A                ,     0, "Richland X4"                   },
	/* 2013 CPUs: Jaguar architecture: Kabini and Temash */
	{ 15,  0, -1, 22,    0,   2,  1024,    -1, FUSION_A                ,     0, "Kabini X2"                     },
	{ 15,  0, -1, 22,    0,   4,  1024,    -1, FUSION_A                ,     0, "Kabini X4"                     },
	/* 2014 CPUs: Steamroller architecture: Kaveri */
	{ 15,  0, -1, 21,   30,   2,  1024,    -1, FUSION_A                ,     0, "Kaveri X2"                     },
	{ 15,  0, -1, 21,   30,   4,  1024,    -1, FUSION_A                ,     0, "Kaveri X4"                     },
	/* 2014 CPUs: Puma architecture: Beema and Mullins */
	{ 15,  0, -1, 22,   30,   2,  1024,    -1, FUSION_E                ,     0, "Mullins X2"                     },
	{ 15,  0, -1, 22,   30,   4,  1024,    -1, FUSION_A                ,     0, "Mullins X4"                     },
	/* 2015 CPUs: Excavator architecture: Carrizo */
	{ 15,  1, -1, 21,   60,   2,  1024,    -1, FUSION_A                ,     0, "Carrizo X2"                     },
	{ 15,  1, -1, 21,   60,   4,  1024,    -1, FUSION_A                ,     0, "Carrizo X4"                     },
	/* 2015 CPUs: Steamroller architecture: Godavari */
	//TODO
	/* 2016 CPUs: Excavator architecture: Bristol Ridge */
	//TODO
	
	/* Newer Opterons: */
	{ 15,  9, -1, 22,    9,   8,    -1,    -1, OPTERON_GENERIC         ,     0, "Magny-Cours Opteron"           },
	
	/* Bulldozer CPUs: */
	{ 15, -1, -1, 21,    0,   4,  2048,    -1, NO_CODE                 ,     0, "Bulldozer X2"                  },
	{ 15, -1, -1, 21,    1,   4,  2048,    -1, NO_CODE                 ,     0, "Bulldozer X2"                  },
	{ 15, -1, -1, 21,    1,   6,  2048,    -1, NO_CODE                 ,     0, "Bulldozer X3"                  },
	{ 15, -1, -1, 21,    1,   8,  2048,    -1, NO_CODE                 ,     0, "Bulldozer X4"                  },
	/* Piledriver CPUs: */
	{ 15, -1, -1, 21,    2,   4,  2048,    -1, NO_CODE                 ,     0, "Vishera X2"                    },
	{ 15, -1, -1, 21,    2,   6,  2048,    -1, NO_CODE                 ,     0, "Vishera X3"                    },
	{ 15, -1, -1, 21,    2,   8,  2048,    -1, NO_CODE                 ,     0, "Vishera X4"                    },
	/* Steamroller CPUs: */
	//TODO
	/* Excavator CPUs: */
	//TODO
	/* Zen CPUs: */
	//TODO
};


static void load_amd_features(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	const struct feature_map_t matchtable_edx81[] = {
		{ 20, CPU_FEATURE_NX },
		{ 22, CPU_FEATURE_MMXEXT },
		{ 25, CPU_FEATURE_FXSR_OPT },
		{ 30, CPU_FEATURE_3DNOWEXT },
		{ 31, CPU_FEATURE_3DNOW },
	};
	const struct feature_map_t matchtable_ecx81[] = {
		{  1, CPU_FEATURE_CMP_LEGACY },
		{  2, CPU_FEATURE_SVM },
		{  5, CPU_FEATURE_ABM },
		{  6, CPU_FEATURE_SSE4A },
		{  7, CPU_FEATURE_MISALIGNSSE },
		{  8, CPU_FEATURE_3DNOWPREFETCH },
		{  9, CPU_FEATURE_OSVW },
		{ 10, CPU_FEATURE_IBS },
		{ 11, CPU_FEATURE_XOP },
		{ 12, CPU_FEATURE_SKINIT },
		{ 13, CPU_FEATURE_WDT },
		{ 16, CPU_FEATURE_FMA4 },
		{ 21, CPU_FEATURE_TBM },
	};
	const struct feature_map_t matchtable_edx87[] = {
		{  0, CPU_FEATURE_TS },
		{  1, CPU_FEATURE_FID },
		{  2, CPU_FEATURE_VID },
		{  3, CPU_FEATURE_TTP },
		{  4, CPU_FEATURE_TM_AMD },
		{  5, CPU_FEATURE_STC },
		{  6, CPU_FEATURE_100MHZSTEPS },
		{  7, CPU_FEATURE_HWPSTATE },
		/* id 8 is handled in common */
		{  9, CPU_FEATURE_CPB },
		{ 10, CPU_FEATURE_APERFMPERF },
		{ 11, CPU_FEATURE_PFI },
		{ 12, CPU_FEATURE_PA },
	};
	if (raw->ext_cpuid[0][0] >= 0x80000001) {
		match_features(matchtable_edx81, COUNT_OF(matchtable_edx81), raw->ext_cpuid[1][3], data);
		match_features(matchtable_ecx81, COUNT_OF(matchtable_ecx81), raw->ext_cpuid[1][2], data);
	}
	if (raw->ext_cpuid[0][0] >= 0x80000007)
		match_features(matchtable_edx87, COUNT_OF(matchtable_edx87), raw->ext_cpuid[7][3], data);
	if (raw->ext_cpuid[0][0] >= 0x8000001a) {
		/* We have the extended info about SSE unit size */
		data->detection_hints[CPU_HINT_SSE_SIZE_AUTH] = 1;
		data->sse_size = (raw->ext_cpuid[0x1a][0] & 1) ? 128 : 64;
	}
}

static void decode_amd_cache_info(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	int l3_result;
	const int assoc_table[16] = {
		0, 1, 2, 0, 4, 0, 8, 0, 16, 0, 32, 48, 64, 96, 128, 255
	};
	unsigned n = raw->ext_cpuid[0][0];
	
	if (n >= 0x80000005) {
		data->l1_data_cache = (raw->ext_cpuid[5][2] >> 24) & 0xff;
		data->l1_assoc = (raw->ext_cpuid[5][2] >> 16) & 0xff;
		data->l1_cacheline = (raw->ext_cpuid[5][2]) & 0xff;
		data->l1_instruction_cache = (raw->ext_cpuid[5][3] >> 24) & 0xff;
	}
	if (n >= 0x80000006) {
		data->l2_cache = (raw->ext_cpuid[6][2] >> 16) & 0xffff;
		data->l2_assoc = assoc_table[(raw->ext_cpuid[6][2] >> 12) & 0xf];
		data->l2_cacheline = (raw->ext_cpuid[6][2]) & 0xff;
		
		l3_result = (raw->ext_cpuid[6][3] >> 18);
		if (l3_result > 0) {
			l3_result = 512 * l3_result; /* AMD spec says it's a range,
			                                but we take the lower bound */
			data->l3_cache = l3_result;
			data->l3_assoc = assoc_table[(raw->ext_cpuid[6][3] >> 12) & 0xf];
			data->l3_cacheline = (raw->ext_cpuid[6][3]) & 0xff;
		} else {
			data->l3_cache = 0;
		}
	}
}

static void decode_amd_number_of_cores(struct cpu_raw_data_t* raw, struct cpu_id_t* data)
{
	int logical_cpus = -1, num_cores = -1;
	
	if (raw->basic_cpuid[0][0] >= 1) {
		logical_cpus = (raw->basic_cpuid[1][1] >> 16) & 0xff;
		if (raw->ext_cpuid[0][0] >= 8) {
			num_cores = 1 + (raw->ext_cpuid[8][2] & 0xff);
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

static int amd_has_turion_modelname(const char *bs)
{
	/* We search for something like TL-60. Ahh, I miss regexes...*/
	int i, l, k;
	char code[3] = {0};
	const char* codes[] = { "ML", "MT", "MK", "TK", "TL", "RM", "ZM", "" };
	l = (int) strlen(bs);
	for (i = 3; i < l - 2; i++) {
		if (bs[i] == '-' &&
		    isupper(bs[i-1]) && isupper(bs[i-2]) && !isupper(bs[i-3]) &&
		    isdigit(bs[i+1]) && isdigit(bs[i+2]) && !isdigit(bs[i+3]))
		{
			code[0] = bs[i-2];
			code[1] = bs[i-1];
			for (k = 0; codes[k][0]; k++)
				if (!strcmp(codes[k], code)) return 1;
		}
	}
	return 0;
}

static amd_code_t decode_amd_codename_part1(const char *bs)
{
	int is_dual = 0, is_quad = 0, is_tri = 0;
	if (strstr(bs, "Dual Core") ||
	    strstr(bs, "Dual-Core") ||
	    strstr(bs, " X2 "))
		is_dual = 1;
	if (strstr(bs, " X4 ")) is_quad = 1;
	if (strstr(bs, " X3 ")) is_tri = 1;
	if (strstr(bs, "Opteron")) {
		return is_dual ? OPTERON_DUALCORE : OPTERON_SINGLE;
	}
	if (strstr(bs, "Phenom")) {
		if (strstr(bs, "II")) return PHENOM2;
		else return PHENOM;
	}
	if (amd_has_turion_modelname(bs)) {
		return is_dual ? TURION_X2 : TURION_64;
	}
	if (strstr(bs, "Athlon(tm) 64 FX")) return ATHLON_64_FX;
	if (strstr(bs, "Athlon(tm) FX")) return ATHLON_FX;
	if (strstr(bs, "Athlon(tm) 64") || strstr(bs, "Athlon(tm) II X") || match_pattern(bs, "Athlon(tm) X#")) {
		if (is_quad) return ATHLON_64_X4;
		if (is_dual) return ATHLON_64_X2;
		if (is_tri) return ATHLON_64_X3;
		return ATHLON_64;
	}
	if (strstr(bs, "Turion")) {
		return is_dual ? TURION_X2 : TURION_64;
	}
	
	if (strstr(bs, "mobile") || strstr(bs, "Mobile")) {
		if (strstr(bs, "Athlon(tm) XP-M (LV)")) return ATHLON_XP_M_LV;
		if (strstr(bs, "Athlon(tm) XP")) return ATHLON_XP_M;
		if (strstr(bs, "Sempron(tm)")) return M_SEMPRON;
		if (strstr(bs, "Athlon")) return MOBILE_ATHLON64;
		if (strstr(bs, "Duron")) return MOBILE_DURON;
		
	} else {
		if (strstr(bs, "Athlon(tm) XP")) return ATHLON_XP;
		if (strstr(bs, "Athlon(tm) MP")) return ATHLON_MP;
		if (strstr(bs, "Sempron(tm)")) return SEMPRON;
		if (strstr(bs, "Duron")) return DURON;
		if (strstr(bs, "Athlon")) return ATHLON;
	}
	if (match_pattern(bs, "C-##")) return FUSION_C;
	if (match_pattern(bs, "E-###")) return FUSION_E;
	if (match_pattern(bs, "Z-##")) return FUSION_Z;
	if (match_pattern(bs, "E#-####") || match_pattern(bs, "A#-####")) return FUSION_EA;
	
	return (amd_code_t) NO_CODE;
}

static void decode_amd_codename(struct cpu_raw_data_t* raw, struct cpu_id_t* data, struct internal_id_info_t* internal)
{
	amd_code_t code = decode_amd_codename_part1(data->brand_str);
	int i = 0;
	char* code_str = NULL;
	for (i = 0; i < COUNT_OF(amd_code_str); i++) {
		if (code == amd_code_str[i].code) {
			code_str = amd_code_str[i].str;
			break;
		}
	}
	if (code == ATHLON_64_X2 && data->l2_cache < 512)
		code = SEMPRON_DUALCORE;
	if (code_str)
		debugf(2, "Detected AMD brand code: %d (%s)\n", code, code_str);
	else
		debugf(2, "Detected AMD brand code: %d\n", code);
	internal->code.amd = code;
	internal->score = match_cpu_codename(cpudb_amd, COUNT_OF(cpudb_amd), data, code, 0);
}

int cpuid_identify_amd(struct cpu_raw_data_t* raw, struct cpu_id_t* data, struct internal_id_info_t* internal)
{
	load_amd_features(raw, data);
	decode_amd_cache_info(raw, data);
	decode_amd_number_of_cores(raw, data);
	decode_amd_codename(raw, data, internal);
	return 0;
}

void cpuid_get_list_amd(struct cpu_list_t* list)
{
	generic_get_cpu_list(cpudb_amd, COUNT_OF(cpudb_amd), list);
}
