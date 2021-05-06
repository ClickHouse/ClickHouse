//
// Copyright (c) 2001-2016, Andrew Aksyonoff
// Copyright (c) 2008-2016, Sphinx Technologies Inc
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

//#include "sphinx.h"
#include <string.h>


struct ClampRule_t
{
	int		m_iMinLength;
	BYTE	m_szSuffix[10];
	int		m_iCheckLength;
	int		m_nRemove;
	bool	m_bPalatalize;
};


static ClampRule_t g_dCaseRules [] =
{
	{ 7,	"atech",	5,	5,	false	},
	{ 6,	"\xECtem",	4,	3,	true	},		// \u011b
	{ 6,	"at\xF9m",	4,	4,	false	},		// \u016f
	{ 5,	"ech",		3,	2,	true	},
	{ 5,	"ich",		3,	2,	true	},
	{ 5,	"\xED!ch",	3,	2,	true	},		// \u00ed
	{ 5,	"\xE9ho",	3,	2,	true	},		// \u00e9
	{ 5,	"\xECmi",	3,	2,	true	},		// \u011b
	{ 5,	"emi",		3,	2,	true	},
	{ 5,	"\xE9mu",	3,	2,	true	},		// \u00e9
	{ 5,	"\xECte",	3,	2,	true	},		// \u011b
	{ 5,	"\xECti",	3,	2,	true	},		// \u011b
	{ 5,	"iho",		3,	2,	true	},
	{ 5,	"\xEDho",	3,	2,	true	},		// \u00ed
	{ 5,	"\xEDmi",	3,	2,	true	},		// \u00ed
	{ 5,	"imu",		3,	2,	true	},
	{ 5,	"\xE1!ch",	3,	3,	false	},		// \u00e1
	{ 5,	"ata",		3,	3,	false	},
	{ 5,	"aty",		3,	3,	false	},
	{ 5,	"\xFD!ch",	3,	3,	false	},		// \u00fd
	{ 5,	"ama",		3,	3,	false	},
	{ 5,	"ami",		3,	3,	false	},
	{ 5,	"ov\xE9",	3,	3,	false	},		// \u00e9
	{ 5,	"ovi",		3,	3,	false	},
	{ 5,	"\xFDmi",	3,	3,	false	},		// \u00fd
	{ 4,	"em",		2,	1,	true	},
	{ 4,	"es",		2,	2,	true	},
	{ 4,	"\xE9m",	2,	2,	true	},		// \u00e9
	{ 4,	"\xEDm",	2,	2,	true	},		// \u00ed
	{ 4,	"\xF9!fm",	2,	2,	false	},		// \u016f
	{ 4,	"at",		2,	2,	false	},
	{ 4,	"\xE1m",	2,	2,	false	},		// \u00e1
	{ 4,	"os",		2,	2,	false	},
	{ 4,	"us",		2,	2,	false	},
	{ 4,	"\xFDm",	2,	2,	false	},		// \u00fd
	{ 4,	"mi",		2,	2,	false	},
	{ 4,	"ou",		2,	2,	false	},
	{ 3,	"e",		1,	0,	true	},
	{ 3,	"i",		1,	0,	true	},
	{ 3,	"\xED",		1,	0,	true	},		// \u00ed
	{ 3,	"\xEC",		1,	0,	true	},		// \u011b
	{ 3,	"u",		1,	1,	false	},
	{ 3,	"y",		1,	1,	false	},
	{ 3,	"\xF9",		1,	1,	false	},		// \u016f
	{ 3,	"a",		1,	1,	false	},
	{ 3,	"o",		1,	1,	false	},
	{ 3,	"\xE1",		1,	1,	false	},		// \u00e1
	{ 3,	"\xE9",		1,	1,	false	},		// \u00e9
	{ 3,	"\xFD",		1,	1,	false	}		// \u00fd
};


static ClampRule_t g_dPosessiveRules [] =
{
	{ 5,	"ov",		2,	2,	false	},
	{ 5,	"\xF9v",	2,	2,	false	},
	{ 5,	"in",		2,	1,	true	},
};


struct ReplaceRule_t
{
	BYTE	m_szSuffix[4];
	int		m_iRemoveLength;
	BYTE	m_szAppend[4];
};


static ReplaceRule_t g_dPalatalizeRules [] =
{
	{ "ci",			2,	"k"		},
	{ "ce",			2,	"k"		},
	{ "\xE8i",		2,	"k"		},	// \u010d
	{ "\xE8!e",		2,	"k"		},	// \u010d
	{ "zi",			2,	"h"		},
	{ "ze",			2,	"h"		},
	{ "\x9Ei",		2,	"h"		},	// \u017e
	{ "\x9E!e",		2,	"h"		},	// \u017e
	{ "\xE8t\xEC",	3,	"ck"	},	// \u010d \u011b
	{ "\xE8ti",		3,	"ck"	},
	{ "\xE8t\xED",	3,	"ck"	},	// \u010d \u00ed
	{ "\x9At\xEC",	3,	"sk"	},	// \u0161 \u011b	// was: check 2, remove 2
	{ "\x9Ati",		3,	"sk"	},	// \u0161			// was: check 2, remove 2
	{ "\x9At\xED",	3,	"sk"	},	// \u0161 \u00ed	// was: check 2, remove 2
};


static void Palatalize ( BYTE * word )
{
	if ( !word )
		return;

	int nRules = sizeof ( g_dPalatalizeRules ) / sizeof ( g_dPalatalizeRules[0] );
	auto iWordLength = (int) strlen ( (char*)word );

	for ( int i = 0; i < nRules; ++i )
	{
		const ReplaceRule_t & Rule = g_dPalatalizeRules[i];
		if ( iWordLength>=Rule.m_iRemoveLength &&
!strncmp ( (char*)word + iWordLength - Rule.m_iRemoveLength, (char*)Rule.m_szSuffix, Rule.m_iRemoveLength ) )
		{
			word [iWordLength - Rule.m_iRemoveLength] = '\0';
			strcat ( (char*)word, (char*)Rule.m_szAppend ); // NOLINT strcat
			return;
		}
	}

	if ( iWordLength > 0 )
		word [iWordLength - 1] = '\0';
}


static void ApplyRules ( BYTE * word, const ClampRule_t * pRules, int nRules )
{
	if ( !word || !pRules )
		return;

	auto iWordLength = (int) strlen ( (char *)word );

	for ( int i = 0; i < nRules; ++i )
	{
		const ClampRule_t & Rule = pRules[i];
		if ( iWordLength > Rule.m_iMinLength &&
			!strncmp ( (char*)word + iWordLength - Rule.m_iCheckLength, (char*)Rule.m_szSuffix, Rule.m_iCheckLength ) )
		{
			word [iWordLength - Rule.m_nRemove] = '\0';
			Palatalize ( word );
			return;
		}
	}
}

static void RemoveChars ( char * szString, char cChar )
{
	char * szPos;
	auto iLength = (int) strlen ( szString );
	while ( ( szPos = strchr ( szString, cChar ) )!=NULL )
		memmove ( szPos, szPos + 1, iLength - ( szPos - szString ) );
}


static void PreprocessRules ( ClampRule_t * pRules, int nRules )
{
	if ( !pRules )
		return;

	for ( int i = 0; i < nRules; ++i )
		RemoveChars ( (char *) pRules[i].m_szSuffix, '!' );
}

static void PreprocessReplace ()
{
	int nRules = sizeof ( g_dPalatalizeRules ) / sizeof ( g_dPalatalizeRules[0] );

	for ( int i = 0; i < nRules; ++i )
	{
		RemoveChars ( (char *) g_dPalatalizeRules[i].m_szSuffix, '!' );
		RemoveChars ( (char *) g_dPalatalizeRules[i].m_szAppend, '!' );
	}
}


void stem_cz_init ()
{
	PreprocessRules ( g_dCaseRules, sizeof ( g_dCaseRules ) / sizeof ( g_dCaseRules[0] ) );
	PreprocessRules ( g_dPosessiveRules, sizeof ( g_dPosessiveRules ) / sizeof ( g_dPosessiveRules[0] ) );
	PreprocessReplace ();
}


void stem_cz ( BYTE * word, int )
{
	ApplyRules ( word, g_dCaseRules, sizeof ( g_dCaseRules ) / sizeof ( g_dCaseRules[0] ) );
	ApplyRules ( word, g_dPosessiveRules, sizeof ( g_dPosessiveRules ) / sizeof ( g_dPosessiveRules[0] ) );
}


