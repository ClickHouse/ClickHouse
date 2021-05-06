//
// Copyright (c) 2010, Ammar Ali
// Copyright (c) 2012, Sphinx Technologies Inc
//
// All rights reserved
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License. You should have
// received a copy of the GPL license along with this program; if you
// did not, you can find it at http://www.gnu.org/
//

//
// The extended ISRI Arabic stemmer
//
// Original algorithm as described by Kazem Taghva, Rania Elkoury, and
// Jeffery Coombs in "Arabic Stemming Without A Root Dictionary", 2005.
// DOI 10.1109/ITCC.2005.90
//
// Extensions (by Ammar Ali) include:
// - added stripping of kashida (tatweel) characters;
// - added support for matching recurring stem characters;
// - added "ef3ou3ala" verb form to the known patterns.
//

//#include "sphinx.h"

/// characters used in affix and word form patterns
/// FIXME! not right on big-endian
#define AR_ALEF_HA	0xA3D8U // U+0623
#define AR_ALEF		0xA7D8U // U+0627
#define AR_BA		0xA8D8U // U+0628
#define AR_TA_M		0xA9D8U	// U+0629
#define AR_TA		0xAAD8U	// U+062A
#define AR_SEEN		0xB3D8U	// U+0633
#define AR_TATWEEL	0x80D9U	// U+0640
#define AR_FA		0x81D9U	// U+0641
#define AR_KAF		0x83D9U	// U+0643
#define AR_LAM		0x84D9U	// U+0644
#define AR_MIM		0x85D9U	// U+0645
#define AR_NOON		0x86D9U	// U+0646
#define AR_HA		0x87D9U	// U+0647
#define AR_WAW		0x88D9U	// U+0648
#define AR_YA		0x8AD9U	// U+064A

/// extension; used for recurring root character matching
#define MATCH_M		0xB0DBU	// 0x06F0
#define MATCH_0		MATCH_M
#define MATCH_1		0xB1DBU	// 0x06F1
#define MATCH_2		0xB2DBU	// 0x06F2
#define MATCH_3		0xB3DBU	// 0x06F3
#define MATCH_4		0xB4DBU	// 0x06F4

//////////////////////////////////////////////////////////////////////////
// CHARACTER SET TESTS
//////////////////////////////////////////////////////////////////////////

typedef WORD ar_char;

// AR_HAMZA_SET (U+0621, U+0624, U+0626)
#define AR_HAMZA_SET	( c==0xA1D8U || c==0xA4D8U || c==0xA6D8U )

// ALEF_SET (U+0623, U+0625, U+0671)
#define AR_ALEF_SET		( c==0xA3D8U || c==0xA5D8U || c==0xB1D9U )

// AR_DIACRITIC	(>= U+064B &&<=U+0652)
#define AR_DIACRITIC(c) \
	( c==0x8BD9U || c==0x8CD9U || c==0x8DD9U \
	|| c==0x8ED9U || c==0x8FD9U || c==0x90D9U \
	|| c==0x91D9U || c==0x92D9U )

// AR_KASHIDA (U+0640)
#define AR_KASHIDA(c)	( c==0x80D9U )

// OPTIMIZE?
#define AR_WORD_LENGTH		((int)(strlen((char*)word) / sizeof(ar_char)))

// FIXME? can crash on misaligned reads
#define AR_CHAR_AT(i)		(*((ar_char*)(&word[(i * sizeof(ar_char))])))

#define AR_CHAR_SET(i,c) \
{ \
	ar_char *p = (ar_char*) &word[(i * sizeof(ar_char))]; \
	*p = (ar_char) c; \
}


/// remove length chars starting from start, null-terminating to new length
static inline void ar_remove ( BYTE * word, int start, int length )
{
	int remain = ((AR_WORD_LENGTH - length - start) * sizeof(ar_char));
	ar_char *s = (ar_char*) &word[(start * sizeof(ar_char))];
	if ( remain > 0 ) {
		memmove((void*)s, (void*)(s + length), remain);
		s = s + (remain / sizeof(ar_char));
	}
	*s = '\0';
}


/// normalize (replace) all occurrences of chars in set to to_char
#define AR_NORMALIZE(set, to_char) \
{ \
	int wlen = AR_WORD_LENGTH; \
	while ( wlen > 0 ) { \
		int ni = wlen - 1; \
		ar_char c = AR_CHAR_AT(ni); \
		if ( set ) { \
			AR_CHAR_SET ( ni, to_char ); \
		} wlen--; \
	} \
}


/// remove all occurances of chars that match the given macro (e.g. KASHIDA)
#define AR_STRIP(_what) \
{ \
	int wlen = AR_WORD_LENGTH; \
	while ( wlen > 0 ) { \
		int si = wlen - 1; \
		if ( _what ( AR_CHAR_AT(si) ) ) \
			ar_remove ( word, si, 1 ); \
		wlen--; \
	} \
}


/// attempt to match and remove a prefix with the given character count
#define AR_PREFIX(count) \
{ \
	int match = ar_match_affix ( word, prefix_##count, count, 0 ); \
	if ( match>=0 ) \
		ar_remove ( word, 0, count ); \
}


/// attempt to match and remove a suffix with the given character count
#define AR_SUFFIX(count) \
{ \
	int match = ar_match_affix ( word, suffix_##count, count, 1 ); \
	if ( match>=0 ) \
		ar_remove ( word, AR_WORD_LENGTH - count, count ); \
}

//////////////////////////////////////////////////////////////////////////
// TYPES
//////////////////////////////////////////////////////////////////////////

struct ar_affix_t
{
	ar_char	chars[4];
};

struct ar_form_entry_t
{
	int		at;	// index to match at
	ar_char	cp;	// code point to match
};

struct ar_form_t
{
	ar_form_entry_t	entry[4];
};

//////////////////////////////////////////////////////////////////////////
// PREFIX LOOKUP TABLES
//////////////////////////////////////////////////////////////////////////

/// 3-letter prefixes
static struct ar_affix_t prefix_3[] =
{
	{ { AR_WAW,		AR_LAM,		AR_LAM,		0 } },
	{ { AR_WAW,		AR_ALEF,	AR_LAM,		0 } },
	{ { AR_KAF,		AR_ALEF,	AR_LAM,		0 } },
	{ { AR_BA,		AR_ALEF,	AR_LAM,		0 } },

	// Extensions
	{ { AR_ALEF,	AR_SEEN,	AR_TA,		0 }},
	{ { AR_WAW,		AR_BA,		AR_MIM,		0 }},
	{ { AR_WAW,		AR_BA,		AR_ALEF,	0 }},
	{ { 0 } }
};

/// 2-letter prefixes
static struct ar_affix_t prefix_2[] =
{
	{ { AR_ALEF,	AR_LAM,	0 } },
	{ { AR_LAM,		AR_LAM,	0 } },
	{ { 0 } }
};

/// 1-letter prefixes
static struct ar_affix_t prefix_1[] =
{
	{ { AR_ALEF,	0 } },
	{ { AR_BA,		0 } },
	{ { AR_TA,		0 } },
	{ { AR_SEEN,	0 } },
	{ { AR_FA,		0 } },
	{ { AR_LAM,		0 } },
	{ { AR_NOON,	0 } },
	{ { AR_WAW,		0 } },
	{ { AR_YA,		0 } },
	{ { 0 } }
};

//////////////////////////////////////////////////////////////////////////
// SUFFIX LOOKUP TABLES
//////////////////////////////////////////////////////////////////////////

/// 3-letter suffixes
static struct ar_affix_t suffix_3[] =
{
	{ { AR_TA,		AR_MIM,		AR_LAM,		0 } },
	{ { AR_HA,		AR_MIM,		AR_LAM,		0 } },
	{ { AR_TA,		AR_ALEF,	AR_NOON,	0 } },
	{ { AR_TA,		AR_YA,		AR_NOON,	0 } },
	{ { AR_KAF,		AR_MIM,		AR_LAM,		0 }	},
	{ { 0 } }
};

/// 2-letter suffixes
static struct ar_affix_t suffix_2[] = {
	{ { AR_WAW,		AR_NOON,	0 } },
	{ { AR_ALEF,	AR_TA,		0 } },
	{ { AR_ALEF,	AR_NOON,	0 } },
	{ { AR_YA,		AR_NOON,	0 } },
	{ { AR_TA,		AR_NOON,	0 } },
	{ { AR_KAF,		AR_MIM,		0 } },
	{ { AR_HA,		AR_NOON,	0 } },
	{ { AR_NOON,	AR_ALEF,	0 } },
	{ { AR_YA,		AR_ALEF,	0 } },
	{ { AR_HA,		AR_ALEF,	0 } },
	{ { AR_TA,		AR_MIM,		0 } },
	{ { AR_KAF,		AR_NOON,	0 } },
	{ { AR_NOON,	AR_YA,		0 } },
	{ { AR_WAW,		AR_ALEF,	0 } },
	{ { AR_MIM,		AR_ALEF,	0 } },
	{ { AR_HA,		AR_MIM,		0 } },

	// extensions
	{ { AR_WAW,		AR_HA,		0 }},
	{ { 0 } }
};

/// 1-letter prefixes
static struct ar_affix_t suffix_1[] =
{
	{ { AR_ALEF,	0 } },
	{ { AR_TA_M,	0 } },
	{ { AR_TA,		0 } },
	{ { AR_KAF,		0 } },
	{ { AR_NOON,	0 } },
	{ { AR_HA,		0 } },
	{ { AR_YA,		0 } },
	{ { 0 } }
};

//////////////////////////////////////////////////////////////////////////
// FORMS
//////////////////////////////////////////////////////////////////////////

/// forms for 4 letter words that yield a 3 letter stem
static struct ar_form_t form_4_3[] =
{
	{ { { 3, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 1, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 0xFF, 0 } } }, // originally at index 05
	{ { { 2, AR_WAW },	{ 0xFF, 0 } } },
	{ { { 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 2, AR_YA },	{ 0xFF, 0 } } },
	{ { { 0xFF,	0 } } }
};

/// forms for 5 letter words that yield a 3 letter stem
static struct ar_form_t form_5_3[] =
{
	{ { { 0, AR_TA },	{ 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 2, AR_TA },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 3, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 2, AR_ALEF },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 3, AR_ALEF },	{ 4, AR_NOON },	{ 0xFF, 0 } } },
	{ { { 2, AR_WAW },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 0, AR_TA },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 0, AR_TA },	{ 3, AR_YA },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 3, AR_WAW },	{ 0xFF, 0 } } },
	{ { { 1, AR_ALEF },	{ 3, AR_WAW },	{ 0xFF, 0 } } },
	{ { { 1, AR_WAW },	{ 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 3, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 3, AR_YA },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 2, AR_ALEF },	{ 3, AR_NOON },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 1, AR_NOON },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 2, AR_TA },	{ 0xFF, 0 } } },
	{ { { 1, AR_ALEF },	{ 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 0, AR_YA },	{ 2, AR_TA },	{ 0xFF, 0 } } },
	{ { { 0, AR_TA },	{ 2, AR_TA },	{ 0xFF, 0 } } },
	{ { { 2, AR_ALEF },	{ 4, AR_YA },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 1, AR_NOON },	{ 0xFF, 0 } } },

	// extensions
	{ { { 1, AR_TA },	{ 4, AR_WAW },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 1, AR_TA },	{ 0xFF, 0 } } },
	{ { { 0, AR_TA },	{ 4, AR_TA },	{ 0xFF, 0 } } },
	{ { { 1, AR_ALEF },	{ 3, AR_YA },	{ 0xFF, 0 } } },
	{ { { 0xFF,	0 } } }
};

/// forms for 5 letter words that yield a 4 letter stem
static struct ar_form_t form_5_4[] =
{
	{ { { 0, AR_TA },	{ 0xFF, 0 } } },
	{ { { 0, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 0xFF, 0 } } },
	{ { { 4, AR_TA_M },	{ 0xFF, 0 } } },
	{ { { 2, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0xFF,	0} }}
};

/// forms for 6 letter words that yield a 3 letter stem
static struct ar_form_t form_6_3[] =
{
	{ { { 0, AR_ALEF},	{ 1, AR_SEEN },	{ 2, AR_TA },	{ 0xFF, 0 } }},
	{ { { 0, AR_MIM},	{ 3, AR_ALEF },	{ 5, AR_TA_M },	{ 0xFF, 0 } }},
	{ { { 0, AR_ALEF},	{ 2, AR_TA },	{ 4, AR_ALEF },	{ 0xFF, 0 } }},

	// extensions that match recurring 2nd root letter
	{ { { 0, AR_ALEF },	{ 3, AR_WAW },	{ 4, MATCH_2 },	{ 0xFF, 0 } }},
	{ { { 0, AR_MIM },	{ 1, AR_SEEN },	{ 2, AR_TA },	{ 0xFF, 0 } }},

	// extensions
	{ { { 0, AR_MIM },	{ 2, AR_ALEF},	{ 4, AR_YA },	{ 0xFF, 0 } }},
	{ { { 0xFF,	0 } }}
};

/// forms for 6 letter words that yield a 4 letter stem
static struct ar_form_t form_6_4[] =
{
	{ { { 0, AR_ALEF },	{ 4, AR_ALEF },	{ 0xFF, 0 } } },
	{ { { 0, AR_MIM },	{ 1, AR_TA },	{ 0xFF, 0 } } },
	{ { { 0xFF,	0} }}
};


/// attempt to match the given word against one of the given affix rules
static int ar_match_affix ( BYTE * word, struct ar_affix_t * affixes, int length, int reverse )
{
	int match = -1, ai = 0;
	while ( affixes[ai].chars[0] && match<0 )
	{
		int ci = 0;
		while ( affixes[ai].chars[ci] && match<0 )
		{
			int wi = ci;
			if ( reverse )
				wi = (AR_WORD_LENGTH - length) + ci;

			if ( AR_CHAR_AT(wi)!=affixes[ai].chars[ci] )
				break;

			ci++;
			if ( affixes[ai].chars[ci]==0 )
				match = ai;
		}
		ai++;
	}
	return match;
}


/// attempt to match the given word against one of the given form
/// rules, and if found, extract the stem
static int ar_match_form ( BYTE * word, struct ar_form_t * forms )
{
	int match = -1, fi = 0;
	while ( forms[fi].entry[0].at!=0xFF && match < 0 )
	{
		int pi = 0;
		while ( forms[fi].entry[pi].at!=0xFF && match<0 )
		{
			if ( forms[fi].entry[pi].cp>=MATCH_M && forms[fi].entry[pi].cp<=MATCH_4 )
			{
				int index = ( forms[fi].entry[pi].cp - MATCH_M ) >> 8;
				if ( AR_CHAR_AT(index)!=AR_CHAR_AT(forms[fi].entry[pi].at) )
					break;
			} else
			{
				if ( forms[fi].entry[pi].cp!=AR_CHAR_AT ( forms[fi].entry[pi].at ) )
					break;
			}

			pi++;
			if ( forms[fi].entry[pi].at==0xFF )
				match = fi;
		}
		fi++;
	}

	// if match found, extract the stem
	if ( match>=0 )
	{
		int pi = 0;
		while ( forms[match].entry[pi].at!=0xFF )
		{
			ar_remove ( word, (forms[match].entry[pi].at - pi), 1 );
			pi++;
		}
	}

	return match;
}


static void ar_word_4 ( BYTE * word )
{
	if ( ar_match_form ( word, form_4_3 )>=0 )
		return;
	AR_SUFFIX(1);
	if ( AR_WORD_LENGTH==4 )
		AR_PREFIX(1);
}


static void ar_word_5 ( BYTE * word )
{
	if ( ar_match_form ( word, form_5_3 )>=0 )
		return;
	AR_SUFFIX(1);
	if ( AR_WORD_LENGTH==4 )
	{
		ar_word_4(word);
		return;
	}
	AR_PREFIX(1);
	if ( AR_WORD_LENGTH==4 )
		ar_word_4(word);
	else if ( AR_WORD_LENGTH==5 )
		ar_match_form ( word, form_5_4 );
}


static void ar_word_6 ( BYTE * word )
{
	if ( ar_match_form ( word, form_6_3 )>=0 )
		return;
	AR_SUFFIX(1);
	if ( AR_WORD_LENGTH==5 )
	{
		ar_word_5(word);
		return;
	}
	AR_PREFIX(1);
	if ( AR_WORD_LENGTH==5 )
		ar_word_5(word);
	else if ( AR_WORD_LENGTH==6 )
		ar_match_form ( word, form_6_4 );
}

void stem_ar_init() {
}

void stem_ar_utf8 ( BYTE * word, int )
{
	AR_STRIP ( AR_DIACRITIC );
	AR_STRIP ( AR_KASHIDA ); // extension

	AR_NORMALIZE ( AR_HAMZA_SET, AR_ALEF_HA );

#ifdef AR_STEM_AGGRESSIVE
		// extension; does both if possible (i.e. 6 and 5, not 6 else 5)
		if ( AR_WORD_LENGTH>=6 )
			AR_PREFIX(3);
		if ( AR_WORD_LENGTH>=5 )
			AR_PREFIX(2);

		if ( AR_WORD_LENGTH>=6 )
			AR_SUFFIX(3);
		if ( AR_WORD_LENGTH>=5 )
			AR_SUFFIX(2);
#else
		// original; does one only (i.e. 6 or 5, not 6 and 5)
		if ( AR_WORD_LENGTH>=6 )
			AR_PREFIX(3)
		else if ( AR_WORD_LENGTH>=5 )
			AR_PREFIX(2);

		if ( AR_WORD_LENGTH>=6 )
			AR_SUFFIX(3)
		else if ( AR_WORD_LENGTH>=5 )
			AR_SUFFIX(2);
#endif

	AR_NORMALIZE ( AR_ALEF_SET, AR_ALEF );

	switch ( AR_WORD_LENGTH )
	{
		case 4:	ar_word_4 ( word ); return;
		case 5:	ar_word_5 ( word ); return;
		case 6:	ar_word_6 ( word ); return;
		case 7:
			AR_SUFFIX(1);
			if ( AR_WORD_LENGTH==6 )
			{
				ar_word_6(word);
				return;
			}
			AR_PREFIX(1);
			if ( AR_WORD_LENGTH==6 )
				ar_word_6(word);
			return;
	}
}

