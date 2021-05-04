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
#include <cstring>

/////////////////////////////////////////////////////////////////////////////
// UTF-8 implementation
/////////////////////////////////////////////////////////////////////////////

#if true //USE_LITTLE_ENDIAN
struct RussianAlphabetUTF8_t
{
	enum
	{
		A	= 0xB0D0U,
		B	= 0xB1D0U,
		V	= 0xB2D0U,
		G	= 0xB3D0U,
		D	= 0xB4D0U,
		E	= 0xB5D0U,
		YO	= 0x91D1U,
		ZH	= 0xB6D0U,
		Z	= 0xB7D0U,
		I	= 0xB8D0U,
		IY	= 0xB9D0U,
		K	= 0xBAD0U,
		L	= 0xBBD0U,
		M	= 0xBCD0U,
		N	= 0xBDD0U,
		O	= 0xBED0U,
		P	= 0xBFD0U,
		R	= 0x80D1U,
		S	= 0x81D1U,
		T	= 0x82D1U,
		U	= 0x83D1U,
		F	= 0x84D1U,
		H	= 0x85D1U,
		TS	= 0x86D1U,
		CH	= 0x87D1U,
		SH	= 0x88D1U,
		SCH	= 0x89D1U,
		TVY	= 0x8AD1U, // TVYordiy znak
		Y	= 0x8BD1U,
		MYA	= 0x8CD1U, // MYAgkiy znak
		EE	= 0x8DD1U,
		YU	= 0x8ED1U,
		YA	= 0x8FD1U
	};
};
#else
struct RussianAlphabetUTF8_t
{
	enum
	{
		A	= 0xD0B0U,
		B	= 0xD0B1U,
		V	= 0xD0B2U,
		G	= 0xD0B3U,
		D	= 0xD0B4U,
		E	= 0xD0B5U,
		YO	= 0xD191U,
		ZH	= 0xD0B6U,
		Z	= 0xD0B7U,
		I	= 0xD0B8U,
		IY	= 0xD0B9U,
		K	= 0xD0BAU,
		L	= 0xD0BBU,
		M	= 0xD0BCU,
		N	= 0xD0BDU,
		O	= 0xD0BEU,
		P	= 0xD0BFU,
		R	= 0xD180U,
		S	= 0xD181U,
		T	= 0xD182U,
		U	= 0xD183U,
		F	= 0xD184U,
		H	= 0xD185U,
		TS	= 0xD186U,
		CH	= 0xD187U,
		SH	= 0xD188U,
		SCH	= 0xD189U,
		TVY	= 0xD18AU, // TVYordiy znak
		Y	= 0xD18BU,
		MYA	= 0xD18CU, // MYAgkiy znak
		EE	= 0xD18DU,
		YU	= 0xD18EU,
		YA	= 0xD18FU
	};
};
#endif

#define LOC_CHAR_TYPE		unsigned short
#define LOC_PREFIX(_a)		_a##_utf8
#define RUS					RussianAlphabetUTF8_t

#include "sphinxstemru.inl" // NOLINT 2nd include

/////////////////////////////////////////////////////////////////////////////

void stem_ru_init ()
{
	stem_ru_init_utf8 ();
}

