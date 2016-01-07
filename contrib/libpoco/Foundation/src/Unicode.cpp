//
// Unicode.cpp
//
// $Id: //poco/1.4/Foundation/src/Unicode.cpp#2 $
//
// Library: Foundation
// Package: Text
// Module:  Unicode
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Unicode.h"


extern "C"
{
#include "pcre_config.h"
GCC_DIAG_OFF(unused-function) // pcre_memmove unused function warning
#include "pcre_internal.h"
}


namespace Poco {


void Unicode::properties(int ch, CharacterProperties& props)
{
	if (ch > UCP_MAX_CODEPOINT) ch = 0;
	const ucd_record* ucd = GET_UCD(ch);
	props.category = static_cast<CharacterCategory>(_pcre_ucp_gentype[ucd->chartype]);
	props.type     = static_cast<CharacterType>(ucd->chartype);
	props.script   = static_cast<Script>(ucd->script);
}


int Unicode::toLower(int ch)
{
	if (isUpper(ch))
		return static_cast<int>(UCD_OTHERCASE(static_cast<unsigned>(ch)));
	else
		return ch;
}


int Unicode::toUpper(int ch)
{
	if (isLower(ch))
		return static_cast<int>(UCD_OTHERCASE(static_cast<unsigned>(ch)));
	else
		return ch;
}


} // namespace Poco
