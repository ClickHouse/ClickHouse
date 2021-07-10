/* intl.h - internationalization
   Copyright (C) 1998-2018 Free Software Foundation, Inc.

   GCC is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */

#ifndef GCC_INTL_H
#define GCC_INTL_H

#ifdef HAVE_LOCALE_H
# include <locale.h>
#endif

#ifndef HAVE_SETLOCALE
# define setlocale(category, locale) (locale)
#endif

#ifdef ENABLE_NLS
#include <libintl.h>
extern void gcc_init_libintl (void);
extern size_t gcc_gettext_width (const char *);
#else
/* Stubs.  */
# undef textdomain
# define textdomain(domain) (domain)
# undef bindtextdomain
# define bindtextdomain(domain, directory) (domain)
# undef gettext
# define gettext(msgid) (msgid)
# define ngettext(singular,plural,n) fake_ngettext (singular, plural, n)
# define gcc_init_libintl()	/* nothing */
# define gcc_gettext_width(s) strlen (s)

extern const char *fake_ngettext (const char *singular, const char *plural,
				  unsigned long int n);

#endif

#ifndef _
# define _(msgid) gettext (msgid)
#endif

#ifndef N_
# define N_(msgid) msgid
#endif

#ifndef G_
# define G_(gmsgid) gmsgid
#endif

extern char *get_spaces (const char *);

extern const char *open_quote;
extern const char *close_quote;
extern const char *locale_encoding;
extern bool locale_utf8;

#endif /* intl.h */
