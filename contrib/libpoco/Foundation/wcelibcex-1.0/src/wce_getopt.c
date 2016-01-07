/*
 * $Id$
 *
 * Copyright (c) 1987, 1993, 1994
 * The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#if 0
static char sccsid[] = "@(#)getopt.c	8.3 (Berkeley) 4/27/95";
__RCSID("$NetBSD: getopt.c,v 1.26 2003/08/07 16:43:40 agc Exp $");
#endif

#include <stdio.h>
#include <string.h>

/*
 * Declared in <unistd.h>
 */
char *optarg;       /* argument associated with option */
int opterr = 1;     /* if error message should be printed */
int optind = 1;		/* index into parent argv vector */
int optopt;         /* character checked for validity */

int optreset;       /* reset getopt */

#define	BADCH	(int)'?'
#define	BADARG	(int)':'
#define	EMSG	""

/*******************************************************************************
* wceex_getopt - function is a command-line parser
*
* Description:
*
*   The parameters argc and argv are the argument count and argument array as
*   passed to main(). The argument optstring is a string of recognised option
*   characters. If a character is followed by a colon, the option takes an argument.
*
*   The variable optind is the index of the next element of the argv[] vector to be
*   processed. It is initialised to 1 by the system, and getopt() updates it when it
*   finishes with each element of argv[].
*
*   NOTE: Implementation of the getopt() function was grabbed from NetBSD
*         operating system sources. See copyright note in the file header.
*
* Return:
*
*   Returns the next option character specified on the command line.
*   A colon (:) is returned if getopt() detects a missing argument and
*   the first character of optstring was a colon (:).
*   A question mark (?) is returned if getopt() encounters an option
*   character not in optstring or detects a missing argument and the first
*   character of optstring was not a colon (:).
*   Otherwise getopt() returns -1 when all command line options are parsed. 
* 
* Reference:
*
*   IEEE 1003.1, 2004 Edition
*
*******************************************************************************/

int wceex_getopt(int argc, char * const argv[], const char *optstring)
{
	static char *place = EMSG;		/* option letter processing */
	char *oli;				/* option letter list index */

	if (optreset || *place == 0) {		/* update scanning pointer */
		optreset = 0;
		place = argv[optind];
		if (optind >= argc || *place++ != '-') {
			/* Argument is absent or is not an option */
			place = EMSG;
			return (-1);
		}
		optopt = *place++;
		if (optopt == '-' && *place == 0) {
			/* "--" => end of options */
			++optind;
			place = EMSG;
			return (-1);
		}
		if (optopt == 0) {
			/* Solitary '-', treat as a '-' option
			   if the program (eg su) is looking for it. */
			place = EMSG;
			if (strchr(optstring, '-') == NULL)
				return -1;
			optopt = '-';
		}
	} else
		optopt = *place++;

	/* See if option letter is one the caller wanted... */
	if (optopt == ':' || (oli = strchr(optstring, optopt)) == NULL) {
		if (*place == 0)
			++optind;
		if (opterr && *optstring != ':')
			(void)fprintf(stderr,
                                      "unknown option -- %c\n", optopt);
		return (BADCH);
	}

	/* Does this option need an argument? */
	if (oli[1] != ':') {
		/* don't need argument */
		optarg = NULL;
		if (*place == 0)
			++optind;
	} else {
		/* Option-argument is either the rest of this argument or the
		   entire next argument. */
		if (*place)
			optarg = place;
		else if (argc > ++optind)
			optarg = argv[optind];
		else {
			/* option-argument absent */
			place = EMSG;
			if (*optstring == ':')
				return (BADARG);
			if (opterr)
				(void)fprintf(stderr,
                                        "option requires an argument -- %c\n",
                                        optopt);
			return (BADCH);
		}
		place = EMSG;
		++optind;
	}
	return (optopt);			/* return option letter */
}
