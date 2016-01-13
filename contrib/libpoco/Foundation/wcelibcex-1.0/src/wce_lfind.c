/*
 * $Id: wce_lfind.c 20 2006-11-18 17:00:30Z mloskot $
 *
 * Defines lfind() function.
 *
 * Created by Mateusz Loskot (mloskot@loskot.net)
 *
 * Implementation of this function was taken from LibTIFF
 * project, http://www.remotesensing.org/libtiff/
 * The copyright note below has been copied without any changes.
 *
 * Copyright (c) 1989, 1993
 * The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Roger L. Snyder.
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

#include <assert.h>
#include <stdlib.h>
#include <wce_types.h>

/*******************************************************************************
* wceex_lfind - TODO
*
* Description:
*
* Return:
*       
* Reference:
*   IEEE 1003.1, 2004 Edition
*******************************************************************************/

void* wceex_lfind(const void *key, const void *base, size_t *nmemb, size_t size,
                 int(*compar)(const void *, const void *))
{
	char *element, *end;

    assert(key != NULL);
    assert(base != NULL);
    assert(compar != NULL);

    element = NULL;
	end = (char *)base + (*nmemb * size);

	for (element = (char *)base; element < end; element += size)
    {
		if (!compar(element, key))
        {
            /* key found */
			return element;
        }
    }

	return NULL;
}

