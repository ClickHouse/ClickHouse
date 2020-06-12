/* $OpenBSD: ui_lib.c,v 1.32 2017/01/29 17:49:23 beck Exp $ */
/* Written by Richard Levitte (richard@levitte.org) for the OpenSSL
 * project 2001.
 */
/* ====================================================================
 * Copyright (c) 2001 The OpenSSL Project.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit. (http://www.openssl.org/)"
 *
 * 4. The names "OpenSSL Toolkit" and "OpenSSL Project" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    openssl-core@openssl.org.
 *
 * 5. Products derived from this software may not be called "OpenSSL"
 *    nor may "OpenSSL" appear in their names without prior written
 *    permission of the OpenSSL Project.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the OpenSSL Project
 *    for use in the OpenSSL Toolkit (http://www.openssl.org/)"
 *
 * THIS SOFTWARE IS PROVIDED BY THE OpenSSL PROJECT ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE OpenSSL PROJECT OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 *
 * This product includes cryptographic software written by Eric Young
 * (eay@cryptsoft.com).  This product includes software written by Tim
 * Hudson (tjh@cryptsoft.com).
 *
 */

#include <string.h>

#include <openssl/opensslconf.h>

#include <openssl/buffer.h>
#include <openssl/err.h>
#include <openssl/ui.h>

#include "ui_locl.h"

static const UI_METHOD *default_UI_meth = NULL;

UI *
UI_new(void)
{
	return (UI_new_method(NULL));
}

UI *
UI_new_method(const UI_METHOD *method)
{
	UI *ret;

	ret = malloc(sizeof(UI));
	if (ret == NULL) {
		UIerror(ERR_R_MALLOC_FAILURE);
		return NULL;
	}
	if (method == NULL)
		ret->meth = UI_get_default_method();
	else
		ret->meth = method;

	ret->strings = NULL;
	ret->user_data = NULL;
	ret->flags = 0;
	CRYPTO_new_ex_data(CRYPTO_EX_INDEX_UI, ret, &ret->ex_data);
	return ret;
}

static void
free_string(UI_STRING *uis)
{
	if (uis->flags & OUT_STRING_FREEABLE) {
		free((char *) uis->out_string);
		switch (uis->type) {
		case UIT_BOOLEAN:
			free((char *)uis->_.boolean_data.action_desc);
			free((char *)uis->_.boolean_data.ok_chars);
			free((char *)uis->_.boolean_data.cancel_chars);
			break;
		default:
			break;
		}
	}
	free(uis);
}

void
UI_free(UI *ui)
{
	if (ui == NULL)
		return;
	sk_UI_STRING_pop_free(ui->strings, free_string);
	CRYPTO_free_ex_data(CRYPTO_EX_INDEX_UI, ui, &ui->ex_data);
	free(ui);
}

static int
allocate_string_stack(UI *ui)
{
	if (ui->strings == NULL) {
		ui->strings = sk_UI_STRING_new_null();
		if (ui->strings == NULL) {
			return -1;
		}
	}
	return 0;
}

static UI_STRING *
general_allocate_prompt(UI *ui, const char *prompt, int prompt_freeable,
    enum UI_string_types type, int input_flags, char *result_buf)
{
	UI_STRING *ret = NULL;

	if (prompt == NULL) {
		UIerror(ERR_R_PASSED_NULL_PARAMETER);
	} else if ((type == UIT_PROMPT || type == UIT_VERIFY ||
	    type == UIT_BOOLEAN) && result_buf == NULL) {
		UIerror(UI_R_NO_RESULT_BUFFER);
	} else if ((ret = malloc(sizeof(UI_STRING)))) {
		ret->out_string = prompt;
		ret->flags = prompt_freeable ? OUT_STRING_FREEABLE : 0;
		ret->input_flags = input_flags;
		ret->type = type;
		ret->result_buf = result_buf;
	}
	return ret;
}

static int
general_allocate_string(UI *ui, const char *prompt, int prompt_freeable,
    enum UI_string_types type, int input_flags, char *result_buf, int minsize,
    int maxsize, const char *test_buf)
{
	int ret = -1;
	UI_STRING *s = general_allocate_prompt(ui, prompt, prompt_freeable,
	    type, input_flags, result_buf);

	if (s) {
		if (allocate_string_stack(ui) >= 0) {
			s->_.string_data.result_minsize = minsize;
			s->_.string_data.result_maxsize = maxsize;
			s->_.string_data.test_buf = test_buf;
			ret = sk_UI_STRING_push(ui->strings, s);
			/* sk_push() returns 0 on error.  Let's adapt that */
			if (ret <= 0)
				ret--;
		} else
			free_string(s);
	}
	return ret;
}

static int
general_allocate_boolean(UI *ui, const char *prompt, const char *action_desc,
    const char *ok_chars, const char *cancel_chars, int prompt_freeable,
    enum UI_string_types type, int input_flags, char *result_buf)
{
	int ret = -1;
	UI_STRING *s;
	const char *p;

	if (ok_chars == NULL) {
		UIerror(ERR_R_PASSED_NULL_PARAMETER);
	} else if (cancel_chars == NULL) {
		UIerror(ERR_R_PASSED_NULL_PARAMETER);
	} else {
		for (p = ok_chars; *p; p++) {
			if (strchr(cancel_chars, *p)) {
				UIerror(UI_R_COMMON_OK_AND_CANCEL_CHARACTERS);
			}
		}

		s = general_allocate_prompt(ui, prompt, prompt_freeable,
		    type, input_flags, result_buf);

		if (s) {
			if (allocate_string_stack(ui) >= 0) {
				s->_.boolean_data.action_desc = action_desc;
				s->_.boolean_data.ok_chars = ok_chars;
				s->_.boolean_data.cancel_chars = cancel_chars;
				ret = sk_UI_STRING_push(ui->strings, s);
				/*
				 * sk_push() returns 0 on error. Let's adapt
				 * that
				 */
				if (ret <= 0)
					ret--;
			} else
				free_string(s);
		}
	}
	return ret;
}

/* Returns the index to the place in the stack or -1 for error.  Uses a
   direct reference to the prompt.  */
int
UI_add_input_string(UI *ui, const char *prompt, int flags, char *result_buf,
    int minsize, int maxsize)
{
	return general_allocate_string(ui, prompt, 0, UIT_PROMPT, flags,
	    result_buf, minsize, maxsize, NULL);
}

/* Same as UI_add_input_string(), excepts it takes a copy of the prompt */
int
UI_dup_input_string(UI *ui, const char *prompt, int flags, char *result_buf,
    int minsize, int maxsize)
{
	char *prompt_copy = NULL;

	if (prompt) {
		prompt_copy = strdup(prompt);
		if (prompt_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			return 0;
		}
	}
	return general_allocate_string(ui, prompt_copy, 1, UIT_PROMPT, flags,
	    result_buf, minsize, maxsize, NULL);
}

int
UI_add_verify_string(UI *ui, const char *prompt, int flags, char *result_buf,
    int minsize, int maxsize, const char *test_buf)
{
	return general_allocate_string(ui, prompt, 0, UIT_VERIFY, flags,
	    result_buf, minsize, maxsize, test_buf);
}

int
UI_dup_verify_string(UI *ui, const char *prompt, int flags,
    char *result_buf, int minsize, int maxsize, const char *test_buf)
{
	char *prompt_copy = NULL;

	if (prompt) {
		prompt_copy = strdup(prompt);
		if (prompt_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			return -1;
		}
	}
	return general_allocate_string(ui, prompt_copy, 1, UIT_VERIFY, flags,
	    result_buf, minsize, maxsize, test_buf);
}

int
UI_add_input_boolean(UI *ui, const char *prompt, const char *action_desc,
    const char *ok_chars, const char *cancel_chars, int flags, char *result_buf)
{
	return general_allocate_boolean(ui, prompt, action_desc, ok_chars,
	    cancel_chars, 0, UIT_BOOLEAN, flags, result_buf);
}

int
UI_dup_input_boolean(UI *ui, const char *prompt, const char *action_desc,
    const char *ok_chars, const char *cancel_chars, int flags, char *result_buf)
{
	char *prompt_copy = NULL;
	char *action_desc_copy = NULL;
	char *ok_chars_copy = NULL;
	char *cancel_chars_copy = NULL;

	if (prompt) {
		prompt_copy = strdup(prompt);
		if (prompt_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}
	if (action_desc) {
		action_desc_copy = strdup(action_desc);
		if (action_desc_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}
	if (ok_chars) {
		ok_chars_copy = strdup(ok_chars);
		if (ok_chars_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}
	if (cancel_chars) {
		cancel_chars_copy = strdup(cancel_chars);
		if (cancel_chars_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			goto err;
		}
	}
	return general_allocate_boolean(ui, prompt_copy, action_desc_copy,
	    ok_chars_copy, cancel_chars_copy, 1, UIT_BOOLEAN, flags,
	    result_buf);

err:
	free(prompt_copy);
	free(action_desc_copy);
	free(ok_chars_copy);
	free(cancel_chars_copy);
	return -1;
}

int
UI_add_info_string(UI *ui, const char *text)
{
	return general_allocate_string(ui, text, 0, UIT_INFO, 0, NULL, 0, 0,
	    NULL);
}

int
UI_dup_info_string(UI *ui, const char *text)
{
	char *text_copy = NULL;

	if (text) {
		text_copy = strdup(text);
		if (text_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			return -1;
		}
	}
	return general_allocate_string(ui, text_copy, 1, UIT_INFO, 0, NULL,
	    0, 0, NULL);
}

int
UI_add_error_string(UI *ui, const char *text)
{
	return general_allocate_string(ui, text, 0, UIT_ERROR, 0, NULL, 0, 0,
	    NULL);
}

int
UI_dup_error_string(UI *ui, const char *text)
{
	char *text_copy = NULL;

	if (text) {
		text_copy = strdup(text);
		if (text_copy == NULL) {
			UIerror(ERR_R_MALLOC_FAILURE);
			return -1;
		}
	}
	return general_allocate_string(ui, text_copy, 1, UIT_ERROR, 0, NULL,
	    0, 0, NULL);
}

char *
UI_construct_prompt(UI *ui, const char *object_desc, const char *object_name)
{
	char *prompt;

	if (ui->meth->ui_construct_prompt)
		return ui->meth->ui_construct_prompt(ui, object_desc,
		    object_name);

	if (object_desc == NULL)
		return NULL;

	if (object_name == NULL) {
		if (asprintf(&prompt, "Enter %s:", object_desc) == -1)
			return (NULL);
	} else {
		if (asprintf(&prompt, "Enter %s for %s:", object_desc,
		    object_name) == -1)
			return (NULL);
	}

	return prompt;
}

void *
UI_add_user_data(UI *ui, void *user_data)
{
	void *old_data = ui->user_data;

	ui->user_data = user_data;
	return old_data;
}

void *
UI_get0_user_data(UI *ui)
{
	return ui->user_data;
}

const char *
UI_get0_result(UI *ui, int i)
{
	if (i < 0) {
		UIerror(UI_R_INDEX_TOO_SMALL);
		return NULL;
	}
	if (i >= sk_UI_STRING_num(ui->strings)) {
		UIerror(UI_R_INDEX_TOO_LARGE);
		return NULL;
	}
	return UI_get0_result_string(sk_UI_STRING_value(ui->strings, i));
}

static int
print_error(const char *str, size_t len, UI *ui)
{
	UI_STRING uis;

	memset(&uis, 0, sizeof(uis));
	uis.type = UIT_ERROR;
	uis.out_string = str;

	if (ui->meth->ui_write_string &&
	    !ui->meth->ui_write_string(ui, &uis))
		return -1;
	return 0;
}

int
UI_process(UI *ui)
{
	int i, ok = 0;

	if (ui->meth->ui_open_session && !ui->meth->ui_open_session(ui))
		return -1;

	if (ui->flags & UI_FLAG_PRINT_ERRORS)
		ERR_print_errors_cb(
		    (int (*)(const char *, size_t, void *)) print_error,
		    (void *)ui);

	for (i = 0; i < sk_UI_STRING_num(ui->strings); i++) {
		if (ui->meth->ui_write_string &&
		    !ui->meth->ui_write_string(ui,
			sk_UI_STRING_value(ui->strings, i))) {
			ok = -1;
			goto err;
		}
	}

	if (ui->meth->ui_flush)
		switch (ui->meth->ui_flush(ui)) {
		case -1:	/* Interrupt/Cancel/something... */
			ok = -2;
			goto err;
		case 0:		/* Errors */
			ok = -1;
			goto err;
		default:	/* Success */
			ok = 0;
			break;
		}

	for (i = 0; i < sk_UI_STRING_num(ui->strings); i++) {
		if (ui->meth->ui_read_string) {
			switch (ui->meth->ui_read_string(ui,
			    sk_UI_STRING_value(ui->strings, i))) {
			case -1:	/* Interrupt/Cancel/something... */
				ui->flags &= ~UI_FLAG_REDOABLE;
				ok = -2;
				goto err;
			case 0:		/* Errors */
				ok = -1;
				goto err;
			default:	/* Success */
				ok = 0;
				break;
			}
		}
	}

err:
	if (ui->meth->ui_close_session && !ui->meth->ui_close_session(ui))
		return -1;
	return ok;
}

int
UI_ctrl(UI *ui, int cmd, long i, void *p, void (*f) (void))
{
	if (ui == NULL) {
		UIerror(ERR_R_PASSED_NULL_PARAMETER);
		return -1;
	}
	switch (cmd) {
	case UI_CTRL_PRINT_ERRORS:
		{
			int save_flag = !!(ui->flags & UI_FLAG_PRINT_ERRORS);
			if (i)
				ui->flags |= UI_FLAG_PRINT_ERRORS;
			else
				ui->flags &= ~UI_FLAG_PRINT_ERRORS;
			return save_flag;
		}
	case UI_CTRL_IS_REDOABLE:
		return !!(ui->flags & UI_FLAG_REDOABLE);
	default:
		break;
	}
	UIerror(UI_R_UNKNOWN_CONTROL_COMMAND);
	return -1;
}

int
UI_get_ex_new_index(long argl, void *argp, CRYPTO_EX_new *new_func,
    CRYPTO_EX_dup *dup_func, CRYPTO_EX_free *free_func)
{
	return CRYPTO_get_ex_new_index(CRYPTO_EX_INDEX_UI, argl, argp,
	    new_func, dup_func, free_func);
}

int
UI_set_ex_data(UI *r, int idx, void *arg)
{
	return (CRYPTO_set_ex_data(&r->ex_data, idx, arg));
}

void *
UI_get_ex_data(UI *r, int idx)
{
	return (CRYPTO_get_ex_data(&r->ex_data, idx));
}

void
UI_set_default_method(const UI_METHOD *meth)
{
	default_UI_meth = meth;
}

const UI_METHOD *
UI_get_default_method(void)
{
	if (default_UI_meth == NULL) {
		default_UI_meth = UI_OpenSSL();
	}
	return default_UI_meth;
}

const UI_METHOD *
UI_get_method(UI *ui)
{
	return ui->meth;
}

const UI_METHOD *
UI_set_method(UI *ui, const UI_METHOD *meth)
{
	ui->meth = meth;
	return ui->meth;
}


UI_METHOD *
UI_create_method(char *name)
{
	UI_METHOD *ui_method = calloc(1, sizeof(UI_METHOD));

	if (ui_method && name)
		ui_method->name = strdup(name);

	return ui_method;
}

/* BIG FSCKING WARNING!!!!  If you use this on a statically allocated method
   (that is, it hasn't been allocated using UI_create_method(), you deserve
   anything Murphy can throw at you and more!  You have been warned. */
void
UI_destroy_method(UI_METHOD *ui_method)
{
	free(ui_method->name);
	ui_method->name = NULL;
	free(ui_method);
}

int
UI_method_set_opener(UI_METHOD *method, int (*opener)(UI *ui))
{
	if (method) {
		method->ui_open_session = opener;
		return 0;
	} else
		return -1;
}

int
UI_method_set_writer(UI_METHOD *method, int (*writer)(UI *ui, UI_STRING *uis))
{
	if (method) {
		method->ui_write_string = writer;
		return 0;
	} else
		return -1;
}

int
UI_method_set_flusher(UI_METHOD *method, int (*flusher)(UI *ui))
{
	if (method) {
		method->ui_flush = flusher;
		return 0;
	} else
		return -1;
}

int
UI_method_set_reader(UI_METHOD *method, int (*reader)(UI *ui, UI_STRING *uis))
{
	if (method) {
		method->ui_read_string = reader;
		return 0;
	} else
		return -1;
}

int
UI_method_set_closer(UI_METHOD *method, int (*closer)(UI *ui))
{
	if (method) {
		method->ui_close_session = closer;
		return 0;
	} else
		return -1;
}

int
UI_method_set_prompt_constructor(UI_METHOD *method,
    char *(*prompt_constructor)(UI *ui, const char *object_desc,
    const char *object_name))
{
	if (method) {
		method->ui_construct_prompt = prompt_constructor;
		return 0;
	} else
		return -1;
}

int
(*UI_method_get_opener(UI_METHOD * method))(UI *)
{
	if (method)
		return method->ui_open_session;
	else
		return NULL;
}

int
(*UI_method_get_writer(UI_METHOD *method))(UI *, UI_STRING *)
{
	if (method)
		return method->ui_write_string;
	else
		return NULL;
}

int
(*UI_method_get_flusher(UI_METHOD *method)) (UI *)
{
	if (method)
		return method->ui_flush;
	else
		return NULL;
}

int
(*UI_method_get_reader(UI_METHOD *method))(UI *, UI_STRING *)
{
	if (method)
		return method->ui_read_string;
	else
		return NULL;
}

int
(*UI_method_get_closer(UI_METHOD *method))(UI *)
{
	if (method)
		return method->ui_close_session;
	else
		return NULL;
}

char *
(*UI_method_get_prompt_constructor(UI_METHOD *method))(UI *, const char *,
    const char *)
{
	if (method)
		return method->ui_construct_prompt;
	else
		return NULL;
}

enum UI_string_types
UI_get_string_type(UI_STRING *uis)
{
	if (!uis)
		return UIT_NONE;
	return uis->type;
}

int
UI_get_input_flags(UI_STRING *uis)
{
	if (!uis)
		return 0;
	return uis->input_flags;
}

const char *
UI_get0_output_string(UI_STRING *uis)
{
	if (!uis)
		return NULL;
	return uis->out_string;
}

const char *
UI_get0_action_string(UI_STRING *uis)
{
	if (!uis)
		return NULL;
	switch (uis->type) {
	case UIT_PROMPT:
	case UIT_BOOLEAN:
		return uis->_.boolean_data.action_desc;
	default:
		return NULL;
	}
}

const char *
UI_get0_result_string(UI_STRING *uis)
{
	if (!uis)
		return NULL;
	switch (uis->type) {
	case UIT_PROMPT:
	case UIT_VERIFY:
		return uis->result_buf;
	default:
		return NULL;
	}
}

const char *
UI_get0_test_string(UI_STRING *uis)
{
	if (!uis)
		return NULL;
	switch (uis->type) {
	case UIT_VERIFY:
		return uis->_.string_data.test_buf;
	default:
		return NULL;
	}
}

int
UI_get_result_minsize(UI_STRING *uis)
{
	if (!uis)
		return -1;
	switch (uis->type) {
	case UIT_PROMPT:
	case UIT_VERIFY:
		return uis->_.string_data.result_minsize;
	default:
		return -1;
	}
}

int
UI_get_result_maxsize(UI_STRING *uis)
{
	if (!uis)
		return -1;
	switch (uis->type) {
	case UIT_PROMPT:
	case UIT_VERIFY:
		return uis->_.string_data.result_maxsize;
	default:
		return -1;
	}
}

int
UI_set_result(UI *ui, UI_STRING *uis, const char *result)
{
	int l = strlen(result);

	ui->flags &= ~UI_FLAG_REDOABLE;

	if (!uis)
		return -1;
	switch (uis->type) {
	case UIT_PROMPT:
	case UIT_VERIFY:
		if (l < uis->_.string_data.result_minsize) {
			ui->flags |= UI_FLAG_REDOABLE;
			UIerror(UI_R_RESULT_TOO_SMALL);
			ERR_asprintf_error_data
			    ("You must type in %d to %d characters",
				uis->_.string_data.result_minsize,
				uis->_.string_data.result_maxsize);
			return -1;
		}
		if (l > uis->_.string_data.result_maxsize) {
			ui->flags |= UI_FLAG_REDOABLE;
			UIerror(UI_R_RESULT_TOO_LARGE);
			ERR_asprintf_error_data
			    ("You must type in %d to %d characters",
				uis->_.string_data.result_minsize,
				uis->_.string_data.result_maxsize);
			return -1;
		}
		if (!uis->result_buf) {
			UIerror(UI_R_NO_RESULT_BUFFER);
			return -1;
		}
		strlcpy(uis->result_buf, result,
		    uis->_.string_data.result_maxsize + 1);
		break;
	case UIT_BOOLEAN:
		{
			const char *p;

			if (!uis->result_buf) {
				UIerror(UI_R_NO_RESULT_BUFFER);
				return -1;
			}
			uis->result_buf[0] = '\0';
			for (p = result; *p; p++) {
				if (strchr(uis->_.boolean_data.ok_chars, *p)) {
					uis->result_buf[0] =
					    uis->_.boolean_data.ok_chars[0];
					break;
				}
				if (strchr(uis->_.boolean_data.cancel_chars, *p)) {
					uis->result_buf[0] =
					    uis->_.boolean_data.cancel_chars[0];
					break;
				}
			}
		default:
			break;
		}
	}
	return 0;
}
