/* ----------------------------------------------------------------------------
Copyright (c) 2018, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc-internal.h"

#include <stdio.h>
#include <string.h> // strcmp
#include <ctype.h>  // toupper
#include <stdarg.h>

// --------------------------------------------------------
// Options
// --------------------------------------------------------
typedef enum mi_init_e {
  UNINIT,       // not yet initialized
  DEFAULTED,    // not found in the environment, use default value
  INITIALIZED   // found in environment or set explicitly
} mi_init_t;

typedef struct mi_option_desc_s {
  long        value;  // the value
  mi_init_t   init;   // is it initialized yet? (from the environment)
  const char* name;   // option name without `mimalloc_` prefix
} mi_option_desc_t;

static mi_option_desc_t options[_mi_option_last] = {
  { 0, UNINIT, "page_reset" },
  { 0, UNINIT, "cache_reset" },
  { 0, UNINIT, "pool_commit" },
  #if MI_SECURE
  { MI_SECURE, INITIALIZED, "secure" }, // in secure build the environment setting is ignored
  #else
  { 0, UNINIT, "secure" },
  #endif
  { 0, UNINIT, "show_stats" },
  { MI_DEBUG, UNINIT, "show_errors" },
  { MI_DEBUG, UNINIT, "verbose" }
};

static void mi_option_init(mi_option_desc_t* desc);

long mi_option_get(mi_option_t option) {
  mi_assert(option >= 0 && option < _mi_option_last);
  mi_option_desc_t* desc = &options[option];
  if (desc->init == UNINIT) {
    mi_option_init(desc);
    if (option != mi_option_verbose) {
      _mi_verbose_message("option '%s': %zd\n", desc->name, desc->value);
    }
  }
  return desc->value;
}

void mi_option_set(mi_option_t option, long value) {
  mi_assert(option >= 0 && option < _mi_option_last);
  mi_option_desc_t* desc = &options[option];
  desc->value = value;
  desc->init = INITIALIZED;
}

void mi_option_set_default(mi_option_t option, long value) {
  mi_assert(option >= 0 && option < _mi_option_last);
  mi_option_desc_t* desc = &options[option];
  if (desc->init != INITIALIZED) {
    desc->value = value;
  }
}

bool mi_option_is_enabled(mi_option_t option) {
  return (mi_option_get(option) != 0);
}

void mi_option_enable(mi_option_t option, bool enable) {
  mi_option_set(option, (enable ? 1 : 0));
}

void mi_option_enable_default(mi_option_t option, bool enable) {
  mi_option_set_default(option, (enable ? 1 : 0));
}

// --------------------------------------------------------
// Messages
// --------------------------------------------------------

// Define our own limited `fprintf` that avoids memory allocation.
// We do this using `snprintf` with a limited buffer.
static void mi_vfprintf( FILE* out, const char* prefix, const char* fmt, va_list args ) {
  char buf[256];
  if (fmt==NULL) return;
  if (out==NULL) out = stdout;
  vsnprintf(buf,sizeof(buf)-1,fmt,args);
  if (prefix != NULL) fputs(prefix,out);
  fputs(buf,out);
}

void _mi_fprintf( FILE* out, const char* fmt, ... ) {
  va_list args;
  va_start(args,fmt);
  mi_vfprintf(out,NULL,fmt,args);
  va_end(args);
}

void _mi_verbose_message(const char* fmt, ...) {
  if (!mi_option_is_enabled(mi_option_verbose)) return;
  va_list args;
  va_start(args,fmt);
  mi_vfprintf(stderr, "mimalloc: ", fmt, args);
  va_end(args);
}

void _mi_error_message(const char* fmt, ...) {
  if (!mi_option_is_enabled(mi_option_show_errors) && !mi_option_is_enabled(mi_option_verbose)) return;
  va_list args;
  va_start(args,fmt);
  mi_vfprintf(stderr, "mimalloc: error: ", fmt, args);
  va_end(args);
  mi_assert(false);
}

void _mi_warning_message(const char* fmt, ...) {
  if (!mi_option_is_enabled(mi_option_show_errors) && !mi_option_is_enabled(mi_option_verbose)) return;
  va_list args;
  va_start(args,fmt);
  mi_vfprintf(stderr, "mimalloc: warning: ", fmt, args);
  va_end(args);
}


#if MI_DEBUG
void _mi_assert_fail(const char* assertion, const char* fname, unsigned line, const char* func ) {
  _mi_fprintf(stderr,"mimalloc: assertion failed: at \"%s\":%u, %s\n  assertion: \"%s\"\n", fname, line, (func==NULL?"":func), assertion);
  abort();
}
#endif

// --------------------------------------------------------
// Initialize options by checking the environment
// --------------------------------------------------------

static void mi_strlcpy(char* dest, const char* src, size_t dest_size) {
  dest[0] = 0;
  #pragma warning(suppress:4996)
  strncpy(dest, src, dest_size - 1);
  dest[dest_size - 1] = 0;
}

static void mi_strlcat(char* dest, const char* src, size_t dest_size) {
  #pragma warning(suppress:4996)
  strncat(dest, src, dest_size - 1);
  dest[dest_size - 1] = 0;
}

static void mi_option_init(mi_option_desc_t* desc) {
  desc->init = DEFAULTED;
  // Read option value from the environment
  char buf[32];
  mi_strlcpy(buf, "mimalloc_", sizeof(buf));
  mi_strlcat(buf, desc->name, sizeof(buf));
  #pragma warning(suppress:4996)
  char* s = getenv(buf);
  if (s == NULL) {
    for (size_t i = 0; i < strlen(buf); i++) {
      buf[i] = toupper(buf[i]);
    }
    #pragma warning(suppress:4996)
    s = getenv(buf);
  }
  if (s != NULL) {
    mi_strlcpy(buf, s, sizeof(buf));
    for (size_t i = 0; i < strlen(buf); i++) {
      buf[i] = toupper(buf[i]);
    }
    if (buf[0]==0 || strstr("1;TRUE;YES;ON", buf) != NULL) {
      desc->value = 1;
      desc->init = INITIALIZED;
    }
    else if (strstr("0;FALSE;NO;OFF", buf) != NULL) {
      desc->value = 0;
      desc->init = INITIALIZED;
    }
    else {
      char* end = buf;
      long value = strtol(buf, &end, 10);
      if (*end == 0) {
        desc->value = value;
        desc->init = INITIALIZED;
      }
      else {
        _mi_warning_message("environment option mimalloc_%s has an invalid value: %s\n", desc->name, buf);
      }
    }
  }
}
