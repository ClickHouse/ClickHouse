/* Header file for internal GCC plugin mechanism.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.

This file is part of GCC.

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

#ifndef PLUGIN_H
#define PLUGIN_H

#include "highlev-plugin-common.h"

/* Event names.  */
enum plugin_event
{
# define DEFEVENT(NAME) NAME,
# include "plugin.def"
# undef DEFEVENT
  PLUGIN_EVENT_FIRST_DYNAMIC
};

/* All globals declared here have C linkage to reduce link compatibility
   issues with implementation language choice and mangling.  */
#ifdef __cplusplus
extern "C" {
#endif

extern const char **plugin_event_name;

struct plugin_argument
{
  char *key;    /* key of the argument.  */
  char *value;  /* value is optional and can be NULL.  */
};

/* Additional information about the plugin. Used by --help and --version. */

struct plugin_info
{
  const char *version;
  const char *help;
};

/* Represents the gcc version. Used to avoid using an incompatible plugin. */

struct plugin_gcc_version
{
  const char *basever;
  const char *datestamp;
  const char *devphase;
  const char *revision;
  const char *configuration_arguments;
};

/* Object that keeps track of the plugin name and its arguments. */
struct plugin_name_args
{
  char *base_name;              /* Short name of the plugin (filename without
                                   .so suffix). */
  const char *full_name;        /* Path to the plugin as specified with
                                   -fplugin=. */
  int argc;                     /* Number of arguments specified with
                                   -fplugin-arg-... */
  struct plugin_argument *argv; /* Array of ARGC key-value pairs. */
  const char *version;          /* Version string provided by plugin. */
  const char *help;             /* Help string provided by plugin. */
};

/* The default version check. Compares every field in VERSION. */

extern bool plugin_default_version_check (struct plugin_gcc_version *,
					  struct plugin_gcc_version *);

/* Function type for the plugin initialization routine. Each plugin module
   should define this as an externally-visible function with name
   "plugin_init."

   PLUGIN_INFO - plugin invocation information.
   VERSION     - the plugin_gcc_version symbol of GCC.

   Returns 0 if initialization finishes successfully.  */

typedef int (*plugin_init_func) (struct plugin_name_args *plugin_info,
                                 struct plugin_gcc_version *version);

/* Declaration for "plugin_init" function so that it doesn't need to be
   duplicated in every plugin.  */
extern int plugin_init (struct plugin_name_args *plugin_info,
                        struct plugin_gcc_version *version);

/* Function type for a plugin callback routine.

   GCC_DATA  - event-specific data provided by GCC
   USER_DATA - plugin-specific data provided by the plugin  */

typedef void (*plugin_callback_func) (void *gcc_data, void *user_data);

/* Called from the plugin's initialization code. Register a single callback.
   This function can be called multiple times.

   PLUGIN_NAME - display name for this plugin
   EVENT       - which event the callback is for
   CALLBACK    - the callback to be called at the event
   USER_DATA   - plugin-provided data.
*/

/* Number of event ids / names registered so far.  */

extern int get_event_last (void);

int get_named_event_id (const char *name, enum insert_option insert);

/* This is also called without a callback routine for the
   PLUGIN_PASS_MANAGER_SETUP, PLUGIN_INFO and PLUGIN_REGISTER_GGC_ROOTS
   pseudo-events, with a specific user_data.
  */

extern void register_callback (const char *plugin_name,
			       int event,
                               plugin_callback_func callback,
                               void *user_data);

extern int unregister_callback (const char *plugin_name, int event);


/* Retrieve the plugin directory name, as returned by the
   -fprint-file-name=plugin argument to the gcc program, which is the
   -iplugindir program argument to cc1.  */
extern const char* default_plugin_dir_name (void);

#ifdef __cplusplus
}
#endif

/* In case the C++ compiler does name mangling for globals, declare
   plugin_is_GPL_compatible extern "C" so that a later definition
   in a plugin file will have this linkage.  */
#ifdef __cplusplus
extern "C" {
#endif
extern int plugin_is_GPL_compatible;
#ifdef __cplusplus
}
#endif


struct attribute_spec;
struct scoped_attributes;

extern void add_new_plugin (const char *);
extern void parse_plugin_arg_opt (const char *);
extern int invoke_plugin_callbacks_full (int, void *);
extern void initialize_plugins (void);
extern bool plugins_active_p (void);
extern void dump_active_plugins (FILE *);
extern void debug_active_plugins (void);
extern void warn_if_plugins (void);
extern void print_plugins_versions (FILE *file, const char *indent);
extern void print_plugins_help (FILE *file, const char *indent);
extern void finalize_plugins (void);

extern bool flag_plugin_added;

/* Called from inside GCC.  Invoke all plugin callbacks registered with
   the specified event.
   Return PLUGEVT_SUCCESS if at least one callback was called,
   PLUGEVT_NO_CALLBACK if there was no callback.

   EVENT    - the event identifier
   GCC_DATA - event-specific data provided by the compiler  */

static inline int
invoke_plugin_callbacks (int event ATTRIBUTE_UNUSED,
			 void *gcc_data ATTRIBUTE_UNUSED)
{
#ifdef ENABLE_PLUGIN
  /* True iff at least one plugin has been added.  */
  if (flag_plugin_added)
    return invoke_plugin_callbacks_full (event, gcc_data);
#endif

  return PLUGEVT_NO_CALLBACK;
}

/* In attribs.c.  */

extern void register_attribute (const struct attribute_spec *attr);
extern struct scoped_attributes* register_scoped_attributes (const struct attribute_spec *,
							     const char *);

#endif /* PLUGIN_H */
