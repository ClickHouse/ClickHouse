/**
 * @file
 *
 * @brief compile-time version information
 *
 * compile-time version information for the XML library
 *
 * @copyright See Copyright for the status of this software.
 *
 * @author Daniel Veillard
 */

#ifndef __XML_VERSION_H__
#define __XML_VERSION_H__

/**
 * the version string like "1.2.3"
 */
#define LIBXML_DOTTED_VERSION "2.15.1"

/**
 * the version number: 1.2.3 value is 10203
 */
#define LIBXML_VERSION 21501

/**
 * the version number string, 1.2.3 value is "10203"
 */
#define LIBXML_VERSION_STRING "21501"

/**
 * extra version information, used to show a git commit description
 */
#define LIBXML_VERSION_EXTRA ""

/**
 * Macro to check that the libxml version in use is compatible with
 * the version the software has been compiled against
 */
#define LIBXML_TEST_VERSION xmlCheckVersion(21501);

#if 1
/**
 * Whether the thread support is configured in
 */
#define LIBXML_THREAD_ENABLED
#endif

#if 0
/**
 * Whether the allocation hooks are per-thread
 */
#define LIBXML_THREAD_ALLOC_ENABLED
#endif

/**
 * Always enabled since 2.14.0
 */
#define LIBXML_TREE_ENABLED

#if 1
/**
 * Whether the serialization/saving support is configured in
 */
#define LIBXML_OUTPUT_ENABLED
#endif

#if 1
/**
 * Whether the push parsing interfaces are configured in
 */
#define LIBXML_PUSH_ENABLED
#endif

#if 1
/**
 * Whether the xmlReader parsing interface is configured in
 */
#define LIBXML_READER_ENABLED
#endif

#if 1
/**
 * Whether the xmlPattern node selection interface is configured in
 */
#define LIBXML_PATTERN_ENABLED
#endif

#if 1
/**
 * Whether the xmlWriter saving interface is configured in
 */
#define LIBXML_WRITER_ENABLED
#endif

#if 1
/**
 * Whether the older SAX1 interface is configured in
 */
#define LIBXML_SAX1_ENABLED
#endif

#if 0
/**
 * HTTP support was removed in 2.15
 */
#define LIBXML_HTTP_STUBS_ENABLED
#endif

#if 1
/**
 * Whether the DTD validation support is configured in
 */
#define LIBXML_VALID_ENABLED
#endif

#if 1
/**
 * Whether the HTML support is configured in
 */
#define LIBXML_HTML_ENABLED
#endif

/*
 * Removed in 2.14
 */
#undef LIBXML_LEGACY_ENABLED

#if 1
/**
 * Whether the Canonicalization support is configured in
 */
#define LIBXML_C14N_ENABLED
#endif

#if 1
/**
 * Whether the Catalog support is configured in
 */
#define LIBXML_CATALOG_ENABLED
#define LIBXML_SGML_CATALOG_ENABLED
#endif

#if 1
/**
 * Whether XPath is configured in
 */
#define LIBXML_XPATH_ENABLED
#endif

#if 1
/**
 * Whether XPointer is configured in
 */
#define LIBXML_XPTR_ENABLED
#endif

#if 1
/**
 * Whether XInclude is configured in
 */
#define LIBXML_XINCLUDE_ENABLED
#endif

#if 1
/**
 * Whether iconv support is available
 */
#define LIBXML_ICONV_ENABLED
#endif

#if 0
/**
 * Whether icu support is available
 */
#define LIBXML_ICU_ENABLED
#endif

#if 1
/**
 * Whether ISO-8859-* support is made available in case iconv is not
 */
#define LIBXML_ISO8859X_ENABLED
#endif

#if 1
/**
 * Whether Debugging module is configured in
 */
#define LIBXML_DEBUG_ENABLED
#endif

/*
 * Removed in 2.14
 */
#undef LIBXML_UNICODE_ENABLED

#if 1
/**
 * Whether the regular expressions interfaces are compiled in
 */
#define LIBXML_REGEXP_ENABLED
#endif

#if 1
/**
 * Whether the automata interfaces are compiled in
 */
#define LIBXML_AUTOMATA_ENABLED
#endif

#if 1
/**
 * Whether the RelaxNG validation interfaces are compiled in
 */
#define LIBXML_RELAXNG_ENABLED
#endif

#if 1
/**
 * Whether the Schemas validation interfaces are compiled in
 */
#define LIBXML_SCHEMAS_ENABLED
#endif

#if 0
/**
 * Whether the Schematron validation interfaces are compiled in
 */
#define LIBXML_SCHEMATRON_ENABLED
#endif

#if 1
/**
 * Whether the module interfaces are compiled in
 */
#define LIBXML_MODULES_ENABLED
/**
 * the string suffix used by dynamic modules (usually shared libraries)
 */
#define LIBXML_MODULE_EXTENSION ".so" 
#endif

#if 0
/**
 * Whether the Zlib support is compiled in
 */
#define LIBXML_ZLIB_ENABLED
#endif

#include <libxml/xmlexports.h>

#endif


