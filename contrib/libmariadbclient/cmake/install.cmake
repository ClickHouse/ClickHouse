#
#  Copyright (C) 2013-2016 MariaDB Corporation AB
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the COPYING-CMAKE-SCRIPTS file.
#

#
# This file contains settings for the following layouts:
#
# - RPM
# Built with default prefix=/usr
#
#
# The following va+riables are used and can be overwritten
# 
# INSTALL_LAYOUT     installation layout (DEFAULT = standard for tar.gz and zip packages
#                                         RPM packages
#
# INSTALL_BINDIR    location of binaries (mariadb_config)
# INSTALL_LIBDIR    location of libraries
# INSTALL_PLUGINDIR location of plugins

IF(DEFINED INSTALL_BINDIR AND DEFINED INSTALL_LIBDIR AND DEFINED INSTALL_INCLUDEDIR AND DEFINED INSTALL_DOCDIR AND DEFINED INSTALL_PLUGINDIR)
  RETURN()
ENDIF()

IF(NOT INSTALL_LAYOUT)
  SET(INSTALL_LAYOUT "DEFAULT")
ENDIF()

SET(INSTALL_LAYOUT ${INSTALL_LAYOUT} CACHE
  STRING "Installation layout. Currently supported options are DEFAULT (tar.gz and zip) and RPM")

# On Windows we only provide zip and .msi. Latter one uses a different packager. 
IF(UNIX)
  IF(INSTALL_LAYOUT MATCHES "RPM")
    SET(libmariadb_prefix "/usr")
  ELSEIF(INSTALL_LAYOUT MATCHES "DEFAULT")
    SET(libmariadb_prefix ${CMAKE_INSTALL_PREFIX})
  ENDIF()
ENDIF()

IF(CMAKE_DEFAULT_PREFIX_INITIALIZED_BY_DEFAULT)
  SET(CMAKE_DEFAULT_PREFIX ${libmariadb_prefix} CACHE PATH "Installation prefix" FORCE)
ENDIF()

# check if the specified installation layout is valid
SET(VALID_INSTALL_LAYOUTS "DEFAULT" "RPM")
LIST(FIND VALID_INSTALL_LAYOUTS "${INSTALL_LAYOUT}" layout_no)
IF(layout_no EQUAL -1)
  MESSAGE(FATAL_ERROR "Invalid installation layout ${INSTALL_LAYOUT}. Please specify one of the following layouts: ${VALID_INSTALL_LAYOUTS}")
ENDIF()



#
# Todo: We don't generate man pages yet, will fix it
#       later (webhelp to man transformation)
#

#
# DEFAULT layout
#



SET(INSTALL_BINDIR_DEFAULT "bin")
SET(INSTALL_LIBDIR_DEFAULT "lib/mariadb")
SET(INSTALL_INCLUDEDIR_DEFAULT "include/mariadb")
SET(INSTALL_DOCDIR_DEFAULT "docs")
SET(INSTALL_PLUGINDIR_DEFAULT "lib/mariadb/plugin")

#
# RPM layout
#
SET(INSTALL_BINDIR_RPM "bin")
IF(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
  SET(INSTALL_LIBDIR_RPM "lib64/mariadb")
  SET(INSTALL_PLUGINDIR_RPM "lib64/mariadb/plugin")
ELSE()
  SET(INSTALL_LIBDIR_RPM "lib")
  SET(INSTALL_PLUGINDIR_RPM "lib/mariadb/plugin")
ENDIF()
SET(INSTALL_INCLUDEDIR_RPM "include")
SET(INSTALL_DOCDIR_RPM "docs")

#
# Overwrite defaults
#
IF(INSTALL_LIBDIR)
  SET(INSTALL_LIBDIR_${INSTALL_LAYOUT} ${INSTALL_LIBDIR})
ENDIF()

IF(INSTALL_PLUGINDIR)
  SET(INSTALL_PLUGINDIR_${INSTALL_LAYOUT} ${INSTALL_PLUGINDIR})
ENDIF()

IF(INSTALL_INCLUDEDIR)
  SET(INSTALL_INCLUDEDIR_${INSTALL_LAYOUT} ${INSTALL_INCLUDEDIR})
ENDIF()

IF(INSTALL_BINDIR)
  SET(INSTALL_BINDIR_${INSTALL_LAYOUT} ${INSTALL_BINDIR})
ENDIF()

IF(NOT INSTALL_PREFIXDIR)
  SET(INSTALL_PREFIXDIR_${INSTALL_LAYOUT} ${libmariadb_prefix})
ELSE()
  SET(INSTALL_PREFIXDIR_${INSTALL_LAYOUT} ${INSTALL_PREFIXDIR})
ENDIF()

IF(DEFINED INSTALL_SUFFIXDIR)
  SET(INSTALL_SUFFIXDIR_${INSTALL_LAYOUT} ${INSTALL_SUFFIXDIR})
ENDIF()

FOREACH(dir "BIN" "LIB" "INCLUDE" "DOCS"  "PLUGIN")
  SET(INSTALL_${dir}DIR ${INSTALL_${dir}DIR_${INSTALL_LAYOUT}})
  MARK_AS_ADVANCED(INSTALL_${dir}DIR)
ENDFOREACH()
