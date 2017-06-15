#
#  Copyright (C) 2013-2016 MariaDB Corporation AB
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the COPYING-CMAKE-SCRIPTS file.
#
# plugin configuration

MACRO(REGISTER_PLUGIN name source struct type target allow)
  SET(PLUGIN_TYPE ${${name}})
  IF(NOT PLUGIN_TYPE STREQUAL "OFF" AND NOT PLUGIN_TYPE)
    SET(PLUGIN_TYPE ${type})
  ENDIF()
  IF(PLUGINS)
    LIST(REMOVE_ITEM PLUGINS ${name})
  ENDIF()
  SET(${name}_PLUGIN_SOURCE ${source})
  MARK_AS_ADVANCED(${name}_PLUGIN_SOURCE})
  SET(${name}_PLUGIN_TYPE ${PLUGIN_TYPE})
  IF(NOT ${target} STREQUAL "")
    SET(${name}_PLUGIN_TARGET ${target})
  ENDIF()
  SET(${name}_PLUGIN_STRUCT ${struct})
  SET(${name}_PLUGIN_SOURCE ${source})
  SET(${name}_PLUGIN_CHG ${allow})
  SET(PLUGINS ${PLUGINS} "${name}")
  ADD_DEFINITIONS(-DHAVE_${name}=1)
ENDMACRO()

# CIO
REGISTER_PLUGIN("SOCKET" "${CC_SOURCE_DIR}/plugins/pvio/pvio_socket.c" "pvio_socket_plugin" "STATIC" pvio_socket 0)
REGISTER_PLUGIN("AUTH_NATIVE" "${CC_SOURCE_DIR}/plugins/auth/my_auth.c" "native_password_client_plugin" "STATIC" "" 0)
REGISTER_PLUGIN("AUTH_OLDPASSWORD" "${CC_SOURCE_DIR}/plugins/auth/old_password.c" "old_password_client_plugin" "STATIC" "" 1)

SET(LIBMARIADB_SOURCES "")

FOREACH(PLUGIN ${PLUGINS})
  IF(${OPT}WITH_${PLUGIN}_PLUGIN AND ${${PLUGIN}_PLUGIN_CHG} GREATER 0)
    SET(${PLUGIN}_PLUGIN_TYPE ${${OPT}WITH_${PLUGIN}_PLUGIN})
  ENDIF()
  IF(${PLUGIN}_PLUGIN_TYPE MATCHES "STATIC")
    SET(LIBMARIADB_SOURCES ${LIBMARIADB_SOURCES} ${${PLUGIN}_PLUGIN_SOURCE})
    SET(EXTERNAL_PLUGINS "${EXTERNAL_PLUGINS}extern struct st_mysql_client_plugin ${${PLUGIN}_PLUGIN_STRUCT};\n")
    SET(BUILTIN_PLUGINS "${BUILTIN_PLUGINS}(struct st_mysql_client_plugin *)&${${PLUGIN}_PLUGIN_STRUCT},\n")
  ENDIF()
  SET(plugin_config "${plugin_config}\n-- ${PLUGIN}: ${${PLUGIN}_PLUGIN_TYPE}")
  MARK_AS_ADVANCED(${PLUGIN}_PLUGIN_TYPE)
ENDFOREACH()
MESSAGE(plugin_config "Plugin configuration:${plugin_config}")
MESSAGE(LIBMARIADB_SOURCES "STATIC PLUGIN SOURCES: ${LIBMARIADB_SOURCES}")

# since some files contain multiple plugins, remove duplicates from source files 
LIST(REMOVE_DUPLICATES LIBMARIADB_SOURCES)

CONFIGURE_FILE(${CC_SOURCE_DIR}/libmariadb/ma_client_plugin.c.in
  ${CC_BINARY_DIR}/libmariadb/ma_client_plugin.c)
