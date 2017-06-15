INCLUDE_DIRECTORIES(${CC_SOURCE_DIR}/include)
INCLUDE_DIRECTORIES(${CC_SOURCE_DIR}/plugins/auth)

INCLUDE(${CC_SOURCE_DIR}/cmake/install_plugins.cmake)
IF(WITH_SIGNCODE)
  INCLUDE(${CC_SOURCE_DIR}/cmake/sign.cmake)
ENDIF()

# Dialog plugin
IF(AUTH_DIALOG_PLUGIN_TYPE MATCHES "DYNAMIC")
  ADD_DEFINITIONS(-DHAVE_DIALOG_DYNAMIC=1)
  IF(WIN32)
    SET_VERSION_INFO("TARGET:dialog"
                     "FILE_TYPE:VFT_DLL"
                     "SOURCE_FILE:plugins/auth/dialog.c"
                     "ORIGINAL_FILE_NAME:dialog.dll"
                     "FILE_DESCRIPTION:Authentication plugin")
  ENDIF()
  SET(DIALOG_SOURCES ${dialog_RC} dialog.c ${CC_SOURCE_DIR}/libmariadb/get_password.c)
  IF(WIN32)
    SET(DIALOG_SOURCES ${DIALOG_SOURCES} ${CC_SOURCE_DIR}/plugins/plugin.def)
  ENDIF()
  ADD_LIBRARY(dialog MODULE ${DIALOG_SOURCES})
  SET_TARGET_PROPERTIES(dialog PROPERTIES PREFIX "")
  SIGN_TARGET(dialog)
  INSTALL_PLUGIN(dialog ${CC_BINARY_DIR}/plugins/auth)
ENDIF()

# SHA256 plugin
IF(AUTH_SHA256PW_PLUGIN_TYPE MATCHES "DYNAMIC")
  ADD_DEFINITIONS(-DHAVE_SHA256PW_DYNAMIC=1)

  IF(WIN32)
    SET_VERSION_INFO("TARGET:sha256_password"
                     "FILE_TYPE:VFT_DLL"
                     "SOURCE_FILE:plugins/auth/sha256_pw.c"
                     "ORIGINAL_FILE_NAME:sha256_password.dll"
                     "FILE_DESCRIPTION:Authentication plugin")
  ENDIF()
  SET(SHA256PW_SOURCES ${sha256_RC} sha256_pw.c)
  IF(WIN32)
    SET(SHA256PW_SOURCES ${SHA256PW_SOURCES} ${CC_SOURCE_DIR}/plugins/plugin.def)
  ENDIF()
  ADD_LIBRARY(sha256_password MODULE ${SHA256PW_SOURCES})
  IF(WITH_SSL STREQUAL "OPENSSL")
    TARGET_LINK_LIBRARIES(sha256_password ${SSL_LIBRARIES})
  ELSEIF(WIN32)
    TARGET_LINK_LIBRARIES(sha256_password crypt32)
  ENDIF()
  SET_TARGET_PROPERTIES(sha256_password PROPERTIES PREFIX "")
  SIGN_TARGET(sha256_password)
  INSTALL_PLUGIN(sha256_password ${CC_BINARY_DIR}/plugins/auth)
ENDIF()

# old_password plugin
IF(AUTH_OLDPASSWORD_PLUGIN_TYPE MATCHES "DYNAMIC")
  ADD_DEFINITIONS(-DHAVE_OLDPASSWORD_DYNAMIC=1)
  IF(WIN32)
    SET_VERSION_INFO("TARGET:mysql_old_password"
                     "FILE_TYPE:VFT_DLL"
                     "SOURCE_FILE:plugins/auth/old_password.c"
                     "ORIGINAL_FILE_NAME:mysql_old_password.dll"
                     "FILE_DESCRIPTION:Authentication plugin")
  ENDIF()
  SET(OLDPASSWORD_SOURCES ${old_password_RC} old_password.c)
  IF(WIN32)
    SET(OLDPASSWORD_SOURCES ${DIALOG_SOURCES} ${CC_SOURCE_DIR}/plugins/plugin.def)
  ENDIF()
  ADD_LIBRARY(mysql_old_password MODULE ${OLDPASSWORD_SOURCES})
  SET_TARGET_PROPERTIES(mysql_old_password PROPERTIES PREFIX "")
  INSTALL_PLUGIN(mysql_old_password ${CC_BINARY_DIR}/plugins/auth)
  SIGN_TARGET(mysql_old_password)
ENDIF()

# Cleartext
IF(AUTH_CLEARTEXT_PLUGIN_TYPE MATCHES "DYNAMIC")
  ADD_DEFINITIONS(-DHAVE_CLEARTEXT_DYNAMIC=1)
  IF(WIN32)
    SET_VERSION_INFO("TARGET:mysql_clear_password"
                     "FILE_TYPE:VFT_DLL"
                     "SOURCE_FILE:plugins/auth/mariadb_cleartext.c"
                     "ORIGINAL_FILE_NAME:mysql_clear_password.dll"
                     "FILE_DESCRIPTION:Authentication plugin")
  ENDIF()
  SET(CTEXT_SOURCES mariadb_cleartext.c)
  IF(WIN32)
    SET(CTEXT_SOURCES ${CTEXT_SOURCES} ${mysql_clear_password_RC} ${CC_SOURCE_DIR}/plugins/plugin.def)
  ENDIF()
  ADD_LIBRARY(mysql_clear_password MODULE ${CTEXT_SOURCES})
  SET_TARGET_PROPERTIES(mysql_clear_password PROPERTIES PREFIX "")
  INSTALL_PLUGIN(mysql_clear_password ${CC_BINARY_DIR}/plugins/auth)
  SIGN_TARGET(mysql_clear_password)
ENDIF()

# SSPI/GSSAPI plugin
IF(WIN32)
  SET(USE_SSPI 1)
ENDIF()

MESSAGE(GSSAPI "GSSAPI: ${AUTH_GSSAPI_PLUGIN_TYPE}")
IF(${AUTH_GSSAPI_PLUGIN_TYPE} MATCHES "DYNAMIC")
  IF(WIN32)
    SET_VERSION_INFO("TARGET:auth_gssapi_client"
                     "FILE_TYPE:VFT_DLL"
                     "SOURCE_FILE:plugins/auth/auth_gssapi_client.c"
                     "ORIGINAL_FILE_NAME:auth_gssapi_client.dll"
                     "FILE_DESCRIPTION:Authentication plugin")
                   ADD_DEFINITIONS(-DHAVE_AUTH_GSSAPI_DYNAMIC=1)               
    SET(GSSAPI_SOURCES auth_gssapi_client.c sspi_client.c sspi_errmsg.c ${CC_SOURCE_DIR}/plugins/plugin.def ${auth_gssapi_client_RC})
  ELSE()
    IF(GSSAPI_FOUND)
      SET(GSSAPI_SOURCES auth_gssapi_client.c gssapi_client.c gssapi_errmsg.c)
    ENDIF()
  ENDIF()
  IF(GSSAPI_FOUND OR WIN32)
    ADD_LIBRARY(auth_gssapi_client MODULE ${GSSAPI_SOURCES})
    IF(WIN32)
      TARGET_LINK_LIBRARIES(auth_gssapi_client secur32.lib)
    ELSE()
      TARGET_LINK_LIBRARIES(auth_gssapi_client ${GSSAPI_LIBS})
    ENDIF()
    SET_TARGET_PROPERTIES(auth_gssapi_client PROPERTIES PREFIX "")
    SIGN_TARGET(auth_gssapi_client)
    INSTALL_PLUGIN(auth_gssapi_client ${CC_BINARY_DIR}/plugins/auth)
  ENDIF()
ENDIF()
