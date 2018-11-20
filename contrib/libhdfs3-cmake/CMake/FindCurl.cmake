# - Try to find the CURL library (curl)
#
# Once done this will define
#
#  CURL_FOUND - System has gnutls
#  CURL_INCLUDE_DIR - The gnutls include directory
#  CURL_LIBRARIES - The libraries needed to use gnutls
#  CURL_DEFINITIONS - Compiler switches required for using gnutls


IF (CURL_INCLUDE_DIR AND CURL_LIBRARIES)
	# in cache already
	SET(CURL_FIND_QUIETLY TRUE)
ENDIF (CURL_INCLUDE_DIR AND CURL_LIBRARIES)

FIND_PATH(CURL_INCLUDE_DIR curl/curl.h)

FIND_LIBRARY(CURL_LIBRARIES curl)

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set CURL_FOUND to TRUE if 
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CURL DEFAULT_MSG CURL_LIBRARIES CURL_INCLUDE_DIR)

MARK_AS_ADVANCED(CURL_INCLUDE_DIR CURL_LIBRARIES)
