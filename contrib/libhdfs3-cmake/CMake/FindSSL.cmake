# - Try to find the Open ssl library (ssl)
#
# Once done this will define
#
#  SSL_FOUND - System has gnutls
#  SSL_INCLUDE_DIR - The gnutls include directory
#  SSL_LIBRARIES - The libraries needed to use gnutls
#  SSL_DEFINITIONS - Compiler switches required for using gnutls


IF (SSL_INCLUDE_DIR AND SSL_LIBRARIES)
	# in cache already
	SET(SSL_FIND_QUIETLY TRUE)
ENDIF (SSL_INCLUDE_DIR AND SSL_LIBRARIES)

FIND_PATH(SSL_INCLUDE_DIR openssl/opensslv.h)

FIND_LIBRARY(SSL_LIBRARIES crypto)

INCLUDE(FindPackageHandleStandardArgs)

# handle the QUIETLY and REQUIRED arguments and set SSL_FOUND to TRUE if
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SSL DEFAULT_MSG SSL_LIBRARIES SSL_INCLUDE_DIR)

MARK_AS_ADVANCED(SSL_INCLUDE_DIR SSL_LIBRARIES)