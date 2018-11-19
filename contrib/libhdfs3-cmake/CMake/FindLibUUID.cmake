# - Find libuuid
# Find the native LIBUUID includes and library
#
#  LIBUUID_INCLUDE_DIRS - where to find uuid/uuid.h, etc.
#  LIBUUID_LIBRARIES    - List of libraries when using uuid.
#  LIBUUID_FOUND        - True if uuid found.

IF (LIBUUID_INCLUDE_DIRS)
  # Already in cache, be silent
  SET(LIBUUID_FIND_QUIETLY TRUE)
ENDIF (LIBUUID_INCLUDE_DIRS)

FIND_PATH(LIBUUID_INCLUDE_DIRS uuid/uuid.h)

SET(LIBUUID_NAMES uuid)
FIND_LIBRARY(LIBUUID_LIBRARIES NAMES ${LIBUUID_NAMES})

# handle the QUIETLY and REQUIRED arguments and set LIBUUID_FOUND to TRUE if 
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LIBUUID DEFAULT_MSG LIBUUID_LIBRARIES LIBUUID_INCLUDE_DIRS)

MARK_AS_ADVANCED(LIBUUID_LIBRARIES LIBUUID_INCLUDE_DIRS)
