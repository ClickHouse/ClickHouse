# - Find libpq
# Find the native PostgreSQL includes and library
#
#  PGSQL_INCLUDE_DIR - where to find libpq-fe.h, etc.
#  PGSQL_LIBRARIES   - List of libraries when using PGSQL.
#  PGSQL_FOUND       - True if PGSQL found.

MACRO(FIND_PGSQL)
IF (PGSQL_INCLUDE_DIR)
  # Already in cache, be silent
  SET(PostgreSQL_FIND_QUIETLY TRUE)
ENDIF (PGSQL_INCLUDE_DIR)

FIND_PATH(PGSQL_INCLUDE_DIR libpq-fe.h
  $ENV{ProgramFiles}/PostgreSQL/*/include
  $ENV{SystemDrive}/PostgreSQL/*/include
  /usr/local/pgsql/include
  /usr/local/postgresql/include
  /usr/local/include/pgsql
  /usr/local/include/postgresql
  /usr/local/include
  /usr/include/pgsql
  /usr/include/postgresql
  /usr/include
  /usr/pgsql/include
  /usr/postgresql/include
)

SET(PGSQL_NAMES pq libpq)
SET(PGSQL_SEARCH_LIB_PATHS
  ${PGSQL_SEARCH_LIB_PATHS}
  $ENV{ProgramFiles}/PostgreSQL/*/lib
  $ENV{SystemDrive}/PostgreSQL/*/lib
  /usr/local/pgsql/lib
  /usr/local/lib
  /usr/lib
)
FIND_LIBRARY(PGSQL_LIBRARY
  NAMES ${PGSQL_NAMES}
  PATHS ${PGSQL_SEARCH_LIB_PATHS}
)

IF (PGSQL_INCLUDE_DIR AND PGSQL_LIBRARY)
  SET(PGSQL_FOUND TRUE)
  SET( PGSQL_LIBRARIES ${PGSQL_LIBRARY} )
ELSE (PGSQL_INCLUDE_DIR AND PGSQL_LIBRARY)
  SET(PGSQL_FOUND FALSE)
  SET( PGSQL_LIBRARIES )
ENDIF (PGSQL_INCLUDE_DIR AND PGSQL_LIBRARY)

IF (PGSQL_FOUND)
  IF (NOT PostgreSQL_FIND_QUIETLY)
    MESSAGE(STATUS "Found PostgreSQL: ${PGSQL_LIBRARY}")
  ENDIF (NOT PostgreSQL_FIND_QUIETLY)
ELSE (PGSQL_FOUND)
  IF (PostgreSQL_FIND_REQUIRED)
    MESSAGE(STATUS "Looked for PostgreSQL libraries named ${PGSQL_NAMES}.")
    MESSAGE(FATAL_ERROR "Could NOT find PostgreSQL library")
  ENDIF (PostgreSQL_FIND_REQUIRED)
ENDIF (PGSQL_FOUND)

MARK_AS_ADVANCED(
  PGSQL_LIBRARY
  PGSQL_INCLUDE_DIR
)
ENDMACRO(FIND_PGSQL)