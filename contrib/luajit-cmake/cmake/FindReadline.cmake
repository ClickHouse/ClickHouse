# - Try to find Readline
# Once done this will define
#  READLINE_FOUND - System has readline
#  READLINE_INCLUDE_DIRS - The readline include directories
#  READLINE_LIBRARIES - The libraries needed to use readline
#  READLINE_DEFINITIONS - Compiler switches required for using readline

find_package ( PkgConfig )
pkg_check_modules ( PC_READLINE QUIET readline )
set ( READLINE_DEFINITIONS ${PC_READLINE_CFLAGS_OTHER} )

find_path ( READLINE_INCLUDE_DIR readline/readline.h
      HINTS ${PC_READLINE_INCLUDEDIR} ${PC_READLINE_INCLUDE_DIRS}
      PATH_SUFFIXES readline )

find_library ( READLINE_LIBRARY NAMES readline
      HINTS ${PC_READLINE_LIBDIR} ${PC_READLINE_LIBRARY_DIRS} )

set ( READLINE_LIBRARIES ${READLINE_LIBRARY} )
set ( READLINE_INCLUDE_DIRS ${READLINE_INCLUDE_DIR} )

include ( FindPackageHandleStandardArgs )
# handle the QUIETLY and REQUIRED arguments and set READLINE_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args ( readline DEFAULT_MSG READLINE_LIBRARY READLINE_INCLUDE_DIR )
