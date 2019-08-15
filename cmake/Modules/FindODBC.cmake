# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

#.rst:
# FindMySQL
# -------
#
# Find ODBC Runtime
#
# This will define the following variables::
#
#   ODBC_FOUND           - True if the system has the libraries
#   ODBC_INCLUDE_DIRS    - where to find the headers
#   ODBC_LIBRARIES       - where to find the libraries
#   ODBC_DEFINITIONS     - compile definitons
#
# Hints:
# Set ``ODBC_ROOT_DIR`` to the root directory of an installation.
#
include(FindPackageHandleStandardArgs)

find_package(PkgConfig QUIET)
pkg_check_modules(PC_ODBC QUIET odbc)

if(WIN32)
	get_filename_component(kit_dir "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Windows Kits\\Installed Roots;KitsRoot]" REALPATH)
	get_filename_component(kit81_dir "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Windows Kits\\Installed Roots;KitsRoot81]" REALPATH)
endif()

find_path(ODBC_INCLUDE_DIR
	NAMES sql.h
	HINTS
		${ODBC_ROOT_DIR}/include
		${ODBC_ROOT_INCLUDE_DIRS}
	PATHS
		${PC_ODBC_INCLUDE_DIRS}
		/usr/include
		/usr/local/include
		/usr/local/odbc/include
		/usr/local/iodbc/include
		"C:/Program Files/ODBC/include"
		"C:/Program Files/Microsoft SDKs/Windows/v7.0/include"
		"C:/Program Files/Microsoft SDKs/Windows/v6.0a/include"
		"C:/ODBC/include"
		"${kit_dir}/Include/um"
		"${kit81_dir}/Include/um"
	PATH_SUFFIXES
		odbc
		iodbc
	DOC "Specify the directory containing sql.h."
)

if(NOT ODBC_INCLUDE_DIR AND WIN32)
  set(ODBC_INCLUDE_DIR "")
else()
  set(REQUIRED_INCLUDE_DIR ODBC_INCLUDE_DIR)
endif()

if(WIN32 AND CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(WIN_ARCH x64)
elseif(WIN32 AND CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(WIN_ARCH x86)
endif()

find_library(ODBC_LIBRARY
	NAMES unixodbc iodbc odbc odbc32
	HINTS
		${ODBC_ROOT_DIR}/lib
		${ODBC_ROOT_LIBRARY_DIRS}
	PATHS
		${PC_ODBC_LIBRARY_DIRS}
		/usr/lib
		/usr/local/lib
		/usr/local/odbc/lib
		/usr/local/iodbc/lib
		"C:/Program Files/ODBC/lib"
		"C:/ODBC/lib/debug"
		"C:/Program Files (x86)/Microsoft SDKs/Windows/v7.0A/Lib"
		"${kit81_dir}/Lib/winv6.3/um"
		"${kit_dir}/Lib/win8/um"
	PATH_SUFIXES
		odbc
		${WIN_ARCH}
	DOC "Specify the ODBC driver manager library here."
)

if(NOT ODBC_LIBRARY AND WIN32)
  # List names of ODBC libraries on Windows
  set(ODBC_LIBRARY odbc32.lib)
endif()

# List additional libraries required to use ODBC library
if(WIN32 AND MSVC OR CMAKE_CXX_COMPILER_ID MATCHES "Intel")
  set(_odbc_required_libs_names odbccp32;ws2_32)
endif()
foreach(_lib_name IN LISTS _odbc_required_libs_names)
	find_library(_lib_path
		NAMES ${_lib_name}
		HINTS
			${ODBC_ROOT_DIR}/lib
			${ODBC_ROOT_LIBRARY_DIRS}
		PATHS
			${PC_ODBC_LIBRARY_DIRS}
			/usr/lib
			/usr/local/lib
			/usr/local/odbc/lib
			/usr/local/iodbc/lib
			"C:/Program Files/ODBC/lib"
			"C:/ODBC/lib/debug"
			"C:/Program Files (x86)/Microsoft SDKs/Windows/v7.0A/Lib"
		PATH_SUFFIXES
			odbc
	)
	if (_lib_path)
		list(APPEND _odbc_required_libs_paths ${_lib_path})
	endif()
	unset(_lib_path CACHE)
endforeach()
unset(_odbc_lib_paths)
unset(_odbc_required_libs_names)


find_package_handle_standard_args(ODBC
	FOUND_VAR ODBC_FOUND
	REQUIRED_VARS
		ODBC_LIBRARY
		${REQUIRED_INCLUDE_DIR}
	VERSION_VAR ODBC_VERSION
)

if(ODBC_FOUND)
  set(ODBC_LIBRARIES ${ODBC_LIBRARY} ${_odbc_required_libs_paths})
  set(ODBC_INCLUDE_DIRS ${ODBC_INCLUDE_DIR})
  set(ODBC_DEFINITIONS ${PC_ODBC_CFLAGS_OTHER})
endif()

if(ODBC_FOUND AND NOT TARGET ODBC::ODBC)
  add_library(ODBC::ODBC UNKNOWN IMPORTED)
  set_target_properties(ODBC::ODBC PROPERTIES
	IMPORTED_LOCATION "${ODBC_LIBRARY}"
	INTERFACE_LINK_LIBRARIES "${_odbc_required_libs_paths}"
	INTERFACE_COMPILE_OPTIONS "${PC_ODBC_CFLAGS_OTHER}"
	INTERFACE_INCLUDE_DIRECTORIES "${ODBC_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(ODBC_LIBRARY ODBC_INCLUDE_DIR)
