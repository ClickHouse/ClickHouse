# 
# Find the ODBC driver manager includes and library.
# 
# ODBC is an open standard for connecting to different databases in a
# semi-vendor-independent fashion.  First you install the ODBC driver
# manager.  Then you need a driver for each separate database you want
# to connect to (unless a generic one works).  VTK includes neither
# the driver manager nor the vendor-specific drivers: you have to find
# those yourself.
#  
# This module defines
# ODBC_INCLUDE_DIRECTORIES, where to find sql.h
# ODBC_LIBRARIES, the libraries to link against to use ODBC
# ODBC_FOUND.  If false, you cannot build anything that requires MySQL.

find_path(ODBC_INCLUDE_DIRECTORIES 
	NAMES sql.h
	HINTS
	/usr/include
	/usr/include/odbc
	/usr/local/include
	/usr/local/include/odbc
	/usr/local/odbc/include
	"C:/Program Files/ODBC/include"
	"C:/Program Files/Microsoft SDKs/Windows/v7.0/include" 
	"C:/Program Files/Microsoft SDKs/Windows/v6.0a/include" 
	"C:/ODBC/include"
	DOC "Specify the directory containing sql.h."
)

find_library(ODBC_LIBRARIES 
	NAMES iodbc odbc odbcinst odbc32
	HINTS
	/usr/lib
	/usr/lib/odbc
	/usr/local/lib
	/usr/local/lib/odbc
	/usr/local/odbc/lib
	"C:/Program Files/ODBC/lib"
	"C:/ODBC/lib/debug"
	"C:/Program Files (x86)/Microsoft SDKs/Windows/v7.0A/Lib"
	DOC "Specify the ODBC driver manager library here."
)

# MinGW find usually fails
if(MINGW)
	set(ODBC_INCLUDE_DIRECTORIES ".")
	set(ODBC_LIBRARIES odbc32)
endif()
	
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ODBC
	DEFAULT_MSG
	ODBC_INCLUDE_DIRECTORIES
	ODBC_LIBRARIES
	)

mark_as_advanced(ODBC_FOUND ODBC_LIBRARIES ODBC_INCLUDE_DIRECTORIES)
