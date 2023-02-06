# Sources
file(GLOB SRCS_G "src/*.cpp")
POCO_SOURCES_AUTO(SQLITE_SRCS ${SRCS_G})

# Headers
file(GLOB_RECURSE HDRS_G "include/*.h")
POCO_HEADERS_AUTO(SQLITE_SRCS ${HDRS_G})

option(POCO_UNBUNDLED_SQLITE
  "Set to OFF|ON (default is OFF) to control linking sqlite as external" ${POCO_UNBUNDLED})

if (POCO_UNBUNDLED_SQLITE)
	find_package(SQLite3 REQUIRED)
else()
	# sqlite3
	POCO_SOURCES(SQLITE_SRCS sqlite3
		src/sqlite3.c
	)

	POCO_HEADERS(SQLITE_SRCS sqlite3
		src/sqlite3.h
	)
endif()

# Version Resource
if(MSVC AND BUILD_SHARED_LIBS)
	source_group("Resources" FILES ${PROJECT_SOURCE_DIR}/DLLVersion.rc)
	list(APPEND SQLITE_SRCS ${PROJECT_SOURCE_DIR}/DLLVersion.rc)
endif()

add_library(DataSQLite ${SQLITE_SRCS})

add_library(Poco::DataSQLite ALIAS DataSQLite)
set_target_properties(DataSQLite
	PROPERTIES
	VERSION ${SHARED_LIBRARY_VERSION} SOVERSION ${SHARED_LIBRARY_VERSION}
	OUTPUT_NAME PocoDataSQLite
	DEFINE_SYMBOL SQLite_EXPORTS
)
target_link_libraries(DataSQLite PUBLIC Poco::Data)
target_include_directories(DataSQLite
	SYSTEM PUBLIC
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
		$<INSTALL_INTERFACE:include>
	PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if(POCO_UNBUNDLED)
		target_link_libraries(DataSQLite PUBLIC SQLite::SQLite3)
	target_compile_definitions(DataSQLite PUBLIC
		POCO_UNBUNDLED
		SQLITE_THREADSAFE=1
)
else()
	if(WINCE)
		target_compile_definitions(DataSQLite PRIVATE SQLITE_MSVC_LOCALTIME_API)
	endif(WINCE)
	target_compile_definitions(DataSQLite PRIVATE
		SQLITE_THREADSAFE=1
		SQLITE_DISABLE_LFS
		SQLITE_OMIT_UTF16
		SQLITE_OMIT_PROGRESS_CALLBACK
		SQLITE_OMIT_COMPLETE
		SQLITE_OMIT_TCL_VARIABLE
		SQLITE_OMIT_DEPRECATED
	)
endif()

POCO_INSTALL(DataSQLite)
POCO_GENERATE_PACKAGE(DataSQLite)

if(ENABLE_TESTS)
	add_subdirectory(testsuite)
endif()
