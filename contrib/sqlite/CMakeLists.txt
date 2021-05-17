cmake_minimum_required(VERSION 3.8)
project(SQLite3
        VERSION   3.35.5
        LANGUAGES C
        )

include(GNUInstallDirs)

#------------------------------------------------------------------------------
# build options and optional modules:
option(SQLITE_ENABLE_DBSTAT_VTAB    "enables dbstat virtual table"                    OFF)
option(SQLITE_ENABLE_FTS3           "enables full text searches version 3"            OFF)
option(SQLITE_ENABLE_FTS4           "enables full text searches version 3 & 4"        OFF)
option(SQLITE_ENABLE_FTS5           "enables full text searches version 5"            OFF)
option(SQLITE_ENABLE_GEOPOLY        "enables Geopoly extention"                       OFF)
option(SQLITE_ENABLE_ICU            "enables international components for unicode"    OFF)
option(SQLITE_ENABLE_JSON1          "enables JSON SQL functions"                      ON)
option(SQLITE_ENABLE_MATH_FUNCTIONS "enables the built-in SQL math functions"         ON)
option(SQLITE_ENABLE_RBU            "enables resumable bulk update extension"         OFF)
option(SQLITE_ENABLE_RTREE          "enables R*TRee index extension"                  OFF)
option(SQLITE_ENABLE_STAT4          "enhances query planner under certain situations" OFF)
option(SQLITE_USE_URI               "enables the default URI filename processing"     OFF)
option(SQLITE_RECOMMENDED_OPTIONS   "compile by SQLite3 recommended options"          ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Release or Debug?" FORCE)
endif()

#------------------------------------------------------------------------------

# SQLite3 as static library:
add_library(${PROJECT_NAME} STATIC sqlite3.c)
set_target_properties(${PROJECT_NAME} PROPERTIES
        OUTPUT_NAME   sqlite3
        PUBLIC_HEADER sqlite3.h
        DEBUG_POSTFIX d
        )
target_include_directories(${PROJECT_NAME} PUBLIC $<INSTALL_INTERFACE:include>)
target_compile_definitions(${PROJECT_NAME} PUBLIC # inject user's options
        $<BUILD_INTERFACE:
        $<$<BOOL:${SQLITE_ENABLE_DBSTAT_VTAB}>:SQLITE_ENABLE_DBSTAT_VTAB>
        $<$<BOOL:${SQLITE_ENABLE_FTS3}>:SQLITE_ENABLE_FTS3>
        $<$<BOOL:${SQLITE_ENABLE_FTS4}>:SQLITE_ENABLE_FTS4>
        $<$<BOOL:${SQLITE_ENABLE_FTS5}>:SQLITE_ENABLE_FTS5>
        $<$<BOOL:${SQLITE_ENABLE_GEOPOLY}>:SQLITE_ENABLE_GEOPOLY>
        $<$<BOOL:${SQLITE_ENABLE_ICU}>:SQLITE_ENABLE_ICU>
        $<$<BOOL:${SQLITE_ENABLE_JSON1}>:SQLITE_ENABLE_JSON1>
        $<$<BOOL:${SQLITE_ENABLE_MATH_FUNCTIONS}>:SQLITE_ENABLE_MATH_FUNCTIONS>
        $<$<BOOL:${SQLITE_ENABLE_RBU}>:SQLITE_ENABLE_RBU>
        $<$<BOOL:${SQLITE_ENABLE_RTREE}>:SQLITE_ENABLE_RTREE>
        $<$<BOOL:${SQLITE_ENABLE_STAT4}>:SQLITE_ENABLE_STAT4>
        $<$<BOOL:${SQLITE_USE_URI}>:SQLITE_USE_URI>
        $<$<BOOL:${SQLITE_RECOMMENDED_OPTIONS}>:
        SQLITE_DQS=0
        SQLITE_DEFAULT_MEMSTATUS=0
        SQLITE_DEFAULT_WAL_SYNCHRONOUS=1
        SQLITE_LIKE_DOESNT_MATCH_BLOBS
        SQLITE_MAX_EXPR_DEPTH=0
        SQLITE_OMIT_DECLTYPE
        SQLITE_OMIT_DEPRECATED
        SQLITE_OMIT_PROGRESS_CALLBACK
        SQLITE_OMIT_SHARED_CACHE
        SQLITE_USE_ALLOCA
        >
        >
        )

# platform/compiler specific settings
if(CMAKE_SYSTEM_NAME MATCHES Linux)
    find_package(Threads REQUIRED)
    target_link_libraries(${PROJECT_NAME} INTERFACE Threads::Threads ${CMAKE_DL_LIBS})
elseif(WIN32 AND ${CMAKE_SIZEOF_VOID_P} LESS 8) # this is a 32bit windows
    option(BUILD_WITH_XPSDK "build for old 32bit (WinXP/2003) targets" OFF)
    if(BUILD_WITH_XPSDK)
        target_compile_definitions(${PROJECT_NAME} PUBLIC
                $<BUILD_INTERFACE:
                -DSQLITE_OS_WINRT=0 -D_WIN32_WINNT=0x0502 -DWINVER=0x0502
                >
                )
    endif()
endif()

#------------------------------------------------------------------------------
configure_file(sqlite3_config.h.in ${CMAKE_BINARY_DIR}/sqlite3_config.h)

install(TARGETS ${PROJECT_NAME} EXPORT ${PROJECT_NAME}Config
        ARCHIVE       DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY       DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )
install(EXPORT  ${PROJECT_NAME}Config
        NAMESPACE   SQLite::
        DESTINATION cmake
        )
install(FILES
        ${CMAKE_BINARY_DIR}/sqlite3_config.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        )
