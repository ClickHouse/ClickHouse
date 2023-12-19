option (ENABLE_MAXMINDDB "Enable maxminddb library" OFF)

if (NOT ENABLE_MAXMINDDB)
  message (STATUS "Not using maxminddb")
  return()
endif()

set(LIBMAXMINDDB_VERSION "1.7.1")

include(GNUInstallDirs)
include(CheckTypeSize)
check_type_size("unsigned __int128" UINT128)
check_type_size("unsigned int __attribute__((mode(TI)))" UINT128_USING_MODE)
if(HAVE_UINT128)
  set(MMDB_UINT128_USING_MODE 0)
  set(MMDB_UINT128_IS_BYTE_ARRAY 0)
elseif(HAVE_UINT128_USING_MODE)
  set(MMDB_UINT128_USING_MODE 1)
  set(MMDB_UINT128_IS_BYTE_ARRAY 0)
else()
  set(MMDB_UINT128_USING_MODE 0)
  set(MMDB_UINT128_IS_BYTE_ARRAY 1)
endif()

include (TestBigEndian)
TEST_BIG_ENDIAN(IS_BIG_ENDIAN)

if (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

set(LIBMAXMINDDB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libmaxminddb")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/maxminddb_config.h.in 
               ${CMAKE_CURRENT_BINARY_DIR}/include/maxminddb_config.h)
configure_file(
    "${LIBMAXMINDDB_SOURCE_DIR}/include/maxminddb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/include/maxminddb.h"
    COPYONLY
)

add_library(_maxminddb
  ${LIBMAXMINDDB_SOURCE_DIR}/src/maxminddb.c
  ${LIBMAXMINDDB_SOURCE_DIR}/src/data-pool.c
)
add_library(ch_contrib::maxminddb ALIAS _maxminddb)

target_compile_definitions(_maxminddb PRIVATE PACKAGE_VERSION="${LIBMAXMINDDB_VERSION}")

if(NOT IS_BIG_ENDIAN)
  target_compile_definitions(_maxminddb PRIVATE MMDB_LITTLE_ENDIAN=1)
endif()

if(MSVC)
  target_compile_definitions(_maxminddb PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()

if(WIN32)
  target_link_libraries(_maxminddb ws2_32)
  if(MSVC_STATIC_RUNTIME)
    # On MSVC, when MSVC_STATIC_RUNTIME is ON, MT (Release) and MTd (Debug)
    # run-time libraries will be used instead of MD/MDd. The default is OFF so
    # MD/MDd are used when nothing related is passed.
    #
    # Adapted from https://gitlab.kitware.com/cmake/community/-/wikis/FAQ#make-override-files
    if(MSVC)
      target_compile_options(my_target PRIVATE
        $<$<CONFIG:Debug>:/D_DEBUG /MTd /Zi /Ob0 /Od /RTC1>
        $<$<CONFIG:MinSizeRel>:/MT /O1 /Ob1 /D NDEBUG>
        $<$<CONFIG:Release>:/MT /O2 /Ob2 /D NDEBUG>
        $<$<CONFIG:RelWithDebInfo>:/MT /Zi /O2 /Ob1 /D NDEBUG>
      )
    endif()
  endif()
endif()

target_include_directories(_maxminddb PUBLIC
  ${LIBMAXMINDDB_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}/include
)

if(NOT MSVC)
  add_executable(mmdblookup
    ${LIBMAXMINDDB_SOURCE_DIR}/bin/mmdblookup.c
  )
  target_compile_definitions(mmdblookup PRIVATE PACKAGE_VERSION="${LIBMAXMINDDB_VERSION}")

  # Otherwise 'undefined reference to WinMain' linker error happen due to wmain()
  if(MINGW)
    target_link_options(mmdblookup PRIVATE "-municode")
  endif()

  target_link_libraries(mmdblookup _maxminddb pthread)
endif()

