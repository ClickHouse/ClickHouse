option (USE_INTERNAL_CCTZ "Use internal cctz library" ${NOT_UNBUNDLED})

if (USE_INTERNAL_CCTZ)
    SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cctz)

    SET (SRCS
        ${LIBRARY_DIR}/src/civil_time_detail.cc
        ${LIBRARY_DIR}/src/time_zone_fixed.cc
        ${LIBRARY_DIR}/src/time_zone_format.cc
        ${LIBRARY_DIR}/src/time_zone_if.cc
        ${LIBRARY_DIR}/src/time_zone_impl.cc
        ${LIBRARY_DIR}/src/time_zone_info.cc
        ${LIBRARY_DIR}/src/time_zone_libc.cc
        ${LIBRARY_DIR}/src/time_zone_lookup.cc
        ${LIBRARY_DIR}/src/time_zone_posix.cc
        ${LIBRARY_DIR}/src/zone_info_source.cc
    )

    add_library (cctz ${SRCS})
    target_include_directories (cctz PUBLIC ${LIBRARY_DIR}/include)

    if (OS_FREEBSD)
        # yes, need linux, because bsd check inside linux in time_zone_libc.cc:24
        target_compile_definitions (cctz PRIVATE __USE_BSD linux _XOPEN_SOURCE=600)
    endif ()
else ()
    find_library (LIBRARY_CCTZ cctz)
    find_path (INCLUDE_CCTZ NAMES cctz/civil_time.h)

    add_library (cctz UNKNOWN IMPORTED)
    set_property (TARGET cctz PROPERTY IMPORTED_LOCATION ${LIBRARY_CCTZ})
    set_property (TARGET cctz PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_CCTZ})

    set(CMAKE_REQUIRED_LIBRARIES cctz)
    check_cxx_source_compiles(
        "
        #include <cctz/civil_time.h>
        int main() {
            cctz::civil_day date;
        }
        "
        EXTERNAL_CCTZ_WORKS
    )

    if (NOT EXTERNAL_CCTZ_WORKS)
        message (FATAL_ERROR "cctz is unusable: ${LIBRARY_CCTZ} ${INCLUDE_CCTZ}")
    endif ()
endif ()

message (STATUS "Using cctz")
