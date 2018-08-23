SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cctz)

add_library(cctz ${SPLIT_SHARED}
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

    ${LIBRARY_DIR}/src/time_zone_libc.h
    ${LIBRARY_DIR}/src/time_zone_if.h
    ${LIBRARY_DIR}/src/tzfile.h
    ${LIBRARY_DIR}/src/time_zone_impl.h
    ${LIBRARY_DIR}/src/time_zone_posix.h
    ${LIBRARY_DIR}/src/time_zone_info.h

    ${LIBRARY_DIR}/include/cctz/time_zone.h
    ${LIBRARY_DIR}/include/cctz/civil_time_detail.h
    ${LIBRARY_DIR}/include/cctz/civil_time.h)

if (CMAKE_SYSTEM MATCHES "FreeBSD")
    # yes, need linux, because bsd check inside linux in time_zone_libc.cc:24
    target_compile_definitions (cctz PRIVATE __USE_BSD linux _XOPEN_SOURCE=600)
endif ()

target_include_directories (cctz PUBLIC ${LIBRARY_DIR}/include)
