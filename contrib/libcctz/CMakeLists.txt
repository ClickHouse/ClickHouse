include_directories (include)

if (CMAKE_SYSTEM MATCHES "FreeBSD")
	#yes, need linux, bacause bsd check inside linux in time_zone_libc.cc:24
	add_definitions (-D__USE_BSD -Dlinux -D_XOPEN_SOURCE=600)
endif ()

add_library(cctz
    src/time_zone_libc.cc
    src/time_zone_posix.cc
    src/time_zone_lookup.cc
    src/time_zone_info.cc
    src/time_zone_if.cc
    src/time_zone_format.cc
    src/time_zone_impl.cc

    src/time_zone_libc.h
    src/time_zone_if.h
    src/tzfile.h
    src/time_zone_impl.h
    src/time_zone_posix.h
    src/time_zone_info.h

    include/time_zone.h
    include/civil_time_detail.h
    include/civil_time.h)
