if (NOT NO_WERROR)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

add_subdirectory (libcommon)
add_subdirectory (libpocoext)
add_subdirectory (libzkutil)
add_subdirectory (libdaemon)

if (MYSQL_FOUND)
    add_subdirectory (libmysqlxx)
endif ()
