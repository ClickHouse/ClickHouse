
# TODO? Maybe recursive collect on all deps

get_property (dirs1 TARGET dbms PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET clickhouse_common_io PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET common PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET cityhash PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET roaring PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

if (TARGET double-conversion)
    get_property (dirs1 TARGET double-conversion PROPERTY INCLUDE_DIRECTORIES)
    list(APPEND dirs ${dirs1})
endif ()

list(REMOVE_DUPLICATES dirs)
file (WRITE ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "")
foreach (dir ${dirs})
    string (REPLACE "${ClickHouse_SOURCE_DIR}" "." dir "${dir}")
    file (APPEND ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "-I ${dir} ")
endforeach ()
