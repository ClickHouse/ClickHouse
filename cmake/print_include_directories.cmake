
# TODO? Maybe recursive collect on all deps

get_property (dirs1 TARGET dbms PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

get_property (dirs1 TARGET common PROPERTY INCLUDE_DIRECTORIES)
list(APPEND dirs ${dirs1})

list(REMOVE_DUPLICATES dirs)
file (WRITE ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "")
foreach (dir ${dirs})
    string (REPLACE "${ClickHouse_SOURCE_DIR}" "." dir "${dir}")
    file (APPEND ${CMAKE_CURRENT_BINARY_DIR}/include_directories.txt "-I ${dir} ")
endforeach ()
