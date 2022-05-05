add_library(_openmp SHARED IMPORTED GLOBAL)
set_target_properties(_openmp
    PROPERTIES IMPORTED_LOCATION "${ClickHouse_SOURCE_DIR}/contrib/openmp/libomp.so.5"
)
target_include_directories(_openmp INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/openmp/include")
add_library(ch_contrib::openmp ALIAS _openmp)
