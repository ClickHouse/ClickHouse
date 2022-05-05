add_library(_lapack SHARED IMPORTED GLOBAL)
set_target_properties(_lapack
    PROPERTIES IMPORTED_LOCATION "${ClickHouse_SOURCE_DIR}/contrib/lapack/liblapack.so"
)
add_library(ch_contrib::lapack ALIAS _lapack)
