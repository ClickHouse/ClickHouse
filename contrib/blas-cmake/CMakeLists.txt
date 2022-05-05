add_library(_blas SHARED IMPORTED GLOBAL)
set_target_properties(_blas
    PROPERTIES IMPORTED_LOCATION "${ClickHouse_SOURCE_DIR}/contrib/blas/libblas.so"
)
add_library(ch_contrib::blas ALIAS _blas)
