add_library(aquahash ${ClickHouse_SOURCE_DIR}/contrib/aquahash/aquahash.h)
target_compile_options(aquahash PUBLIC "-maes")
add_library(ch_contrib::aquahash ALIAS aquahash)
