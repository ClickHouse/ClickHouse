set(PDX_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/pdx")

add_library(_pdx INTERFACE)
target_include_directories(_pdx SYSTEM INTERFACE
    ${PDX_PROJECT_DIR}/include
)

add_library(ch_contrib::pdx ALIAS _pdx)
