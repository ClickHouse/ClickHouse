add_library(_pocketfft INTERFACE)
target_include_directories(_pocketfft INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/pocketfft)
add_library(ch_contrib::pocketfft ALIAS _pocketfft)
