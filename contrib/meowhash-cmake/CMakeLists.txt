set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/meowhash")

add_library(meowhash ${LIBRARY_DIR}/meow_hash_x64_aesni.h)
target_include_directories(meowhash SYSTEM PUBLIC ${LIBRARY_DIR})
target_compile_options(meowhash PUBLIC "-maes")
add_library(ch_contrib::meowhash ALIAS meowhash)