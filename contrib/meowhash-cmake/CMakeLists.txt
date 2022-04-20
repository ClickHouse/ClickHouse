SET(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/meowhash")

add_library(meowhash "${LIBRARY_DIR}/meow_hash_x64_aesni.h")

target_include_directories(meowhash PUBLIC ${LIBRARY_DIR})

add_library(ch_contrib::meowhash ALIAS meowhash)
