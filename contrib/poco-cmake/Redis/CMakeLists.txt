set (SRCS
    "${LIBRARY_DIR}/Redis/src/Array.cpp"
    "${LIBRARY_DIR}/Redis/src/AsyncReader.cpp"
    "${LIBRARY_DIR}/Redis/src/Client.cpp"
    "${LIBRARY_DIR}/Redis/src/Command.cpp"
    "${LIBRARY_DIR}/Redis/src/Error.cpp"
    "${LIBRARY_DIR}/Redis/src/Exception.cpp"
    "${LIBRARY_DIR}/Redis/src/RedisEventArgs.cpp"
    "${LIBRARY_DIR}/Redis/src/RedisStream.cpp"
    "${LIBRARY_DIR}/Redis/src/Type.cpp"
)

add_library (_poco_redis ${SRCS})
add_library (Poco::Redis ALIAS _poco_redis)

if (COMPILER_GCC)
    target_compile_options (_poco_redis PRIVATE -Wno-deprecated-copy)
endif ()
target_compile_options (_poco_redis PRIVATE -Wno-shadow)
target_include_directories (_poco_redis SYSTEM PUBLIC "${LIBRARY_DIR}/Redis/include")
target_link_libraries (_poco_redis PUBLIC Poco::Net)
