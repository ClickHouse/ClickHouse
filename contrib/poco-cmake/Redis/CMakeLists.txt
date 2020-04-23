if (USE_INTERNAL_POCO_LIBRARY)
    set (SRCS
        ${LIBRARY_DIR}/Redis/src/Array.cpp
        ${LIBRARY_DIR}/Redis/src/AsyncReader.cpp
        ${LIBRARY_DIR}/Redis/src/Client.cpp
        ${LIBRARY_DIR}/Redis/src/Command.cpp
        ${LIBRARY_DIR}/Redis/src/Error.cpp
        ${LIBRARY_DIR}/Redis/src/Exception.cpp
        ${LIBRARY_DIR}/Redis/src/RedisEventArgs.cpp
        ${LIBRARY_DIR}/Redis/src/RedisStream.cpp
        ${LIBRARY_DIR}/Redis/src/Type.cpp
    )

    add_library (poco_redis ${SRCS})

    target_compile_options (poco_redis PUBLIC -Wno-shadow)
    target_include_directories (poco_redis PUBLIC ${LIBRARY_DIR}/Redis/include)
    target_link_libraries (poco_redis PUBLIC poco_net)
endif ()
