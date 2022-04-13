option(ENABLE_AMQPCPP "Enable AMQP-CPP" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AMQPCPP)
    message(STATUS "Not using AMQP-CPP")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP")

set (SRCS
    "${LIBRARY_DIR}/src/array.cpp"
    "${LIBRARY_DIR}/src/channel.cpp"
    "${LIBRARY_DIR}/src/channelimpl.cpp"
    "${LIBRARY_DIR}/src/connectionimpl.cpp"
    "${LIBRARY_DIR}/src/deferredcancel.cpp"
    "${LIBRARY_DIR}/src/deferredconfirm.cpp"
    "${LIBRARY_DIR}/src/deferredconsumer.cpp"
    "${LIBRARY_DIR}/src/deferredextreceiver.cpp"
    "${LIBRARY_DIR}/src/deferredget.cpp"
    "${LIBRARY_DIR}/src/deferredrecall.cpp"
    "${LIBRARY_DIR}/src/deferredreceiver.cpp"
    "${LIBRARY_DIR}/src/field.cpp"
    "${LIBRARY_DIR}/src/flags.cpp"
    "${LIBRARY_DIR}/src/linux_tcp/openssl.cpp"
    "${LIBRARY_DIR}/src/linux_tcp/sslerrorprinter.cpp"
    "${LIBRARY_DIR}/src/linux_tcp/tcpconnection.cpp"
    "${LIBRARY_DIR}/src/inbuffer.cpp"
    "${LIBRARY_DIR}/src/receivedframe.cpp"
    "${LIBRARY_DIR}/src/table.cpp"
    "${LIBRARY_DIR}/src/watchable.cpp"
)

add_library(_amqp-cpp ${SRCS})

target_compile_options (_amqp-cpp
    PRIVATE
        -Wno-old-style-cast
        -Wno-inconsistent-missing-destructor-override
        -Wno-deprecated
        -Wno-unused-parameter
        -Wno-shadow
        -Wno-tautological-type-limit-compare
        -Wno-extra-semi
# NOTE: disable all warnings at last because the warning:
        #       "conversion function converting 'XXX' to itself will never be used"
        #       doesn't have it's own diagnostic flag yet.
        -w
)

target_include_directories (_amqp-cpp SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include" "${LIBRARY_DIR}")
target_link_libraries (_amqp-cpp PUBLIC OpenSSL::Crypto OpenSSL::SSL ch_contrib::uv)
add_library (ch_contrib::amqp_cpp ALIAS _amqp-cpp)
