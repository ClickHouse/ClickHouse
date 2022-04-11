if (NOT ENABLE_KAFKA)
    message(STATUS "Not using librdkafka (skip cppkafka)")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/cppkafka")

set(SRCS
    "${LIBRARY_DIR}/src/buffer.cpp"
    "${LIBRARY_DIR}/src/configuration_option.cpp"
    "${LIBRARY_DIR}/src/configuration.cpp"
    "${LIBRARY_DIR}/src/consumer.cpp"
    "${LIBRARY_DIR}/src/error.cpp"
    "${LIBRARY_DIR}/src/event.cpp"
    "${LIBRARY_DIR}/src/exceptions.cpp"
    "${LIBRARY_DIR}/src/group_information.cpp"
    "${LIBRARY_DIR}/src/kafka_handle_base.cpp"
    "${LIBRARY_DIR}/src/message_internal.cpp"
    "${LIBRARY_DIR}/src/message_timestamp.cpp"
    "${LIBRARY_DIR}/src/message.cpp"
    "${LIBRARY_DIR}/src/metadata.cpp"
    "${LIBRARY_DIR}/src/producer.cpp"
    "${LIBRARY_DIR}/src/queue.cpp"
    "${LIBRARY_DIR}/src/topic_configuration.cpp"
    "${LIBRARY_DIR}/src/topic_partition_list.cpp"
    "${LIBRARY_DIR}/src/topic_partition.cpp"
    "${LIBRARY_DIR}/src/topic.cpp"
)

add_library(_cppkafka ${SRCS})
add_library(ch_contrib::cppkafka ALIAS _cppkafka)

target_link_libraries(_cppkafka
    PRIVATE
        ch_contrib::rdkafka
        boost::headers_only
)
target_include_directories(_cppkafka PRIVATE "${LIBRARY_DIR}/include/cppkafka")
target_include_directories(_cppkafka SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include")
