set(CPPKAFKA_DIR ${CMAKE_SOURCE_DIR}/contrib/cppkafka)

set(SRCS
  ${CPPKAFKA_DIR}/src/configuration.cpp
  ${CPPKAFKA_DIR}/src/topic_configuration.cpp
  ${CPPKAFKA_DIR}/src/configuration_option.cpp
  ${CPPKAFKA_DIR}/src/exceptions.cpp
  ${CPPKAFKA_DIR}/src/topic.cpp
  ${CPPKAFKA_DIR}/src/buffer.cpp
  ${CPPKAFKA_DIR}/src/queue.cpp
  ${CPPKAFKA_DIR}/src/message.cpp
  ${CPPKAFKA_DIR}/src/message_timestamp.cpp
  ${CPPKAFKA_DIR}/src/message_internal.cpp
  ${CPPKAFKA_DIR}/src/topic_partition.cpp
  ${CPPKAFKA_DIR}/src/topic_partition_list.cpp
  ${CPPKAFKA_DIR}/src/metadata.cpp
  ${CPPKAFKA_DIR}/src/group_information.cpp
  ${CPPKAFKA_DIR}/src/error.cpp
  ${CPPKAFKA_DIR}/src/event.cpp

  ${CPPKAFKA_DIR}/src/kafka_handle_base.cpp
  ${CPPKAFKA_DIR}/src/producer.cpp
  ${CPPKAFKA_DIR}/src/consumer.cpp
)

add_library(cppkafka ${SRCS})

target_link_libraries(cppkafka PRIVATE ${RDKAFKA_LIBRARY})
target_include_directories(cppkafka PRIVATE ${CPPKAFKA_DIR}/include/cppkafka)
target_include_directories(cppkafka PRIVATE ${Boost_INCLUDE_DIRS})
target_include_directories(cppkafka SYSTEM PUBLIC ${CPPKAFKA_DIR}/include)
