# project and source dir
set(AVROCPP_ROOT_DIR ${CMAKE_SOURCE_DIR}/contrib/avro/lang/c++)
set(AVROCPP_INCLUDE_DIR ${AVROCPP_ROOT_DIR}/api)
set(AVROCPP_SOURCE_DIR ${AVROCPP_ROOT_DIR}/impl)

#set(AVROCPP_COMMON_DIR ${HDFS3_SOURCE_DIR}/common)

if (EXISTS ${AVROCPP_ROOT_DIR}/../../share/VERSION.txt)
    file(READ "${AVROCPP_ROOT_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif()

string(REPLACE "\n" "" AVRO_VERSION  ${AVRO_VERSION})
set (AVRO_VERSION_MAJOR ${AVRO_VERSION})
set (AVRO_VERSION_MINOR "0")

set (AVROCPP_SOURCE_FILES
    ${AVROCPP_SOURCE_DIR}/Compiler.cc
    ${AVROCPP_SOURCE_DIR}/Node.cc
    ${AVROCPP_SOURCE_DIR}/LogicalType.cc
    ${AVROCPP_SOURCE_DIR}/NodeImpl.cc
    ${AVROCPP_SOURCE_DIR}/ResolverSchema.cc
    ${AVROCPP_SOURCE_DIR}/Schema.cc
    ${AVROCPP_SOURCE_DIR}/Types.cc
    ${AVROCPP_SOURCE_DIR}/ValidSchema.cc
    ${AVROCPP_SOURCE_DIR}/Zigzag.cc
    ${AVROCPP_SOURCE_DIR}/BinaryEncoder.cc
    ${AVROCPP_SOURCE_DIR}/BinaryDecoder.cc
    ${AVROCPP_SOURCE_DIR}/Stream.cc
    ${AVROCPP_SOURCE_DIR}/FileStream.cc
    ${AVROCPP_SOURCE_DIR}/Generic.cc
    ${AVROCPP_SOURCE_DIR}/GenericDatum.cc
    ${AVROCPP_SOURCE_DIR}/DataFile.cc
    ${AVROCPP_SOURCE_DIR}/parsing/Symbol.cc
    ${AVROCPP_SOURCE_DIR}/parsing/ValidatingCodec.cc
    ${AVROCPP_SOURCE_DIR}/parsing/JsonCodec.cc
    ${AVROCPP_SOURCE_DIR}/parsing/ResolvingDecoder.cc
    ${AVROCPP_SOURCE_DIR}/json/JsonIO.cc
    ${AVROCPP_SOURCE_DIR}/json/JsonDom.cc
    ${AVROCPP_SOURCE_DIR}/Resolver.cc
    ${AVROCPP_SOURCE_DIR}/Validator.cc
    )


add_definitions(-std=c++17 -fPIC)

add_library (avrocpp SHARED ${AVROCPP_SOURCE_FILES})

set_property (TARGET avrocpp
    APPEND PROPERTY COMPILE_DEFINITIONS AVRO_DYN_LINK)

add_library (avrocpp_s STATIC ${AVROCPP_SOURCE_FILES})

set_property (TARGET avrocpp avrocpp_s
    APPEND PROPERTY COMPILE_DEFINITIONS AVRO_SOURCE)

set_target_properties (avrocpp PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

set_target_properties (avrocpp_s PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

target_link_libraries (avrocpp ${Boost_IOSTREAMS_LIBRARY} ${SNAPPY_LIBRARY})
target_link_libraries (avrocpp_s ${Boost_IOSTREAMS_LIBRARY} ${SNAPPY_LIBRARY})

target_compile_definitions (avrocpp PUBLIC SNAPPY_CODEC_AVAILABLE)
target_compile_definitions (avrocpp_s PUBLIC SNAPPY_CODEC_AVAILABLE)

include_directories(${AVROCPP_INCLUDE_DIR})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${SNAPPY_INCLUDE_DIR})

ADD_CUSTOM_TARGET(symlink_headers ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory ${AVROCPP_ROOT_DIR}/include
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${AVROCPP_ROOT_DIR}/api ${AVROCPP_ROOT_DIR}/include/avro
)