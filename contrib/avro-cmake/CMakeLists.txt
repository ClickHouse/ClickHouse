# Needed when using Apache Avro serialization format
option (ENABLE_AVRO "Enable Avro" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AVRO)
    message(STATUS "Not using avro")
    return()
endif()

set(AVROCPP_ROOT_DIR "${PROJECT_SOURCE_DIR}/contrib/avro/lang/c++")
set(AVROCPP_INCLUDE_DIR "${AVROCPP_ROOT_DIR}/api")
set(AVROCPP_SOURCE_DIR "${AVROCPP_ROOT_DIR}/impl")

set (CMAKE_CXX_STANDARD 17)

if (EXISTS "${AVROCPP_ROOT_DIR}/../../share/VERSION.txt")
    file(READ "${AVROCPP_ROOT_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif()

string(REPLACE "\n" "" AVRO_VERSION "${AVRO_VERSION}")
set (AVRO_VERSION_MAJOR ${AVRO_VERSION})
set (AVRO_VERSION_MINOR "0")

set (AVROCPP_SOURCE_FILES
    "${AVROCPP_SOURCE_DIR}/Compiler.cc"
    "${AVROCPP_SOURCE_DIR}/Node.cc"
    "${AVROCPP_SOURCE_DIR}/LogicalType.cc"
    "${AVROCPP_SOURCE_DIR}/NodeImpl.cc"
    "${AVROCPP_SOURCE_DIR}/ResolverSchema.cc"
    "${AVROCPP_SOURCE_DIR}/Schema.cc"
    "${AVROCPP_SOURCE_DIR}/Types.cc"
    "${AVROCPP_SOURCE_DIR}/ValidSchema.cc"
    "${AVROCPP_SOURCE_DIR}/Zigzag.cc"
    "${AVROCPP_SOURCE_DIR}/BinaryEncoder.cc"
    "${AVROCPP_SOURCE_DIR}/BinaryDecoder.cc"
    "${AVROCPP_SOURCE_DIR}/Stream.cc"
    "${AVROCPP_SOURCE_DIR}/FileStream.cc"
    "${AVROCPP_SOURCE_DIR}/Generic.cc"
    "${AVROCPP_SOURCE_DIR}/GenericDatum.cc"
    "${AVROCPP_SOURCE_DIR}/DataFile.cc"
    "${AVROCPP_SOURCE_DIR}/parsing/Symbol.cc"
    "${AVROCPP_SOURCE_DIR}/parsing/ValidatingCodec.cc"
    "${AVROCPP_SOURCE_DIR}/parsing/JsonCodec.cc"
    "${AVROCPP_SOURCE_DIR}/parsing/ResolvingDecoder.cc"
    "${AVROCPP_SOURCE_DIR}/json/JsonIO.cc"
    "${AVROCPP_SOURCE_DIR}/json/JsonDom.cc"
    "${AVROCPP_SOURCE_DIR}/Resolver.cc"
    "${AVROCPP_SOURCE_DIR}/Validator.cc"
    )

add_library (_avrocpp ${AVROCPP_SOURCE_FILES})
add_library (ch_contrib::avrocpp ALIAS _avrocpp)
set_target_properties (_avrocpp PROPERTIES VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR})

target_include_directories(_avrocpp SYSTEM PUBLIC ${AVROCPP_INCLUDE_DIR})

target_link_libraries (_avrocpp PRIVATE boost::headers_only boost::iostreams)

target_compile_definitions (_avrocpp PUBLIC SNAPPY_CODEC_AVAILABLE)
target_include_directories (_avrocpp PRIVATE ${SNAPPY_INCLUDE_DIR})
target_link_libraries (_avrocpp PRIVATE ch_contrib::snappy)

# The `bzip2` and `xz` codecs use the Boost.Iostreams filters, which are not part of `boost::iostreams`,
# because Boost is added before the compression libraries.
set (BOOST_IOSTREAMS_SOURCE_DIR "${PROJECT_SOURCE_DIR}/contrib/boost/libs/iostreams/src")

if (TARGET ch_contrib::bzip2)
    target_sources (_avrocpp PRIVATE "${BOOST_IOSTREAMS_SOURCE_DIR}/bzip2.cpp")
    target_compile_definitions (_avrocpp PUBLIC BZIP2_CODEC_AVAILABLE)
    target_link_libraries (_avrocpp PRIVATE ch_contrib::bzip2)
endif ()

if (TARGET ch_contrib::xz)
    target_sources (_avrocpp PRIVATE "${BOOST_IOSTREAMS_SOURCE_DIR}/lzma.cpp")
    target_compile_definitions (_avrocpp PUBLIC XZ_CODEC_AVAILABLE)
    target_compile_definitions (_avrocpp PRIVATE BOOST_IOSTREAMS_LZMA_NO_MULTITHREADED)
    target_link_libraries (_avrocpp PRIVATE ch_contrib::xz)
endif ()
