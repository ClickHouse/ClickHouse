option (ENABLE_FLATBUFFERS "Enable flatbuffers library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_FLATBUFFERS)
    message(STATUS "Not using flatbuffers")
    return()
endif()

#[[set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/flatbuffers")
set(INCLUDE_DIR "${LIBRARY_DIR}/include")
set(SOURCE_DIR "${LIBRARY_DIR}/src")

set(SRCS
        "${INCLUDE_DIR}/flatbuffers/allocator.h"
        "${INCLUDE_DIR}/flatbuffers/array.h"
        "${INCLUDE_DIR}/flatbuffers/base.h"
        "${INCLUDE_DIR}/flatbuffers/buffer.h"
        "${INCLUDE_DIR}/flatbuffers/buffer_ref.h"
        "${INCLUDE_DIR}/flatbuffers/default_allocator.h"
        "${INCLUDE_DIR}/flatbuffers/detached_buffer.h"
        "${INCLUDE_DIR}/flatbuffers/code_generator.h"
        "${INCLUDE_DIR}/flatbuffers/file_manager.h"
        "${INCLUDE_DIR}/flatbuffers/flatbuffer_builder.h"
        "${INCLUDE_DIR}/flatbuffers/flatbuffers.h"
        "${INCLUDE_DIR}/flatbuffers/flexbuffers.h"
        "${INCLUDE_DIR}/flatbuffers/flex_flat_util.h"
        "${INCLUDE_DIR}/flatbuffers/hash.h"
        "${INCLUDE_DIR}/flatbuffers/idl.h"
        "${INCLUDE_DIR}/flatbuffers/minireflect.h"
        "${INCLUDE_DIR}/flatbuffers/reflection.h"
        "${INCLUDE_DIR}/flatbuffers/reflection_generated.h"
        "${INCLUDE_DIR}/flatbuffers/registry.h"
        "${INCLUDE_DIR}/flatbuffers/stl_emulation.h"
        "${INCLUDE_DIR}/flatbuffers/string.h"
        "${INCLUDE_DIR}/flatbuffers/struct.h"
        "${INCLUDE_DIR}/flatbuffers/table.h"
        "${INCLUDE_DIR}/flatbuffers/util.h"
        "${INCLUDE_DIR}/flatbuffers/vector.h"
        "${INCLUDE_DIR}/flatbuffers/vector_downward.h"
        "${INCLUDE_DIR}/flatbuffers/verifier.h"
        "${SOURCE_DIR}/idl_parser.cpp"
        "${SOURCE_DIR}/idl_gen_text.cpp"
        "${SOURCE_DIR}/reflection.cpp"
        "${SOURCE_DIR}/util.cpp"
        )


add_library(_flatbuffers ${SRCS})
target_include_directories(_cbor SYSTEM PUBLIC "${INCLUDE_DIR}")]]
add_library(ch_contrib::flatbuffers ALIAS _flatbuffers)