if (Protobuf_PROTOC_EXECUTABLE)
    option (ENABLE_PARQUET "Enable parquet" ${ENABLE_LIBRARIES})
elseif(ENABLE_PARQUET OR USE_INTERNAL_PARQUET_LIBRARY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use parquet without protoc executable")
endif()

if (NOT ENABLE_PARQUET)
    if(USE_INTERNAL_PARQUET_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use internal parquet with ENABLE_PARQUET=OFF")
    endif()
    message(STATUS "Building without Parquet support")
    return()
endif()

if (NOT OS_FREEBSD) # Freebsd: ../contrib/arrow/cpp/src/arrow/util/bit-util.h:27:10: fatal error: endian.h: No such file or directory
    option(USE_INTERNAL_PARQUET_LIBRARY "Set to FALSE to use system parquet library instead of bundled" ${NOT_UNBUNDLED})
elseif(USE_INTERNAL_PARQUET_LIBRARY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Using internal parquet is not supported on freebsd")
endif()

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/CMakeLists.txt")
    if(USE_INTERNAL_PARQUET_LIBRARY)
        message(WARNING "submodule contrib/arrow (required for Parquet) is missing. to fix try run: \n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal parquet library")
        set(USE_INTERNAL_PARQUET_LIBRARY 0)
    endif()
    set(MISSING_INTERNAL_PARQUET_LIBRARY 1)
endif()

if (NOT SNAPPY_LIBRARY)
    include(cmake/find/snappy.cmake)
endif()

if(NOT USE_INTERNAL_PARQUET_LIBRARY)
    find_package(Arrow)
    find_package(Parquet)
    find_library(THRIFT_LIBRARY thrift)
    find_library(UTF8_PROC_LIBRARY utf8proc)
    find_package(BZip2)

    if(USE_STATIC_LIBRARIES)
        find_library(ARROW_DEPS_LIBRARY arrow_bundled_dependencies)

        if (ARROW_DEPS_LIBRARY)
            set(ARROW_IMPORT_OBJ_DIR "${CMAKE_CURRENT_BINARY_DIR}/contrib/arrow-cmake/imported-objects")
            set(ARROW_OTHER_OBJS
                "${ARROW_IMPORT_OBJ_DIR}/jemalloc.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/arena.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/background_thread.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/base.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/bin.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/bitmap.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/ckh.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/ctl.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/div.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/extent.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/extent_dss.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/extent_mmap.pic.o"
                # skip hash
                "${ARROW_IMPORT_OBJ_DIR}/hook.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/large.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/log.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/malloc_io.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/mutex.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/mutex_pool.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/nstime.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/pages.pic.o"
                # skip prng
                "${ARROW_IMPORT_OBJ_DIR}/prof.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/rtree.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/stats.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/sc.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/sz.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/tcache.pic.o"
                # skip ticker
                "${ARROW_IMPORT_OBJ_DIR}/tsd.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/test_hooks.pic.o"
                "${ARROW_IMPORT_OBJ_DIR}/witness.pic.o"
            )
            add_custom_command(OUTPUT ${ARROW_OTHER_OBJS}
                               COMMAND
                                   mkdir -p "${ARROW_IMPORT_OBJ_DIR}" &&
                                   cd "${ARROW_IMPORT_OBJ_DIR}" &&
                                   "${CMAKE_AR}" x "${ARROW_DEPS_LIBRARY}"
                               )
            set_source_files_properties(jemalloc.pic.o PROPERTIES EXTERNAL_OBJECT true GENERATED true)
            add_library(imported_arrow_deps STATIC ${ARROW_OTHER_OBJS})

            set(ARROW_LIBRARY ${ARROW_STATIC_LIB}
                imported_arrow_deps ${THRIFT_LIBRARY} ${UTF8_PROC_LIBRARY} ${BZIP2_LIBRARIES} ${SNAPPY_LIBRARY})
        else()
            message(WARNING "Using external static Arrow does not always work. "
                    "Could not find arrow_bundled_dependencies.a. If compilation fails, "
                    "Try: -D\"USE_INTERNAL_PARQUET_LIBRARY\"=ON or -D\"ENABLE_PARQUET\"=OFF or "
                    "-D\"USE_STATIC_LIBRARIES\"=OFF")
            set(ARROW_LIBRARY ${ARROW_STATIC_LIB})
        endif()
        set(PARQUET_LIBRARY ${PARQUET_STATIC_LIB})
    else()
        set(ARROW_LIBRARY ${ARROW_SHARED_LIB})
        set(PARQUET_LIBRARY ${PARQUET_SHARED_LIB})
    endif()

    if(ARROW_INCLUDE_DIR AND ARROW_LIBRARY AND PARQUET_INCLUDE_DIR AND PARQUET_LIBRARY AND THRIFT_LIBRARY AND UTF8_PROC_LIBRARY AND BZIP2_FOUND)
        set(USE_PARQUET 1)
        set(EXTERNAL_PARQUET_FOUND 1)
    else()
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "Can't find system parquet: arrow=${ARROW_INCLUDE_DIR}:${ARROW_LIBRARY} ;"
                 " parquet=${PARQUET_INCLUDE_DIR}:${PARQUET_LIBRARY} ;"
                 " thrift=${THRIFT_LIBRARY} ;")
        set(EXTERNAL_PARQUET_FOUND 0)
    endif()
endif()

if(NOT EXTERNAL_PARQUET_FOUND AND NOT MISSING_INTERNAL_PARQUET_LIBRARY AND NOT OS_FREEBSD)
    if(SNAPPY_LIBRARY)
        set(CAN_USE_INTERNAL_PARQUET_LIBRARY 1)
    else()
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal parquet library without snappy")
    endif()

    include(CheckCXXSourceCompiles)
    if(NOT USE_INTERNAL_DOUBLE_CONVERSION_LIBRARY)
        set(CMAKE_REQUIRED_LIBRARIES ${DOUBLE_CONVERSION_LIBRARIES})
        set(CMAKE_REQUIRED_INCLUDES ${DOUBLE_CONVERSION_INCLUDE_DIR})
        check_cxx_source_compiles("
               #include <double-conversion/double-conversion.h>
               int main() { static const int flags_ = double_conversion::StringToDoubleConverter::ALLOW_CASE_INSENSIBILITY; return 0;}
        " HAVE_DOUBLE_CONVERSION_ALLOW_CASE_INSENSIBILITY)

        if(NOT HAVE_DOUBLE_CONVERSION_ALLOW_CASE_INSENSIBILITY) # HAVE_STD_RANDOM_SHUFFLE
                message (${RECONFIGURE_MESSAGE_LEVEL} "Disabling internal parquet library because arrow is broken (can't use old double_conversion)")
                set(CAN_USE_INTERNAL_PARQUET_LIBRARY 0)
        endif()
    endif()

    if(NOT CAN_USE_INTERNAL_PARQUET_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal parquet")
        set(USE_INTERNAL_PARQUET_LIBRARY 0)
    else()
    set(USE_INTERNAL_PARQUET_LIBRARY 1)

    if(MAKE_STATIC_LIBRARIES)
        set(FLATBUFFERS_LIBRARY flatbuffers)
        set(ARROW_LIBRARY arrow_static)
        set(PARQUET_LIBRARY parquet_static)
        set(THRIFT_LIBRARY thrift_static)
    else()
        set(FLATBUFFERS_LIBRARY flatbuffers_shared)
        set(ARROW_LIBRARY arrow_shared)
        set(PARQUET_LIBRARY parquet_shared)
        set(THRIFT_LIBRARY thrift)
    endif()

    set(USE_PARQUET 1)
    set(USE_ORC 1)
    set(USE_ARROW 1)
   endif()
elseif(OS_FREEBSD)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Using internal parquet library on FreeBSD is not supported")
endif()

if(USE_PARQUET)
    message(STATUS "Using Parquet: arrow=${ARROW_LIBRARY}:${ARROW_INCLUDE_DIR} ;"
            " parquet=${PARQUET_LIBRARY}:${PARQUET_INCLUDE_DIR} ;"
            " thrift=${THRIFT_LIBRARY} ;"
            " flatbuffers=${FLATBUFFERS_LIBRARY}")
else()
    message(STATUS "Building without Parquet support")
endif()
