option (ENABLE_REPLXX "Enable replxx support" ${ENABLE_LIBRARIES})

if (ENABLE_REPLXX)
    option (USE_INTERNAL_REPLXX "Use internal replxx library" ${NOT_UNBUNDLED})

    if (USE_INTERNAL_REPLXX)
        set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/replxx")

        set(SRCS
            ${LIBRARY_DIR}/src/conversion.cxx
            ${LIBRARY_DIR}/src/ConvertUTF.cpp
            ${LIBRARY_DIR}/src/escape.cxx
            ${LIBRARY_DIR}/src/history.cxx
            ${LIBRARY_DIR}/src/io.cxx
            ${LIBRARY_DIR}/src/prompt.cxx
            ${LIBRARY_DIR}/src/replxx_impl.cxx
            ${LIBRARY_DIR}/src/replxx.cxx
            ${LIBRARY_DIR}/src/util.cxx
            ${LIBRARY_DIR}/src/wcwidth.cpp
        )

        add_library (replxx ${SRCS})
        target_include_directories(replxx SYSTEM PUBLIC ${LIBRARY_DIR}/include)
    else ()
        find_library(LIBRARY_REPLXX NAMES replxx replxx-static)
        find_path(INCLUDE_REPLXX replxx.hxx)

        add_library(replxx UNKNOWN IMPORTED)
        set_property(TARGET replxx PROPERTY IMPORTED_LOCATION ${LIBRARY_REPLXX})
        target_include_directories(replxx SYSTEM PUBLIC ${INCLUDE_REPLXX})

        set(CMAKE_REQUIRED_LIBRARIES replxx)
        check_cxx_source_compiles(
            "
            #include <replxx.hxx>
            int main() {
                replxx::Replxx rx;
            }
            "
            EXTERNAL_REPLXX_WORKS
        )

        if (NOT EXTERNAL_REPLXX_WORKS)
            message (FATAL_ERROR "replxx is unusable: ${LIBRARY_REPLXX} ${INCLUDE_REPLXX}")
        endif ()
    endif ()

    if (COMPILER_CLANG)
        target_compile_options(replxx PRIVATE -Wno-documentation)
    endif ()

    target_compile_definitions(replxx PUBLIC USE_REPLXX=1)

    message (STATUS "Using replxx")
else ()
    add_library(replxx INTERFACE)
    target_compile_definitions(replxx INTERFACE USE_REPLXX=0)

    message (STATUS "Not using replxx (Beware! Runtime fallback to readline is possible!)")
endif ()
