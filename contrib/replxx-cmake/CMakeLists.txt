option (ENABLE_READLINE "Enable readline support" ${ENABLE_LIBRARIES})

if (ENABLE_READLINE)
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
            ${LIBRARY_DIR}/src/replxx.cxx
            ${LIBRARY_DIR}/src/replxx_impl.cxx
            ${LIBRARY_DIR}/src/util.cxx
            ${LIBRARY_DIR}/src/wcwidth.cpp
            ${LIBRARY_DIR}/src/windows.cxx
        )

        add_library(replxx ${SRCS})
        target_include_directories(replxx PUBLIC ${LIBRARY_DIR}/include)
        target_compile_options(replxx PUBLIC -Wno-documentation)
    else ()
        find_library(LIBRARY_REPLXX NAMES replxx replxx-static)
        find_path(INCLUDE_REPLXX replxx.hxx)

        add_library(replxx UNKNOWN IMPORTED)
        set_property(TARGET replxx PROPERTY IMPORTED_LOCATION ${LIBRARY_REPLXX})
        target_include_directories(replxx PUBLIC ${INCLUDE_REPLXX})

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

    target_compile_definitions(replxx PUBLIC USE_REPLXX)

    message (STATUS "Using replxx")
endif ()
