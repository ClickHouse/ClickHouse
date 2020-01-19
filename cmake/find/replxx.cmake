option (ENABLE_REPLXX "Enable replxx support" ${ENABLE_LIBRARIES})

if (ENABLE_REPLXX)
    option (USE_INTERNAL_REPLXX "Use internal replxx library" ${NOT_UNBUNDLED})

    if (USE_INTERNAL_REPLXX AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/replxx/README.md")
       message (WARNING "submodule contrib/replxx is missing. to fix try run: \n git submodule update --init --recursive")
       set (USE_INTERNAL_REPLXX 0)
    endif ()

    if (NOT USE_INTERNAL_REPLXX)
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

    set(USE_REPLXX 1)

    message (STATUS "Using replxx")
else ()
    set(USE_REPLXX 0)
endif ()
