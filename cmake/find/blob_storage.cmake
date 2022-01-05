option (ENABLE_AZURE_BLOB_STORAGE "Enable Azure blob storage" ${ENABLE_LIBRARIES})

if (ENABLE_AZURE_BLOB_STORAGE)
    option(USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY
        "Set to FALSE to use system Azure SDK instead of bundled (OFF currently not implemented)"
        ON)

    set(USE_AZURE_BLOB_STORAGE 1)
    set(AZURE_BLOB_STORAGE_LIBRARY azure_sdk)

    if ((NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/azure/sdk"
            OR NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/azure/cmake-modules")
            AND USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY)
        message (WARNING "submodule contrib/azure is missing. to fix try run: \n git submodule update --init")
        set(USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY OFF)
        set(USE_AZURE_BLOB_STORAGE 0)
    endif ()

    if (NOT USE_INTERNAL_SSL_LIBRARY AND USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY)
        message (FATAL_ERROR "Currently Blob Storage support can be built only with internal SSL library")
    endif()

    if (NOT USE_INTERNAL_CURL AND USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY)
        message (FATAL_ERROR "Currently Blob Storage support can be built only with internal curl library")
    endif()

endif()

message (STATUS "Using Azure Blob Storage - ${USE_AZURE_BLOB_STORAGE}")
