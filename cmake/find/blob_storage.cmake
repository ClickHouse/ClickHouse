option(USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY "Set to FALSE to use system S3 instead of bundled (OFF currently not implemented)"
    ON)

if (USE_INTERNAL_AZURE_BLOB_STORAGE_LIBRARY)
    set(USE_AZURE_BLOB_STORAGE 1)
    set(AZURE_BLOB_STORAGE_LIBRARY azure_sdk)
endif()

message (STATUS "Using Azure Blob Storage - ${USE_AZURE_BLOB_STORAGE}")
