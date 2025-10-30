option(USE_MONGODB "Enable MongoDB support" ${ENABLE_LIBRARIES})

if(NOT USE_MONGODB)
    message(STATUS "Not using mongocxx and bsoncxx")
    return()
endif()

set(BSONCXX_SOURCES_DIR "${ClickHouse_SOURCE_DIR}/contrib/mongo-cxx-driver/src/bsoncxx")
set(BSONCXX_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/mongo-cxx-driver/src/bsoncxx")

include(GenerateExportHeader)

set(BSONCXX_SOURCES
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/private/itoa.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v1/detail/postlude.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v1/detail/prelude.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/array/element.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/array/value.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/array/view.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/builder/core.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/decimal128.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/document/element.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/document/value.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/document/view.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/exception/error_code.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/exception/exception.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/json.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/oid.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/string/view_or_value.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/types.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/types/bson_value/value.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/types/bson_value/view.cpp
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi/bsoncxx/validate.cpp
)

set(BSONCXX_POLY_USE_IMPLS ON)

configure_file(
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v1/config/config.hpp.in
        ${BSONCXX_BINARY_DIR}/lib/bsoncxx/v1/config/config.hpp
)
configure_file(
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v1/config/version.hpp.in
        ${BSONCXX_BINARY_DIR}/lib/bsoncxx/v1/config/version.hpp
)
configure_file(
        ${BSONCXX_SOURCES_DIR}/lib/bsoncxx/private/config/config.hh.in
        ${BSONCXX_BINARY_DIR}/lib/bsoncxx/private/config/config.hh
)

add_library(_bsoncxx ${BSONCXX_SOURCES})
add_library(ch_contrib::bsoncxx ALIAS _bsoncxx)
target_include_directories(_bsoncxx SYSTEM PUBLIC
        "${BSONCXX_SOURCES_DIR}/include"
        "${BSONCXX_SOURCES_DIR}/include/bsoncxx/v_noabi"
        "${BSONCXX_SOURCES_DIR}/lib"
        "${BSONCXX_SOURCES_DIR}/lib/bsoncxx/v_noabi"

        "${BSONCXX_BINARY_DIR}/lib"
        "${BSONCXX_BINARY_DIR}/lib/bsoncxx/v_noabi"
)
target_link_libraries(_bsoncxx ch_contrib::libbson)

# Taken from mongo-cxx-driver/src/bsoncxx/CMakeLists.txt
set(bsoncxx_export_header_custom_content "")
string(APPEND bsoncxx_export_header_custom_content [[

#undef BSONCXX_DEPRECATED_EXPORT
#undef BSONCXX_DEPRECATED_NO_EXPORT

#if defined(_MSC_VER)
#define BSONCXX_ABI_CDECL __cdecl
#else
#define BSONCXX_ABI_CDECL
#endif

#define BSONCXX_ABI_EXPORT_CDECL(...) BSONCXX_ABI_EXPORT __VA_ARGS__ BSONCXX_ABI_CDECL

]]
)
generate_export_header(_bsoncxx
        BASE_NAME BSONCXX_ABI
        EXPORT_MACRO_NAME BSONCXX_ABI_EXPORT
        DEPRECATED_MACRO_NAME BSONCXX_DEPRECATED
        EXPORT_FILE_NAME ${BSONCXX_BINARY_DIR}/lib/bsoncxx/v_noabi/bsoncxx/v1/config/export.hpp
        STATIC_DEFINE BSONCXX_STATIC
        CUSTOM_CONTENT_FROM_VARIABLE bsoncxx_export_header_custom_content
)


set(MONGOCXX_SOURCES_DIR "${ClickHouse_SOURCE_DIR}/contrib/mongo-cxx-driver/src/mongocxx")
set(MONGOCXX_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/mongo-cxx-driver/src/mongocxx")
set(MONGOCXX_SOURCES
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/private/bson.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/private/conversions.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/private/mongoc.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/private/numeric_casting.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v1/detail/postlude.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v1/detail/prelude.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/bulk_write.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/change_stream.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/client.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/client_encryption.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/client_session.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/collection.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/cursor.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/database.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/command_failed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/command_started_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/command_succeeded_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/heartbeat_failed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/heartbeat_started_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/heartbeat_succeeded_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/server_changed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/server_closed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/server_description.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/server_opening_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/topology_changed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/topology_closed_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/topology_description.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/events/topology_opening_event.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/bulk_write_exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/error_code.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/gridfs_exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/logic_error.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/operation_exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/query_exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/server_error_code.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/exception/write_exception.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/gridfs/bucket.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/gridfs/downloader.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/gridfs/uploader.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/hint.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/index_model.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/index_view.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/instance.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/logger.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/delete_many.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/delete_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/insert_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/replace_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/update_many.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/update_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/model/write.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/aggregate.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/apm.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/auto_encryption.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/bulk_write.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/change_stream.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/client.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/client_encryption.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/client_session.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/count.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/data_key.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/delete.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/distinct.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/encrypt.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/estimated_document_count.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/find.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/find_one_and_delete.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/find_one_and_replace.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/find_one_and_update.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/gridfs/bucket.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/gridfs/upload.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/index.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/index_view.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/insert.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/pool.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/range.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/replace.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/rewrap_many_datakey.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/server_api.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/tls.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/transaction.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/options/update.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/pipeline.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/pool.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/read_concern.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/read_preference.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/bulk_write.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/delete.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/gridfs/upload.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/insert_many.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/insert_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/replace_one.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/rewrap_many_datakey.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/result/update.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/search_index_model.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/search_index_view.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/uri.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/validation_criteria.cpp
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi/mongocxx/write_concern.cpp
)

set(MONGOCXX_COMPILER_VERSION "${CMAKE_CXX_COMPILER_VERSION}")
set(MONGOCXX_COMPILER_ID "${CMAKE_CXX_COMPILER_ID}")

if(ENABLE_SSL)
    set(MONGOCXX_ENABLE_SSL 1)
endif()

set(BSONCXX_STATIC 1)
set(MONGOCXX_STATIC 1)

configure_file(
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v1/config/config.hpp.in
        ${MONGOCXX_BINARY_DIR}/lib/mongocxx/v1/config/config.hpp
)
configure_file(
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v1/config/version.hpp.in
        ${MONGOCXX_BINARY_DIR}/lib/mongocxx/v1/config/version.hpp
)
configure_file(
        ${MONGOCXX_SOURCES_DIR}/lib/mongocxx/private/config/config.hh.in
        ${MONGOCXX_BINARY_DIR}/lib/mongocxx/private/config/config.hh
)

add_library(_mongocxx ${MONGOCXX_SOURCES})
add_library(ch_contrib::mongocxx ALIAS _mongocxx)
target_include_directories(_mongocxx SYSTEM PUBLIC
        "${MONGOCXX_SOURCES_DIR}/include"
        "${MONGOCXX_SOURCES_DIR}/include/mongocxx/v_noabi"
        "${MONGOCXX_SOURCES_DIR}/lib"
        "${MONGOCXX_SOURCES_DIR}/lib/mongocxx/v_noabi"

        "${MONGOCXX_BINARY_DIR}/lib"
        "${MONGOCXX_BINARY_DIR}/lib/mongocxx/v_noabi"
)
target_link_libraries(_mongocxx ch_contrib::bsoncxx ch_contrib::libmongoc)

# Taken from mongo-cxx-driver/src/mongocxx/CMakeLists.txt
set(mongocxx_export_header_custom_content "")
string(APPEND mongocxx_export_header_custom_content [[

#undef MONGOCXX_DEPRECATED_EXPORT
#undef MONGOCXX_DEPRECATED_NO_EXPORT

#if defined(_MSC_VER)
#define MONGOCXX_ABI_CDECL __cdecl
#else
#define MONGOCXX_ABI_CDECL
#endif

#define MONGOCXX_ABI_EXPORT_CDECL(...) MONGOCXX_ABI_EXPORT __VA_ARGS__ MONGOCXX_ABI_CDECL

]]
)
generate_export_header(_mongocxx
        BASE_NAME MONGOCXX_ABI
        EXPORT_MACRO_NAME MONGOCXX_ABI_EXPORT
        DEPRECATED_MACRO_NAME MONGOCXX_DEPRECATED
        EXPORT_FILE_NAME ${MONGOCXX_BINARY_DIR}/lib/mongocxx/v_noabi/mongocxx/v1/config/export.hpp
        STATIC_DEFINE MONGOCXX_STATIC
        CUSTOM_CONTENT_FROM_VARIABLE mongocxx_export_header_custom_content
)
