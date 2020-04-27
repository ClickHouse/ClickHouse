if (USE_INTERNAL_POCO_LIBRARY)
    set (SRCS
        ${LIBRARY_DIR}/Data/src/AbstractBinder.cpp
        ${LIBRARY_DIR}/Data/src/AbstractBinding.cpp
        ${LIBRARY_DIR}/Data/src/AbstractExtraction.cpp
        ${LIBRARY_DIR}/Data/src/AbstractExtractor.cpp
        ${LIBRARY_DIR}/Data/src/AbstractPreparation.cpp
        ${LIBRARY_DIR}/Data/src/AbstractPreparator.cpp
        ${LIBRARY_DIR}/Data/src/ArchiveStrategy.cpp
        ${LIBRARY_DIR}/Data/src/Bulk.cpp
        ${LIBRARY_DIR}/Data/src/Connector.cpp
        ${LIBRARY_DIR}/Data/src/DataException.cpp
        ${LIBRARY_DIR}/Data/src/Date.cpp
        ${LIBRARY_DIR}/Data/src/DynamicLOB.cpp
        ${LIBRARY_DIR}/Data/src/Limit.cpp
        ${LIBRARY_DIR}/Data/src/MetaColumn.cpp
        ${LIBRARY_DIR}/Data/src/PooledSessionHolder.cpp
        ${LIBRARY_DIR}/Data/src/PooledSessionImpl.cpp
        ${LIBRARY_DIR}/Data/src/Position.cpp
        ${LIBRARY_DIR}/Data/src/Range.cpp
        ${LIBRARY_DIR}/Data/src/RecordSet.cpp
        ${LIBRARY_DIR}/Data/src/Row.cpp
        ${LIBRARY_DIR}/Data/src/RowFilter.cpp
        ${LIBRARY_DIR}/Data/src/RowFormatter.cpp
        ${LIBRARY_DIR}/Data/src/RowIterator.cpp
        ${LIBRARY_DIR}/Data/src/Session.cpp
        ${LIBRARY_DIR}/Data/src/SessionFactory.cpp
        ${LIBRARY_DIR}/Data/src/SessionImpl.cpp
        ${LIBRARY_DIR}/Data/src/SessionPool.cpp
        ${LIBRARY_DIR}/Data/src/SessionPoolContainer.cpp
        ${LIBRARY_DIR}/Data/src/SimpleRowFormatter.cpp
        ${LIBRARY_DIR}/Data/src/SQLChannel.cpp
        ${LIBRARY_DIR}/Data/src/Statement.cpp
        ${LIBRARY_DIR}/Data/src/StatementCreator.cpp
        ${LIBRARY_DIR}/Data/src/StatementImpl.cpp
        ${LIBRARY_DIR}/Data/src/Time.cpp
        ${LIBRARY_DIR}/Data/src/Transaction.cpp
    )

    add_library (poco_data ${SRCS})

    if (COMPILER_GCC)
        target_compile_options (poco_data PUBLIC -Wno-deprecated-copy)
    endif ()
    target_include_directories (poco_data PUBLIC ${LIBRARY_DIR}/Data/include)
    target_link_libraries (poco_data PUBLIC poco_foundation)
else ()
    find_library(LIBRARY_POCO_DATA PocoData)
    find_path(INCLUDE_POCO_DATA Poco/Data/Data.h)

    add_library (poco_data UNKNOWN IMPORTED)
    set_property (TARGET poco_data PROPERTY IMPORTED_LOCATION ${LIBRARY_POCO_DATA})
    set_property (TARGET poco_data PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_POCO_DATA})

    set (CMAKE_REQUIRED_LIBRARIES poco_data)
    check_cxx_source_compiles (
        "
        #include <Poco/Data/Time.h>

        int main() {
            Poco::Data::Time();
        }
        "
        EXTERNAL_POCO_DATA_WORKS
    )

    if (NOT EXTERNAL_POCO_DATA_WORKS)
        message (FATAL_ERROR "Poco::Data is unusable: ${LIBRARY_POCO_DATA} ${INCLUDE_POCO_DATA}")
    endif ()
endif ()
