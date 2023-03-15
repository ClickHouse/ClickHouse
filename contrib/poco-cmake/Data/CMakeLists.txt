set (SRCS
    "${LIBRARY_DIR}/Data/src/AbstractBinder.cpp"
    "${LIBRARY_DIR}/Data/src/AbstractBinding.cpp"
    "${LIBRARY_DIR}/Data/src/AbstractExtraction.cpp"
    "${LIBRARY_DIR}/Data/src/AbstractExtractor.cpp"
    "${LIBRARY_DIR}/Data/src/AbstractPreparation.cpp"
    "${LIBRARY_DIR}/Data/src/AbstractPreparator.cpp"
    "${LIBRARY_DIR}/Data/src/ArchiveStrategy.cpp"
    "${LIBRARY_DIR}/Data/src/Bulk.cpp"
    "${LIBRARY_DIR}/Data/src/Connector.cpp"
    "${LIBRARY_DIR}/Data/src/DataException.cpp"
    "${LIBRARY_DIR}/Data/src/Date.cpp"
    "${LIBRARY_DIR}/Data/src/DynamicLOB.cpp"
    "${LIBRARY_DIR}/Data/src/Limit.cpp"
    "${LIBRARY_DIR}/Data/src/MetaColumn.cpp"
    "${LIBRARY_DIR}/Data/src/PooledSessionHolder.cpp"
    "${LIBRARY_DIR}/Data/src/PooledSessionImpl.cpp"
    "${LIBRARY_DIR}/Data/src/Position.cpp"
    "${LIBRARY_DIR}/Data/src/Range.cpp"
    "${LIBRARY_DIR}/Data/src/RecordSet.cpp"
    "${LIBRARY_DIR}/Data/src/Row.cpp"
    "${LIBRARY_DIR}/Data/src/RowFilter.cpp"
    "${LIBRARY_DIR}/Data/src/RowFormatter.cpp"
    "${LIBRARY_DIR}/Data/src/RowIterator.cpp"
    "${LIBRARY_DIR}/Data/src/Session.cpp"
    "${LIBRARY_DIR}/Data/src/SessionFactory.cpp"
    "${LIBRARY_DIR}/Data/src/SessionImpl.cpp"
    "${LIBRARY_DIR}/Data/src/SessionPool.cpp"
    "${LIBRARY_DIR}/Data/src/SessionPoolContainer.cpp"
    "${LIBRARY_DIR}/Data/src/SimpleRowFormatter.cpp"
    "${LIBRARY_DIR}/Data/src/SQLChannel.cpp"
    "${LIBRARY_DIR}/Data/src/Statement.cpp"
    "${LIBRARY_DIR}/Data/src/StatementCreator.cpp"
    "${LIBRARY_DIR}/Data/src/StatementImpl.cpp"
    "${LIBRARY_DIR}/Data/src/Time.cpp"
    "${LIBRARY_DIR}/Data/src/Transaction.cpp"
)

add_library (_poco_data ${SRCS})
add_library (Poco::Data ALIAS _poco_data)

if (COMPILER_GCC)
    target_compile_options (_poco_data PRIVATE -Wno-deprecated-copy)
endif ()
target_include_directories (_poco_data SYSTEM PUBLIC "${LIBRARY_DIR}/Data/include")
target_link_libraries (_poco_data PUBLIC Poco::Foundation)
