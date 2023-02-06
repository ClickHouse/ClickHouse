set (SRCS
    src/AbstractBinder.cpp
    src/AbstractBinding.cpp
    src/AbstractExtraction.cpp
    src/AbstractExtractor.cpp
    src/AbstractPreparation.cpp
    src/AbstractPreparator.cpp
    src/ArchiveStrategy.cpp
    src/Bulk.cpp
    src/Connector.cpp
    src/DataException.cpp
    src/Date.cpp
    src/DynamicLOB.cpp
    src/Limit.cpp
    src/MetaColumn.cpp
    src/PooledSessionHolder.cpp
    src/PooledSessionImpl.cpp
    src/Position.cpp
    src/Range.cpp
    src/RecordSet.cpp
    src/Row.cpp
    src/RowFilter.cpp
    src/RowFormatter.cpp
    src/RowIterator.cpp
    src/Session.cpp
    src/SessionFactory.cpp
    src/SessionImpl.cpp
    src/SessionPool.cpp
    src/SessionPoolContainer.cpp
    src/SimpleRowFormatter.cpp
    src/SQLChannel.cpp
    src/Statement.cpp
    src/StatementCreator.cpp
    src/StatementImpl.cpp
    src/Time.cpp
    src/Transaction.cpp
)

add_library (_poco_data ${SRCS})
add_library (Poco::Data ALIAS _poco_data)

target_include_directories (_poco_data SYSTEM PUBLIC "include")
target_link_libraries (_poco_data PUBLIC Poco::Foundation)
