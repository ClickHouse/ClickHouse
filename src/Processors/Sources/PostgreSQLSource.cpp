#include <Processors/Sources/PostgreSQLSource.h>
#include <Common/Exception.h>

#if USE_LIBPQXX
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <Common/assert_cast.h>
#include <base/range.h>
#include <Common/logger_useful.h>

#include <future>
#include <Common/ThreadPool.h>
#include <Common/ThreadPool_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
    extern const int POSTGRESQL_CONNECTION_FAILURE;
    extern const int QUERY_WAS_CANCELLED;
}

template<typename T>
PostgreSQLSource<T>::PostgreSQLSource(
    postgres::ConnectionHolderPtr connection_holder_,
    const std::string & query_str_,
    SharedHeader sample_block,
    UInt64 max_block_size_)
    : ISource(std::make_shared<const Block>(sample_block->cloneEmpty()))
    , max_block_size(max_block_size_)
    , connection_holder(std::move(connection_holder_))
    , query_str(query_str_)
{
    LOG_DEBUG(getLogger("PGinit"), "in constructor 1");
    init(*sample_block);
}


template<typename T>
PostgreSQLSource<T>::PostgreSQLSource(
    std::shared_ptr<T> tx_,
    const std::string & query_str_,
    SharedHeader sample_block,
    UInt64 max_block_size_,
    bool auto_commit_)
    : ISource(std::make_shared<const Block>(sample_block->cloneEmpty()))
    , max_block_size(max_block_size_)
    , auto_commit(auto_commit_)
    , query_str(query_str_)
    , tx(std::move(tx_))
{
    LOG_DEBUG(getLogger("PGinit"), "in constructor 2");
    init(*sample_block);
}

template<typename T>
void PostgreSQLSource<T>::init(const Block & sample_block)
{
    description.init(sample_block);

    for (const auto idx : collections::range(0, description.sample_block.columns()))
        if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
            preparePostgreSQLArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);

    /// pqxx::stream_from uses COPY command, will get error if ';' is present
    if (query_str.ends_with(';'))
        query_str.resize(query_str.size() - 1);
}


template<typename T>
void PostgreSQLSource<T>::onStart()
{
    if (!tx)
    {
        try
        {
            auto & conn = connection_holder->get();
            tx = std::make_shared<T>(conn);
        }
        catch (const pqxx::broken_connection &)
        {
            connection_holder->update();
            tx = std::make_shared<T>(connection_holder->get());
        }
    }

    // Check for cancellation before starting any async operations
    if (isCancelled())
    {
        LOG_DEBUG(getLogger("PGinit"), "Query was cancelled before async start");
        finished = true;
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PostgreSQLSource query was cancelled before start");
    }

    // Run stream_from in separate thread with timeout
    LOG_DEBUG(getLogger("PGinit"), "before fut");
    /*
    std::future<std::unique_ptr<pqxx::stream_from>> fut =
        std::async(std::launch::async, [&] {
            return std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, query_str);
        });

    */
    std::promise<std::unique_ptr<pqxx::stream_from>> promise;
    auto fut = promise.get_future();
    std::atomic<bool> task_cancelled{false};

    ThreadFromGlobalPool thread([this, &promise, &task_cancelled]() {
    // ThreadFromGlobalPool thread([&promise, &task_cancelled]() {
        LOG_DEBUG(getLogger("PGinit"), "started stream init thread...");
        try
        {
            if (task_cancelled)
                throw Exception( ErrorCodes::QUERY_WAS_CANCELLED, "Task cancelled");

            auto stream_ = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, query_str);
            // std::unique_ptr<pqxx::stream_from> stream_{nullptr};
            // std::this_thread::sleep_for(std::chrono::seconds(30));

            if (task_cancelled)
            {
                stream_->close();
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Task cancelled");
            }

            promise.set_value(std::move(stream_));

        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
        LOG_DEBUG(getLogger("PGinit"), "finished stream init thread");
    });

    thread.detach();

    LOG_DEBUG(getLogger("PGinit"), "before wait");
    auto status = fut.wait_for(std::chrono::seconds(10));
    LOG_DEBUG(getLogger("PGinit"), "after wait");
    if (status != std::future_status::ready) {
        task_cancelled = true;

        finished = true;

        // Check if query was cancelled before proceeding with cancellation
        LOG_DEBUG(getLogger("PGinit"), "Checking isCancelled()");
        if (isCancelled())
        {
            LOG_DEBUG(getLogger("PGinit"), "Query was cancelled after timeout");
            // finished = true;
            // throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PostgreSQLSource query was cancelled");
            // return;
        }

        // Cancel PostgreSQL query
        LOG_DEBUG(getLogger("PGinit"), "before cancelling PostgreSQL query");
        try
        {
            LOG_DEBUG(getLogger("PGinit"), "cancelling query");
            tx->conn().cancel_query();
            if (stream)
            {
                LOG_DEBUG(getLogger("PGinit"), "closing stream");
                stream->close();

            }
            LOG_DEBUG(getLogger("PGinit"), "waiting more time");
            fut.wait_for(std::chrono::milliseconds(100));
            LOG_DEBUG(getLogger("PGinit"), "aborting transcation");
            tx->abort();
            LOG_DEBUG(getLogger("PGinit"), "transaction aborted");
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("PGinit"), "exception in cancelling PostgreSQL query");
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        // stream.reset();
        // tx.reset();

        // if (connection_holder)
        //     connection_holder->setBroken();



        LOG_DEBUG(getLogger("PGinit"), "wait not OK");
        // finished = true;

        // throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "PostgreSQL query timeout");

        // throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE, "PostgreSQL query timeout");
    }
    else
    {
        // Stream created successfully
        LOG_DEBUG(getLogger("PGinit"), "wait OK");
        try
        {
            stream = fut.get();
            LOG_DEBUG(getLogger("PGinit"), "got stream");

            // Check if async thread threw an exception
            // if (async_exception)
            //     std::rethrow_exception(async_exception);

        }
        catch (const pqxx::broken_connection & e)
        {
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE,
                           "PostgreSQL connection broken: {}", e.what());
        }
        catch (const pqxx::sql_error & e)
        {
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE,
                           "PostgreSQL SQL error: {} (Query: {})", e.what(), e.query());
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::POSTGRESQL_CONNECTION_FAILURE,
                           "Failed to create PostgreSQL stream: {}", e.what());
        }
    }

    // stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view{query_str});
}

template<typename T>
IProcessor::Status PostgreSQLSource<T>::prepare()
{
    if (!started)
    {
        /*
        try
        {
            // Check if query was cancelled before starting
            if (isCancelled())
            {
                LOG_DEBUG(getLogger("PGinit"), "Query was cancelled before start");

                // finished = true;
                // return Status::Finished;

                // throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PostgreSQLSource query was cancelled");
            }
            */

            onStart();
            LOG_DEBUG(getLogger("PGinit"), "Check after onStart");
            // if (finished)
            //     throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "PostgreSQLSource query was finished");


            if (isCancelled())
            {
                LOG_DEBUG(getLogger("PGinit"), "Query was cancelled after start");

                finished = true;

                is_completed = true;
                // return Status::Finished;

                // throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PostgreSQLSource query was cancelled");

                // throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "PostgreSQLSource test exception 3");
            }
            else
                started = true;
        /*
        }
        catch (Exception &)
        {
            LOG_DEBUG(getLogger("PGinit"), "inside Exception catch");
            finished = true;
            throw;
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("PGinit"), "inside catch");
            // getPort().finish();
            // is_completed = true;
            // is_cancelled = true;
            finished = true;
            // return Status::Finished;
            throw;
            // throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "PostgreSQLSource query was cancelled by timeout");
        }
        */
    }

    LOG_DEBUG(getLogger("PGinit"), "before ISource::prepare()");
    auto status = ISource::prepare();
    LOG_DEBUG(getLogger("PGinit"), "after ISource::prepare()");
    if (status == Status::Finished)
    {
        LOG_DEBUG(getLogger("PGinit"), "in finished");
        if (stream)
            stream->close();

        if (tx && auto_commit && !finished)
            tx->commit();

        is_completed = true;
        LOG_DEBUG(getLogger("PGinit"), "after finished");
    }

    // throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "PostgreSQLSource test exception 2");

    return status;
}

template<typename T>
Chunk PostgreSQLSource<T>::generate()
{
    LOG_DEBUG(getLogger("PGinit"), "in generate()");
    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return {};

    // throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "PostgreSQLSource test exception 1");

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    LOG_DEBUG(getLogger("PGinit"), "before while loop");

    while (!isCancelled())
    {
        const std::vector<pqxx::zview> * row{stream->read_row()};

        /// row is nullptr if pqxx::stream_from is finished
        if (!row)
            break;

        if (row->size() > description.sample_block.columns())
            throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
                            "Row has too many columns: {}, expected structure: {}",
                            row->size(), description.sample_block.dumpStructure());

        for (const auto idx : collections::range(0, row->size()))
        {
            const auto & sample = description.sample_block.getByPosition(idx);

            /// if got NULL type, then pqxx::zview will return nullptr in c_str()
            if ((*row)[idx].c_str())
            {
                if (description.types[idx].second)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    const auto & data_type = assert_cast<const DataTypeNullable &>(*sample.type);

                    insertPostgreSQLValue(
                            column_nullable.getNestedColumn(), (*row)[idx],
                            description.types[idx].first, data_type.getNestedType(), array_info, idx);

                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                {
                    insertPostgreSQLValue(
                            *columns[idx], (*row)[idx], description.types[idx].first, sample.type, array_info, idx);
                }
            }
            else
            {
                insertDefaultPostgreSQLValue(*columns[idx], *sample.column);
            }

        }

        if (++num_rows == max_block_size)
            break;
    }

    return Chunk(std::move(columns), num_rows);
}


template<typename T>
void PostgreSQLSource<T>::onCancel() noexcept
{
    LOG_DEBUG(getLogger("PGinit"), "onCancel cancelling query");
    tx->conn().cancel_query();
    if (stream)
    {
        /** Internally libpqxx::stream_from runs PostgreSQL copy query `COPY query TO STDOUT`.
             * During transaction abort we try to execute PostgreSQL `ROLLBACK` command and if
             * copy query is not cancelled, we wait until it finishes.
             */
        // LOG_DEBUG(getLogger("PGinit"), "cancelling query");
        // tx->conn().cancel_query();

        /** If stream is not closed, libpqxx::stream_from closes stream in destructor, but that way
             * exception is added into transaction pending error and we can potentially ignore exception message.
             */
        LOG_DEBUG(getLogger("PGinit"), "onCancel closing stream");
        stream->close();

    }
    LOG_DEBUG(getLogger("PGinit"), "onCancel aborting transcation");
    tx->abort();
    LOG_DEBUG(getLogger("PGinit"), "onCancel transaction aborted");
}

template<typename T>
PostgreSQLSource<T>::~PostgreSQLSource()
{
    LOG_DEBUG(getLogger("PGinit"), "in destructor");
    if (!is_completed)
    {
        LOG_DEBUG(getLogger("PGinit"), "is not completed");
        try
        {
            if (stream)
            {
                /** Internally libpqxx::stream_from runs PostgreSQL copy query `COPY query TO STDOUT`.
                  * During transaction abort we try to execute PostgreSQL `ROLLBACK` command and if
                  * copy query is not cancelled, we wait until it finishes.
                  */
                tx->conn().cancel_query();

                /** If stream is not closed, libpqxx::stream_from closes stream in destructor, but that way
                  * exception is added into transaction pending error and we can potentially ignore exception message.
                  */
                stream->close();
            }
        }
        catch (...)
        {
            LOG_DEBUG(getLogger("PGinit"), "in destructor catch");
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        stream.reset();
        tx.reset();

        if (connection_holder)
            connection_holder->setBroken();
    }
    LOG_DEBUG(getLogger("PGinit"), "exiting destructor");
}

template
class PostgreSQLSource<pqxx::ReplicationTransaction>;

template
class PostgreSQLSource<pqxx::ReadTransaction>;

}

#endif
