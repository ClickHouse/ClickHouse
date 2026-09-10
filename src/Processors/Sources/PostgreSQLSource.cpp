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

#include <cerrno>
#include <sys/socket.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
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


/// Finalizes the state of the source: cancels the query still running on the connection, closes the
/// COPY stream and marks the connection broken. A null argument means there is nothing of that kind
/// to finalize. Must be called with tx_mutex released, since the calls below block.
template<typename T>
void PostgreSQLSource<T>::finalize(const std::shared_ptr<T> & tx_to_cancel, pqxx::stream_from * stream_to_close) noexcept
{
    try
    {
        if (tx_to_cancel)
        {
            /// `cancel_query` reads the connection to build its `PGcancel`, so onCancel() and the
            /// destructor must not reach it at once. It does not make the connection shareable.
            std::lock_guard lock(cancel_mutex);
            tx_to_cancel->conn().cancel_query();
        }

        /// Closing it here keeps the exception out of the transaction's pending error, where it
        /// would hide the message.
        if (stream_to_close)
            stream_to_close->close();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (connection_holder)
        connection_holder->setBroken();
}

template<typename T>
void PostgreSQLSource<T>::onStart()
{
    if (stop_requested.load())
        return;

    if (!tx)
    {
        /// Construct outside tx_mutex: the transaction constructor issues BEGIN, i.e. network I/O.
        std::shared_ptr<T> new_tx;
        try
        {
            auto & conn = connection_holder->get();
            new_tx = std::make_shared<T>(conn);
        }
        catch (const pqxx::broken_connection &)
        {
            connection_holder->update();
            new_tx = std::make_shared<T>(connection_holder->get());
        }
        catch (...)
        {
            /// BEGIN failed on a connection we already took out of the pool - do not hand it back
            /// for reuse. The destructor cannot do this: there is no transaction for it to see.
            connection_holder->setBroken();
            throw;
        }

        std::lock_guard lock(tx_mutex);
        tx = std::move(new_tx);
        /// Taken on the thread that owns the connection, so it cannot be a descriptor being closed.
        interrupt_fd = ::dup(tx->conn().sock());
    }

    /// A cancel during the constructor found `tx` null and could only ask us to stop. Do not open
    /// the COPY then - the destructor tears the transaction down.
    if (stop_requested.load())
        return;

    LOG_TEST(getLogger("PostgreSQLSource"), "Stream data from database");
    stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view{query_str});

    /// No recheck for a cancel racing the line above: it may have interrupted nothing, and the
    /// destructor cancels the stream that exists by then.
}

template<typename T>
IProcessor::Status PostgreSQLSource<T>::prepare()
{
    if (!started.load())
    {
        try
        {
            onStart();
        }
        catch (const pqxx::failure &)
        {
            /// A start that onCancel() interrupted fails instead of returning. Report the cancellation.
            if (!stop_requested.load())
                throw;
        }
        started.store(true);
    }

    auto status = ISource::prepare();
    if (status == Status::Finished && !stop_requested.load() && !teardown_started.exchange(true))
    {
        /// Only a finish that was not cancelled commits here and claims the teardown. After a
        /// cancel it is left to the destructor: the cancelling thread may still be in
        /// `cancel_query` on this connection, and a `COMMIT` racing it is what libpq forbids.
        if (stream)
            stream->close();

        if (tx && auto_commit)
            tx->commit();

        stop_requested.store(true);
        finalized.store(true);
    }

    return status;
}

template<typename T>
Chunk PostgreSQLSource<T>::generate()
{
    LOG_TEST(getLogger("PostgreSQLSource"), "Generate a chunk from stream");

    /// Check if source was cancelled or completed
    if (stop_requested.load() || isCancelled())
        return {};

    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return {};

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (!isCancelled() && !stop_requested.load())
    {
        const std::vector<pqxx::zview> * row{nullptr};
        try
        {
            row = stream->read_row();
        }
        catch (const pqxx::failure &)
        {
            /// An interrupted read fails here instead of returning. Report the cancellation.
            if (stop_requested.load())
                break;
            throw;
        }

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

                    column_nullable.getNullMapData().emplace_back(false);
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
    /// A signal, not a claim on the teardown: this runs while onStart() may still be creating `tx`
    /// and `stream`, so it cannot finish the job, and taking the claim here would leave nobody to.
    stop_requested.store(true);

    /// Outer try/catch: this function is noexcept, and locking tx_mutex may throw.
    try
    {
        /// Snapshot under the lock, then use it with the lock released: the pqxx calls below block.
        std::shared_ptr<T> tx_snapshot;
        int fd = -1;
        {
            std::lock_guard lock(tx_mutex);
            tx_snapshot = tx;
            fd = interrupt_fd;
        }

        if (!tx_snapshot)
            return;

        /// The connection is ours to discard. Ask the server to cancel first, while the connection can still
        /// address it, then take the transport away, which wakes the read whether or not the server obliged.
        if (connection_holder && fd >= 0)
        {
            /// A finish already under way has nothing left to wake, and its COMMIT must not be broken.
            if (teardown_started.exchange(true))
                return;

            if (tx_snapshot->conn().is_open())
                finalize(tx_snapshot, nullptr);

            /// `shutdown` and not `close` keeps the descriptor valid for the thread still reading it.
            ::shutdown(fd, SHUT_RDWR);
            connection_holder->setBroken();
            LOG_DEBUG(getLogger("PostgreSQLSource"), "Shut the connection down to interrupt the read");
        }
        /// A connection handed in with the transaction stays in use by its owner, so it is not ours to take
        /// away. Ask the server instead, which only helps while the COPY is starting.
        else if (!started.load() && tx_snapshot->conn().is_open())
        {
            finalize(tx_snapshot, nullptr);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

template<typename T>
PostgreSQLSource<T>::~PostgreSQLSource()
{
    /// The teardown owner for every path but a clean finish, which prepare() claims. Without
    /// cancelling the COPY the ROLLBACK issued during transaction abort waits for it. With no
    /// transaction nothing reached the connection, so it stays healthy and is left in the pool.
    /// A connection already cancelled and taken down has nothing left to cancel, and the attempt would block.
    if (!finalized.exchange(true) && tx)
        finalize((stream && !teardown_started.load()) ? tx : nullptr, stream.get());

    stream.reset();
    tx.reset();

    if (interrupt_fd >= 0)
    {
        [[maybe_unused]] int err = ::close(interrupt_fd);
        chassert(!err || errno == EINTR);
    }
}

template
class PostgreSQLSource<pqxx::ReplicationTransaction>;

template
class PostgreSQLSource<pqxx::ReadTransaction>;

}

#endif
