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


template<typename T>
void PostgreSQLSource<T>::onStart()
{
    if (is_completed.load())
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

        std::lock_guard lock(tx_mutex);
        tx = std::move(new_tx);
    }

    /// A cancel arriving while the transaction was being constructed saw `tx` still null, so it
    /// consumed `is_completed` and skipped its own teardown. Do that teardown here, otherwise the
    /// connection is returned to the pool with the transaction left open.
    if (is_completed.load())
    {
        if (connection_holder)
            connection_holder->setBroken();
        return;
    }

    LOG_TEST(getLogger("PostgreSQLSource"), "Stream data from database");
    stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view{query_str});

    /// A cancel arriving while the COPY was starting ran before the backend had it, so it
    /// interrupted nothing, and it consumed `is_completed`, so the destructor returns early and
    /// does not cancel it either. Cancel it here, where it exists.
    if (is_completed.load())
    {
        try
        {
            {
                /// That cancel may still be inside cancel_query() on this same connection.
                std::lock_guard lock(cancel_mutex);
                tx->conn().cancel_query();
            }
            stream->close();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (connection_holder)
            connection_holder->setBroken();
    }
}

template<typename T>
IProcessor::Status PostgreSQLSource<T>::prepare()
{
    if (!started.load())
    {
        onStart();
        started.store(true);
    }

    auto status = ISource::prepare();
    if (status == Status::Finished)
    {
        if (stream)
            stream->close();

        if (tx && auto_commit)
            tx->commit();

        is_completed.store(true);
    }

    return status;
}

template<typename T>
Chunk PostgreSQLSource<T>::generate()
{
    LOG_TEST(getLogger("PostgreSQLSource"), "Generate a chunk from stream");

    /// Check if source was cancelled or completed
    if (is_completed.load() || isCancelled())
        return {};

    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return {};

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (!isCancelled() && !is_completed.load())
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
    /// Use atomic flag to prevent double-cancellation
    if (is_completed.exchange(true))
        return;

    /// Outer try/catch: this function is noexcept, and locking tx_mutex may throw.
    try
    {
        /// Snapshot under the lock, then use it with the lock released: the pqxx calls below block.
        std::shared_ptr<T> tx_snapshot;
        {
            std::lock_guard lock(tx_mutex);
            tx_snapshot = tx;
        }

        /// The code is executed only if onStart() was not finished mainly due to freezing on pqxx::from_query
        if (!started.load() && tx_snapshot && tx_snapshot->conn().is_open())
        {
            try
            {
                std::lock_guard lock(cancel_mutex);
                tx_snapshot->conn().cancel_query();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            if (connection_holder)
                connection_holder->setBroken();
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
    /// Use atomic flag to prevent double cleanup
    if (is_completed.exchange(true))
        return;

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
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    stream.reset();
    tx.reset();

    if (connection_holder)
        connection_holder->setBroken();
}

template
class PostgreSQLSource<pqxx::ReplicationTransaction>;

template
class PostgreSQLSource<pqxx::ReadTransaction>;

}

#endif
