#include "PostgreSQLBlockInputStream.h"

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
#include <common/range.h>
#include <common/logger_useful.h>


namespace DB
{


template<typename T>
PostgreSQLBlockInputStream<T>::PostgreSQLBlockInputStream(
    postgres::ConnectionHolderPtr connection_holder_,
    const std::string & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_)
    : query_str(query_str_)
    , max_block_size(max_block_size_)
    , connection_holder(std::move(connection_holder_))
{
    init(sample_block);
}


template<typename T>
PostgreSQLBlockInputStream<T>::PostgreSQLBlockInputStream(
    std::shared_ptr<T> tx_,
    const std::string & query_str_,
    const Block & sample_block,
    const UInt64 max_block_size_,
    bool auto_commit_)
    : query_str(query_str_)
    , tx(std::move(tx_))
    , max_block_size(max_block_size_)
    , auto_commit(auto_commit_)
{
    init(sample_block);
}


template<typename T>
void PostgreSQLBlockInputStream<T>::init(const Block & sample_block)
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
void PostgreSQLBlockInputStream<T>::readPrefix()
{
    tx = std::make_shared<T>(connection_holder->get());
    stream = std::make_unique<pqxx::stream_from>(*tx, pqxx::from_query, std::string_view(query_str));
}


template<typename T>
Block PostgreSQLBlockInputStream<T>::readImpl()
{
    /// Check if pqxx::stream_from is finished
    if (!stream || !(*stream))
        return Block();

    MutableColumns columns = description.sample_block.cloneEmptyColumns();
    size_t num_rows = 0;

    while (true)
    {
        const std::vector<pqxx::zview> * row{stream->read_row()};

        /// row is nullptr if pqxx::stream_from is finished
        if (!row)
            break;

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

    return description.sample_block.cloneWithColumns(std::move(columns));
}


template<typename T>
void PostgreSQLBlockInputStream<T>::readSuffix()
{
    if (stream)
    {
        stream->complete();

        if (auto_commit)
            tx->commit();
    }
}

template
class PostgreSQLBlockInputStream<pqxx::ReplicationTransaction>;

template
class PostgreSQLBlockInputStream<pqxx::ReadTransaction>;

}

#endif
