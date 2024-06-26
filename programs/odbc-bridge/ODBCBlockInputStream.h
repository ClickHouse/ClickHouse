#pragma once

#include <string>
#include <Core/Block.h>
#include <Processors/ISource.h>
#include <Core/ExternalResultDescription.h>
#include "ODBCPooledConnectionFactory.h"


namespace DB
{
/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCSource final : public ISource
{
public:
    ODBCSource(nanodbc::ConnectionHolderPtr connection, const std::string & query_str, const Block & sample_block, UInt64 max_block_size_);

    String getName() const override { return "ODBC"; }

private:
    using QueryResult = std::shared_ptr<nanodbc::result>;
    using ValueType = ExternalResultDescription::ValueType;

    Chunk generate() override;

    static void insertValue(IColumn & column, DataTypePtr data_type, ValueType type, nanodbc::result & row, size_t idx);

    static void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }

    Poco::Logger * log;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    nanodbc::result result;
    String query;
    bool is_finished = false;
};

}
