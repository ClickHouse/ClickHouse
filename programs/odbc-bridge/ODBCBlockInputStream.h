#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>
#include "ODBCConnectionFactory.h"


namespace DB
{
/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IBlockInputStream
{
public:
    ODBCBlockInputStream(nanodbc::ConnectionHolderPtr connection, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size_);

    String getName() const override { return "ODBC"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    using QueryResult = std::shared_ptr<nanodbc::result>;
    using ValueType = ExternalResultDescription::ValueType;

    Block readImpl() override;

    static void insertValue(IColumn & column, const DataTypePtr data_type, const ValueType type, nanodbc::result & row, size_t idx);

    static void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }

    Poco::Logger * log;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    nanodbc::result result;
    String query;
    bool finished = false;
};

}
