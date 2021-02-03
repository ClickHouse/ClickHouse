#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Poco/Data/Session.h>
#include <Core/ExternalResultDescription.h>
#include <Parsers/IdentifierQuotingStyle.h>

namespace DB
{
class ODBCBlockOutputStream : public IBlockOutputStream
{
public:
    ODBCBlockOutputStream(Poco::Data::Session && session_, const std::string & remote_database_name_,
                          const std::string & remote_table_name_, const Block & sample_block_, IdentifierQuotingStyle quoting);

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    Poco::Data::Session session;
    std::string db_name;
    std::string table_name;
    Block sample_block;
    IdentifierQuotingStyle quoting;

    ExternalResultDescription description;
    Poco::Logger * log;
};

}
