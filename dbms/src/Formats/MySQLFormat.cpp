#include <DataStreams/MySQLBlockOutputStream.h>


namespace DB
{

void registerOutputFormatMySQL(FormatFactory & factory)
{
    factory.registerOutputFormat("MySQL", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
        const FormatSettings &)
    {
        return std::make_shared<MySQLBlockOutputStream>(buf, sample, context.client_capabilities, const_cast<Context &>(context).sequence_id);
    });
}

}
