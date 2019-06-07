#include <Formats/MySQLWireBlockOutputStream.h>


namespace DB
{

void registerOutputFormatMySQLWire(FormatFactory & factory)
{
    factory.registerOutputFormat("MySQLWire", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
        const FormatSettings &)
    {
        return std::make_shared<MySQLWireBlockOutputStream>(buf, sample, const_cast<Context &>(context));
    });
}

}
