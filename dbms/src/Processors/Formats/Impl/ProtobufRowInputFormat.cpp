#include <Common/config.h>
#if USE_PROTOBUF

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h>
#include <Processors/Formats/Impl/ProtobufRowInputFormat.h>


namespace DB
{

ProtobufRowInputFormat::ProtobufRowInputFormat(ReadBuffer & in, Block header, Params params, const FormatSchemaInfo & info)
: IRowInputFormat(header, in, params)
,  data_types(header.getDataTypes())
, reader(in, ProtobufSchemas::instance().getMessageTypeForFormatSchema(info), header.getNames())
{
}

ProtobufRowInputFormat::~ProtobufRowInputFormat() = default;

bool ProtobufRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & extra)
{
    if (!reader.startMessage())
        return false; // EOF reached, no more messages.

    // Set of columns for which the values were read. The rest will be filled with default values.
    auto & read_columns = extra.read_columns;
    read_columns.assign(columns.size(), false);

    // Read values from this message and put them to the columns while it's possible.
    size_t column_index;
    while (reader.readColumnIndex(column_index))
    {
        bool allow_add_row = !static_cast<bool>(read_columns[column_index]);
        do
        {
            bool row_added;
            data_types[column_index]->deserializeProtobuf(*columns[column_index], reader, allow_add_row, row_added);
            if (row_added)
            {
                read_columns[column_index] = true;
                allow_add_row = false;
            }
        } while (reader.maybeCanReadValue());
    }

    // Fill non-visited columns with the default values.
    for (column_index = 0; column_index < read_columns.size(); ++column_index)
        if (!read_columns[column_index])
            data_types[column_index]->insertDefaultInto(*columns[column_index]);

    reader.endMessage();
    return true;
}

bool ProtobufRowInputFormat::allowSyncAfterError() const
{
    return true;
}

void ProtobufRowInputFormat::syncAfterError()
{
    reader.endMessage();
}


void registerInputFormatProcessorProtobuf(FormatFactory & factory)
{
    factory.registerInputFormatProcessor("Protobuf", [](
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        IRowInputFormat::Params params,
        const FormatSettings &)
    {
        return std::make_shared<ProtobufRowInputFormat>(buf, sample, params, FormatSchemaInfo(context, "proto"));
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorProtobuf(FormatFactory &) {}
}

#endif
