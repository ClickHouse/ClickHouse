#include <Common/config.h>
#include <Interpreters/Context.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/TabSeparatedRowInputStream.h>
#include <DataStreams/TabSeparatedRowOutputStream.h>
#include <DataStreams/TabSeparatedRawRowOutputStream.h>
#include <DataStreams/BinaryRowInputStream.h>
#include <DataStreams/BinaryRowOutputStream.h>
#include <DataStreams/ValuesRowInputStream.h>
#include <DataStreams/ValuesRowOutputStream.h>
#include <DataStreams/PrettyBlockOutputStream.h>
#include <DataStreams/PrettyCompactBlockOutputStream.h>
#include <DataStreams/PrettySpaceBlockOutputStream.h>
#include <DataStreams/VerticalRowOutputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/JSONRowOutputStream.h>
#include <DataStreams/JSONCompactRowOutputStream.h>
#include <DataStreams/JSONEachRowRowOutputStream.h>
#include <DataStreams/JSONEachRowRowInputStream.h>
#include <DataStreams/XMLRowOutputStream.h>
#include <DataStreams/TSKVRowOutputStream.h>
#include <DataStreams/TSKVRowInputStream.h>
#include <DataStreams/ODBCDriverBlockOutputStream.h>
#include <DataStreams/CSVRowInputStream.h>
#include <DataStreams/CSVRowOutputStream.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/FormatFactory.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataTypes/FormatSettingsJSON.h>
#if USE_CAPNP
#include <DataStreams/CapnProtoRowInputStream.h>
#endif

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
    extern const int UNKNOWN_FORMAT;
}


BlockInputStreamPtr FormatFactory::getInput(const String & name, ReadBuffer & buf,
    const Block & sample, const Context & context, size_t max_block_size) const
{
    const Settings & settings = context.getSettingsRef();

    auto wrap_row_stream = [&](auto && row_stream)
    {
        return std::make_shared<BlockInputStreamFromRowInputStream>(std::move(row_stream), sample, max_block_size,
            settings.input_format_allow_errors_num, settings.input_format_allow_errors_ratio);
    };

    if (name == "Native")
    {
        return std::make_shared<NativeBlockInputStream>(buf, sample, 0);
    }
    else if (name == "RowBinary")
    {
        return wrap_row_stream(std::make_shared<BinaryRowInputStream>(buf, sample));
    }
    else if (name == "TabSeparated" || name == "TSV") /// TSV is a synonym/alias for the original TabSeparated format
    {
        return wrap_row_stream(std::make_shared<TabSeparatedRowInputStream>(buf, sample));
    }
    else if (name == "TabSeparatedWithNames" || name == "TSVWithNames")
    {
        return wrap_row_stream(std::make_shared<TabSeparatedRowInputStream>(buf, sample, true));
    }
    else if (name == "TabSeparatedWithNamesAndTypes" || name == "TSVWithNamesAndTypes")
    {
        return wrap_row_stream(std::make_shared<TabSeparatedRowInputStream>(buf, sample, true, true));
    }
    else if (name == "Values")
    {
        return wrap_row_stream(std::make_shared<ValuesRowInputStream>(buf, sample, context, settings.input_format_values_interpret_expressions));
    }
    else if (name == "CSV")
    {
        return wrap_row_stream(std::make_shared<CSVRowInputStream>(buf, sample, ','));
    }
    else if (name == "CSVWithNames")
    {
        return wrap_row_stream(std::make_shared<CSVRowInputStream>(buf, sample, ',', true));
    }
    else if (name == "TSKV")
    {
        return wrap_row_stream(std::make_shared<TSKVRowInputStream>(buf, sample, settings.input_format_skip_unknown_fields));
    }
    else if (name == "JSONEachRow")
    {
        return wrap_row_stream(std::make_shared<JSONEachRowRowInputStream>(buf, sample, settings.input_format_skip_unknown_fields));
    }
#if USE_CAPNP
    else if (name == "CapnProto")
    {
        std::vector<String> tokens;
        auto schema_and_root = settings.format_schema.toString();
        boost::split(tokens, schema_and_root, boost::is_any_of(":"));
        if (tokens.size() != 2)
            throw Exception("Format CapnProto requires 'format_schema' setting to have a schema_file:root_object format, e.g. 'schema.capnp:Message'");

        const String & schema_dir = context.getFormatSchemaPath();
        return wrap_row_stream(std::make_shared<CapnProtoRowInputStream>(buf, sample, schema_dir, tokens[0], tokens[1]));
    }
#endif
    else if (name == "TabSeparatedRaw"
        || name == "TSVRaw"
        || name == "Pretty"
        || name == "PrettyCompact"
        || name == "PrettyCompactMonoBlock"
        || name == "PrettySpace"
        || name == "PrettyNoEscapes"
        || name == "PrettyCompactNoEscapes"
        || name == "PrettySpaceNoEscapes"
        || name == "Vertical"
        || name == "VerticalRaw"
        || name == "Null"
        || name == "JSON"
        || name == "JSONCompact"
        || name == "XML"
        || name == "ODBCDriver")
    {
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);
    }
    else
        throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}


static BlockOutputStreamPtr getOutputImpl(const String & name, WriteBuffer & buf,
    const Block & sample, const Context & context)
{
    const Settings & settings = context.getSettingsRef();
    FormatSettingsJSON json_settings(settings.output_format_json_quote_64bit_integers, settings.output_format_json_quote_denormals);

    if (name == "Native")
        return std::make_shared<NativeBlockOutputStream>(buf, 0, sample);
    else if (name == "RowBinary")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<BinaryRowOutputStream>(buf), sample);
    else if (name == "TabSeparated" || name == "TSV")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample), sample);
    else if (name == "TabSeparatedWithNames" || name == "TSVWithNames")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true), sample);
    else if (name == "TabSeparatedWithNamesAndTypes" || name == "TSVWithNamesAndTypes")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true, true), sample);
    else if (name == "TabSeparatedRaw" || name == "TSVRaw")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRawRowOutputStream>(buf, sample), sample);
    else if (name == "CSV")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<CSVRowOutputStream>(buf, sample), sample);
    else if (name == "CSVWithNames")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<CSVRowOutputStream>(buf, sample, true), sample);
    else if (name == "Pretty")
        return std::make_shared<PrettyBlockOutputStream>(buf, sample, false, settings.output_format_pretty_max_rows, context);
    else if (name == "PrettyCompact")
        return std::make_shared<PrettyCompactBlockOutputStream>(buf, sample, false, settings.output_format_pretty_max_rows, context);
    else if (name == "PrettyCompactMonoBlock")
    {
        BlockOutputStreamPtr dst = std::make_shared<PrettyCompactBlockOutputStream>(buf, sample, false, settings.output_format_pretty_max_rows, context);
        auto res = std::make_shared<SquashingBlockOutputStream>(dst, settings.output_format_pretty_max_rows, 0);
        res->disableFlush();
        return res;
    }
    else if (name == "PrettySpace")
        return std::make_shared<PrettySpaceBlockOutputStream>(buf, sample, false, settings.output_format_pretty_max_rows, context);
    else if (name == "PrettyNoEscapes")
        return std::make_shared<PrettyBlockOutputStream>(buf, sample, true, settings.output_format_pretty_max_rows, context);
    else if (name == "PrettyCompactNoEscapes")
        return std::make_shared<PrettyCompactBlockOutputStream>(buf, sample, true, settings.output_format_pretty_max_rows, context);
    else if (name == "PrettySpaceNoEscapes")
        return std::make_shared<PrettySpaceBlockOutputStream>(buf, sample, true, settings.output_format_pretty_max_rows, context);
    else if (name == "Vertical")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<VerticalRowOutputStream>(
            buf, sample, settings.output_format_pretty_max_rows), sample);
    else if (name == "VerticalRaw")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<VerticalRawRowOutputStream>(
            buf, sample, settings.output_format_pretty_max_rows), sample);
    else if (name == "Values")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<ValuesRowOutputStream>(buf), sample);
    else if (name == "JSON")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONRowOutputStream>(
            buf, sample, settings.output_format_write_statistics, json_settings), sample);
    else if (name == "JSONCompact")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONCompactRowOutputStream>(
            buf, sample, settings.output_format_write_statistics, json_settings), sample);
    else if (name == "JSONEachRow")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONEachRowRowOutputStream>(
            buf, sample, json_settings), sample);
    else if (name == "XML")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<XMLRowOutputStream>(buf, sample,
            settings.output_format_write_statistics), sample);
    else if (name == "TSKV")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TSKVRowOutputStream>(buf, sample), sample);
    else if (name == "ODBCDriver")
        return std::make_shared<ODBCDriverBlockOutputStream>(buf, sample);
    else if (name == "Null")
        return std::make_shared<NullBlockOutputStream>(sample);
    else
        throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

BlockOutputStreamPtr FormatFactory::getOutput(const String & name, WriteBuffer & buf,
    const Block & sample, const Context & context) const
{
    /** Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return std::make_shared<MaterializingBlockOutputStream>(getOutputImpl(name, buf, materializeBlock(sample), context), sample);
}

}
