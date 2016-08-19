#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/NativeBlockInputStream.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/DataStreams/TabSeparatedRowInputStream.h>
#include <DB/DataStreams/TabSeparatedRowOutputStream.h>
#include <DB/DataStreams/TabSeparatedRawRowOutputStream.h>
#include <DB/DataStreams/BinaryRowInputStream.h>
#include <DB/DataStreams/BinaryRowOutputStream.h>
#include <DB/DataStreams/ValuesRowInputStream.h>
#include <DB/DataStreams/ValuesRowOutputStream.h>
#include <DB/DataStreams/TabSeparatedBlockOutputStream.h>
#include <DB/DataStreams/PrettyBlockOutputStream.h>
#include <DB/DataStreams/PrettyCompactBlockOutputStream.h>
#include <DB/DataStreams/PrettySpaceBlockOutputStream.h>
#include <DB/DataStreams/VerticalRowOutputStream.h>
#include <DB/DataStreams/NullBlockOutputStream.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DB/DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DB/DataStreams/JSONRowOutputStream.h>
#include <DB/DataStreams/JSONCompactRowOutputStream.h>
#include <DB/DataStreams/JSONEachRowRowOutputStream.h>
#include <DB/DataStreams/JSONEachRowRowInputStream.h>
#include <DB/DataStreams/XMLRowOutputStream.h>
#include <DB/DataStreams/TSKVRowOutputStream.h>
#include <DB/DataStreams/TSKVRowInputStream.h>
#include <DB/DataStreams/PrettyCompactMonoBlockOutputStream.h>
#include <DB/DataStreams/ODBCDriverBlockOutputStream.h>
#include <DB/DataStreams/CSVRowInputStream.h>
#include <DB/DataStreams/CSVRowOutputStream.h>
#include <DB/DataStreams/MaterializingBlockOutputStream.h>
#include <DB/DataStreams/FormatFactory.h>


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
	if (name == "Native")
		return std::make_shared<NativeBlockInputStream>(buf);
	else if (name == "RowBinary")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<BinaryRowInputStream>(buf), sample, max_block_size);
	else if (name == "TabSeparated")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<TabSeparatedRowInputStream>(buf, sample), sample, max_block_size);
	else if (name == "TabSeparatedWithNames")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<TabSeparatedRowInputStream>(buf, sample, true), sample, max_block_size);
	else if (name == "TabSeparatedWithNamesAndTypes")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<TabSeparatedRowInputStream>(buf, sample, true, true), sample, max_block_size);
	else if (name == "Values")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<ValuesRowInputStream>(buf, context), sample, max_block_size);
	else if (name == "CSV")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<CSVRowInputStream>(buf, sample, ','), sample, max_block_size);
	else if (name == "CSVWithNames")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<CSVRowInputStream>(buf, sample, ',', true), sample, max_block_size);
	else if (name == "TSKV")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<TSKVRowInputStream>(buf, sample, false), sample, max_block_size);
	else if (name == "JSONEachRow")
		return std::make_shared<BlockInputStreamFromRowInputStream>(std::make_shared<JSONEachRowRowInputStream>(buf, sample), sample, max_block_size);
	else if (name == "TabSeparatedRaw"
		|| name == "BlockTabSeparated"
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
		throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);
	else
		throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}


static BlockOutputStreamPtr getOutputImpl(const String & name, WriteBuffer & buf,
	const Block & sample, const Context & context)
{
	if (name == "Native")
		return std::make_shared<NativeBlockOutputStream>(buf);
	else if (name == "RowBinary")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<BinaryRowOutputStream>(buf));
	else if (name == "TabSeparated")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample));
	else if (name == "TabSeparatedWithNames")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true));
	else if (name == "TabSeparatedWithNamesAndTypes")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true, true));
	else if (name == "TabSeparatedRaw")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TabSeparatedRawRowOutputStream>(buf, sample));
	else if (name == "BlockTabSeparated")
		return std::make_shared<TabSeparatedBlockOutputStream>(buf);
	else if (name == "CSV")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<CSVRowOutputStream>(buf, sample));
	else if (name == "CSVWithNames")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<CSVRowOutputStream>(buf, sample, true));
	else if (name == "Pretty")
		return std::make_shared<PrettyBlockOutputStream>(buf);
	else if (name == "PrettyCompact")
		return std::make_shared<PrettyCompactBlockOutputStream>(buf);
	else if (name == "PrettyCompactMonoBlock")
		return std::make_shared<PrettyCompactMonoBlockOutputStream>(buf);
	else if (name == "PrettySpace")
		return std::make_shared<PrettySpaceBlockOutputStream>(buf);
	else if (name == "PrettyNoEscapes")
		return std::make_shared<PrettyBlockOutputStream>(buf, true);
	else if (name == "PrettyCompactNoEscapes")
		return std::make_shared<PrettyCompactBlockOutputStream>(buf, true);
	else if (name == "PrettySpaceNoEscapes")
		return std::make_shared<PrettySpaceBlockOutputStream>(buf, true);
	else if (name == "Vertical")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<VerticalRowOutputStream>(buf, sample));
	else if (name == "VerticalRaw")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<VerticalRawRowOutputStream>(buf, sample));
	else if (name == "Values")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<ValuesRowOutputStream>(buf));
	else if (name == "JSON")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONRowOutputStream>(buf, sample,
			context.getSettingsRef().output_format_write_statistics));
	else if (name == "JSONCompact")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONCompactRowOutputStream>(buf, sample,
			context.getSettingsRef().output_format_write_statistics));
	else if (name == "JSONEachRow")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<JSONEachRowRowOutputStream>(buf, sample));
	else if (name == "XML")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<XMLRowOutputStream>(buf, sample,
			context.getSettingsRef().output_format_write_statistics));
	else if (name == "TSKV")
		return std::make_shared<BlockOutputStreamFromRowOutputStream>(std::make_shared<TSKVRowOutputStream>(buf, sample));
	else if (name == "ODBCDriver")
		return std::make_shared<ODBCDriverBlockOutputStream>(buf);
	else if (name == "Null")
		return std::make_shared<NullBlockOutputStream>();
	else
		throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

BlockOutputStreamPtr FormatFactory::getOutput(const String & name, WriteBuffer & buf,
	const Block & sample, const Context & context) const
{
	/** Материализация нужна, так как форматы могут использовать функции IDataType,
	  *  которые допускают работу только с полными столбцами.
	  */
	return std::make_shared<MaterializingBlockOutputStream>(getOutputImpl(name, buf, sample, context));
}

}
