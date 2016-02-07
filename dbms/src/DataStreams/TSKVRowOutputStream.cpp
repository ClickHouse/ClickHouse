#include <DB/IO/WriteHelpers.h>
#include <DB/DataStreams/TSKVRowOutputStream.h>


namespace DB
{

TSKVRowOutputStream::TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_)
	: TabSeparatedRowOutputStream(ostr_, sample_)
{
	NamesAndTypesList columns(sample_.getColumnsList());
	fields.assign(columns.begin(), columns.end());

	for (auto & field : fields)
	{
		String prepared_field_name;
		{
			WriteBufferFromString wb(prepared_field_name);
			writeAnyEscapedString<'='>(field.name.data(), field.name.data() + field.name.size(), wb);
			writeCString("=", wb);
		}
		field.name = prepared_field_name;
	}
}


void TSKVRowOutputStream::writeField(const Field & field)
{
	writeString(fields[field_number].name, ostr);
	data_types[field_number]->serializeTextEscaped(field, ostr);
	++field_number;
}

}
