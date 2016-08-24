#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/DataTypes/NullSymbol.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace
{

/// When a column is serialized as a binary data file, its null values are directly
/// represented as null symbols into this file.
/// The template class below provides one method that takes a nullable column being
/// deserialized and looks for the next null symbol from the corresponding binary
/// data file. It updates the null map of the nullable column accordingly. Moreover
/// if a null symbol has been found, a default value is appended to the column data.
template <typename Null>
struct NullDeserializer
{
	static bool execute(ColumnNullable & col, ReadBuffer & istr)
	{
		if (!istr.eof())
		{
			auto & null_map = static_cast<ColumnUInt8 &>(*col.getNullValuesByteMap()).getData();

			if (*istr.position() == Null::name[0])
			{
				++istr.position();
				static constexpr auto length = strlen(Null::name);
				if (length > 1)
					assertString(&Null::name[1], istr);
				null_map.push_back(1);

				ColumnPtr & nested_col = col.getNestedColumn();
				nested_col->insertDefault();

				return true;
			}
			else
			{
				null_map.push_back(0);
				return false;
			}
		}
		else
			return false;
	}
};

}

DataTypeNullable::DataTypeNullable(DataTypePtr nested_data_type_)
	: nested_data_type{nested_data_type_}
{
}

void DataTypeNullable::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
	nested_data_type->serializeBinary(*col.getNestedColumn(), ostr, offset, limit);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);
	nested_data_type->deserializeBinary(*col.getNestedColumn(), istr, limit, avg_value_size_hint);
}

void DataTypeNullable::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);
	nested_data_type->serializeBinary(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);
	nested_data_type->deserializeBinary(*col.getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::Escaped::name, ostr);
	else
		nested_data_type->serializeTextEscaped(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	if (!NullDeserializer<NullSymbol::Escaped>::execute(col, istr))
		nested_data_type->deserializeTextEscaped(*col.getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::Quoted::name, ostr);
	else
		nested_data_type->serializeTextQuoted(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	if (!NullDeserializer<NullSymbol::Quoted>::execute(col, istr))
		nested_data_type->deserializeTextQuoted(*col.getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::Quoted::name, ostr);
	else
		nested_data_type->serializeTextCSV(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	if (!NullDeserializer<NullSymbol::Quoted>::execute(col, istr))
		nested_data_type->deserializeTextCSV(*col.getNestedColumn(), istr, delimiter);
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::Plain::name, ostr);
	else
		nested_data_type->serializeText(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::JSON::name, ostr);
	else
		nested_data_type->serializeTextJSON(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	if (!NullDeserializer<NullSymbol::JSON>::execute(col, istr))
		nested_data_type->deserializeTextJSON(*col.getNestedColumn(), istr);
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString(NullSymbol::XML::name, ostr);
	else
		nested_data_type->serializeTextXML(*col.getNestedColumn(), row_num, ostr);
}

ColumnPtr DataTypeNullable::createColumn() const
{
	ColumnPtr new_col = nested_data_type->createColumn();
	return std::make_shared<ColumnNullable>(new_col, std::make_shared<ColumnUInt8>());
}

ColumnPtr DataTypeNullable::createConstColumn(size_t size, const Field & field) const
{
	ColumnPtr new_col = nested_data_type->createConstColumn(size, field);
	return std::make_shared<ColumnNullable>(new_col, std::make_shared<ColumnUInt8>(size));
}

}
