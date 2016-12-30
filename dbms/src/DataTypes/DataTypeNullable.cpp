#include <DB/DataTypes/DataTypeNullable.h>
#include <DB/Columns/ColumnNullable.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ConcatReadBuffer.h>


namespace DB
{


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

	bool is_null = col.isNullAt(row_num);
	writeBinary(is_null, ostr);
	if (!is_null)
		nested_data_type->serializeBinary(*col.getNestedColumn(), row_num, ostr);
}


/// We need to insert both to nested column and to null byte map, or, in case of exception, to not insert at all.
template <typename CheckForNull, typename DeserializeNested>
static void safeDeserialize(
	IColumn & column,
	CheckForNull && check_for_null, DeserializeNested && deserialize_nested)
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	if (check_for_null())
	{
		col.insertDefault();
	}
	else
	{
		deserialize_nested(*col.getNestedColumn());

		try
		{
			col.getNullMap().push_back(0);
		}
		catch (...)
		{
			col.getNestedColumn()->popBack(1);
			throw;
		}
	}
}


void DataTypeNullable::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
	safeDeserialize(column,
		[&istr] { bool is_null = 0; readBinary(is_null, istr); return is_null; },
		[this, &istr] (IColumn & nested) { nested_data_type->deserializeBinary(nested, istr); } );
}


void DataTypeNullable::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("\\N", ostr);
	else
		nested_data_type->serializeTextEscaped(*col.getNestedColumn(), row_num, ostr);
}


void DataTypeNullable::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	/// Little tricky, because we cannot discriminate null from first character.

	if (istr.eof())
		throw Exception("Unexpected end of stream, while parsing value of Nullable type", ErrorCodes::CANNOT_READ_ALL_DATA);

	/// This is not null, surely.
	if (*istr.position() != '\\')
	{
		safeDeserialize(column,
			[&istr] { return false; },
			[this, &istr] (IColumn & nested) { nested_data_type->deserializeTextEscaped(nested, istr); } );
	}
	else
	{
		/// Now we know, that data in buffer starts with backslash.
		++istr.position();

		if (istr.eof())
			throw Exception("Unexpected end of stream, while parsing value of Nullable type, after backslash", ErrorCodes::CANNOT_READ_ALL_DATA);

		safeDeserialize(column,
			[&istr]
			{
				if (*istr.position() == 'N')
				{
					++istr.position();
					return true;
				}
				return false;
			},
			[this, &istr] (IColumn & nested)
			{
				if (istr.position() != istr.buffer().begin())
				{
					/// We could step back to consume backslash again.
					--istr.position();
					nested_data_type->deserializeTextEscaped(nested, istr);
				}
				else
				{
					/// Otherwise, we need to place backslash back in front of istr.
					ReadBuffer prefix(const_cast<char *>("\\"), 1, 0);
					ConcatReadBuffer prepended_istr(prefix, istr);

					nested_data_type->deserializeTextEscaped(nested, prepended_istr);

					/// Synchronise cursor position in original buffer.

					if (prepended_istr.count() > 1)
						istr.position() = prepended_istr.position();
				}
			});
	}
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("NULL", ostr);
	else
		nested_data_type->serializeTextQuoted(*col.getNestedColumn(), row_num, ostr);
}


void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	safeDeserialize(column,
		[&istr] { return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive("NULL", istr); },
		[this, &istr] (IColumn & nested) { nested_data_type->deserializeTextQuoted(nested, istr); } );
}

void DataTypeNullable::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("\\N", ostr);
	else
		nested_data_type->serializeTextCSV(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	safeDeserialize(column,
		[&istr] { return checkStringByFirstCharacterAndAssertTheRest("\\N", istr); },
		[this, delimiter, &istr] (IColumn & nested) { nested_data_type->deserializeTextCSV(nested, istr, delimiter); } );
}

void DataTypeNullable::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("NULL", ostr);
	else
		nested_data_type->serializeText(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr,
	bool force_quoting_64bit_integers) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("null", ostr);
	else
		nested_data_type->serializeTextJSON(*col.getNestedColumn(), row_num, ostr,
			force_quoting_64bit_integers);
}

void DataTypeNullable::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	safeDeserialize(column,
		[&istr] { return checkStringByFirstCharacterAndAssertTheRest("null", istr); },
		[this, &istr] (IColumn & nested) { nested_data_type->deserializeTextJSON(nested, istr); } );
}

void DataTypeNullable::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
		writeCString("\\N", ostr);
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
