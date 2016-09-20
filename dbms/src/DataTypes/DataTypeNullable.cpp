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
/// represented as null symbols into this file. The methods below are helpers that
/// we use for the binary deserialization. They are used as follows:
///
/// auto action = NullDeserializer<NullSymbol::XXX>::execute(col, istr);
///
/// if (action == Action::NONE) /// eof
///    return;
/// else if (action == Action::ADD_ORDINARY) /// add an ordinary value
///    ... deserialize the nested column ...
///
/// updateNullMap(col, action);
///
/// This two-step process is required because when we perform an INSERT query
/// whose values are expressions, ValuesRowInputStream attempts to deserialize
/// a stream, then raises an exception, and finally evaluates expressions inside
/// the exception handler. If we did something like:
///
/// ... deserialize the null map...
/// ... deserialize the nested column...
///
/// there would be garbage in the null map.


/// Action to be performed while updating the null map of a nullable column.
enum class Action
{
	NONE, 			/// do nothing
	ADD_NULL,		/// add a value indicating a NULL
	ADD_ORDINARY	/// add a value indicating an ordinary value
};

/// The template class below provides one method that takes a nullable column being
/// deserialized and looks if there is a pending null symbol in the corresponding
/// binary data file. It returns the appropriate action to be performed on the null
/// map of the column.
template <typename Null>
struct NullDeserializer
{
	static Action execute(ColumnNullable & col, ReadBuffer & istr)
	{
		if (!istr.eof())
		{
			if (*istr.position() == Null::name[0])
			{
				++istr.position();
				static constexpr auto length = __builtin_strlen(Null::name);
				if (length > 1)
					assertString(&Null::name[1], istr);

				return Action::ADD_NULL;
			}
			else
				return Action::ADD_ORDINARY;
		}
		else
			return Action::NONE;
	}
};

/// This function takes the appropiate action when updating the null map of a nullable
/// column.
void updateNullMap(ColumnNullable & col, const Action & action)
{
	auto & null_map = static_cast<ColumnUInt8 &>(*col.getNullValuesByteMap()).getData();

	if (action == Action::ADD_NULL)
	{
		null_map.push_back(1);

		ColumnPtr & nested_col = col.getNestedColumn();
		nested_col->insertDefault();
	}
	else if (action == Action::ADD_ORDINARY)
		null_map.push_back(0);
	else
		throw Exception{"DataTypeNullable: internal error", ErrorCodes::LOGICAL_ERROR};
}

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

	auto action = NullDeserializer<NullSymbol::Escaped>::execute(col, istr);

	if (action == Action::NONE)
		return;
	else if (action == Action::ADD_ORDINARY)
		nested_data_type->deserializeTextEscaped(*col.getNestedColumn(), istr);

	updateNullMap(col, action);
}

void DataTypeNullable::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	const ColumnNullable & col = static_cast<const ColumnNullable &>(column);

	if (col.isNullAt(row_num))
	{
		/// This is not a typo. We really mean "Escaped" and not "Quoted".
		/// The reason is that, when displaying an array of nullable strings,
		/// we want to see \N instead of NULL.
		writeCString(NullSymbol::Escaped::name, ostr);
	}
	else
		nested_data_type->serializeTextQuoted(*col.getNestedColumn(), row_num, ostr);
}

void DataTypeNullable::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	ColumnNullable & col = static_cast<ColumnNullable &>(column);

	auto action = NullDeserializer<NullSymbol::Quoted>::execute(col, istr);

	if (action == Action::NONE)
		return;
	else if (action == Action::ADD_ORDINARY)
		nested_data_type->deserializeTextQuoted(*col.getNestedColumn(), istr);

	updateNullMap(col, action);

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

	auto action = NullDeserializer<NullSymbol::Quoted>::execute(col, istr);

	if (action == Action::NONE)
		return;
	else if (action == Action::ADD_ORDINARY)
		nested_data_type->deserializeTextCSV(*col.getNestedColumn(), istr, delimiter);

	updateNullMap(col, action);
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

	auto action = NullDeserializer<NullSymbol::JSON>::execute(col, istr);

	if (action == Action::NONE)
		return;
	else if (action == Action::ADD_ORDINARY)
		nested_data_type->deserializeTextJSON(*col.getNestedColumn(), istr);

	updateNullMap(col, action);
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
