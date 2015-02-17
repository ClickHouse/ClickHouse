#include <Poco/SharedPtr.h>

#include <DB/Core/Defines.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnConst.h>

#include <DB/DataTypes/DataTypeString.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/VarInt.h>

#include <emmintrin.h>


namespace DB
{

using Poco::SharedPtr;


void DataTypeString::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
	const String & s = get<const String &>(field);
	writeVarUInt(s.size(), ostr);
	writeString(s, ostr);
}


void DataTypeString::deserializeBinary(Field & field, ReadBuffer & istr) const
{
	UInt64 size;
	readVarUInt(size, istr);
	field = String();
	String & s = get<String &>(field);
	s.resize(size);
	istr.readStrict(&s[0], size);
}


void DataTypeString::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	const ColumnString & column_string = typeid_cast<const ColumnString &>(column);
	const ColumnString::Chars_t & data = column_string.getChars();
	const ColumnString::Offsets_t & offsets = column_string.getOffsets();

	size_t size = column.size();
	if (!size)
		return;

	size_t end = limit && offset + limit < size
		? offset + limit
		: size;

	if (offset == 0)
	{
		UInt64 str_size = offsets[0] - 1;
		writeVarUInt(str_size, ostr);
		ostr.write(reinterpret_cast<const char *>(&data[0]), str_size);

		++offset;
	}

	for (size_t i = offset; i < end; ++i)
	{
		UInt64 str_size = offsets[i] - offsets[i - 1] - 1;
		writeVarUInt(str_size, ostr);
		ostr.write(reinterpret_cast<const char *>(&data[offsets[i - 1]]), str_size);
	}
}


template <int UNROLL_TIMES>
static NO_INLINE void deserializeBinarySSE2(ColumnString::Chars_t & data, ColumnString::Offsets_t & offsets, ReadBuffer & istr, size_t limit)
{
	size_t offset = data.size();
	for (size_t i = 0; i < limit; ++i)
	{
		if (istr.eof())
			break;

		UInt64 size;
		readVarUInt(size, istr);

		offset += size + 1;
		offsets.push_back(offset);

		data.resize(offset);

		if (size)
		{
			/// Оптимистичная ветка, в которой возможно более эффективное копирование.
			if (offset + 16 * UNROLL_TIMES <= data.capacity() && istr.position() + size + 16 * UNROLL_TIMES <= istr.buffer().end())
			{
				const __m128i * sse_src_pos = reinterpret_cast<const __m128i *>(istr.position());
				const __m128i * sse_src_end = sse_src_pos + (size + (16 * UNROLL_TIMES - 1)) / 16 / UNROLL_TIMES * UNROLL_TIMES;
				__m128i * sse_dst_pos = reinterpret_cast<__m128i *>(&data[offset - size - 1]);

				while (sse_src_pos < sse_src_end)
				{
					/// NOTE gcc 4.9.2 разворачивает цикл, но почему-то использует только один xmm регистр.
					///for (size_t j = 0; j < UNROLL_TIMES; ++j)
					///	_mm_storeu_si128(sse_dst_pos + j, _mm_loadu_si128(sse_src_pos + j));

					sse_src_pos += UNROLL_TIMES;
					sse_dst_pos += UNROLL_TIMES;

					if (UNROLL_TIMES >= 4) __asm__("movdqu %0, %%xmm0" :: "m"(sse_src_pos[-4]));
					if (UNROLL_TIMES >= 3) __asm__("movdqu %0, %%xmm1" :: "m"(sse_src_pos[-3]));
					if (UNROLL_TIMES >= 2) __asm__("movdqu %0, %%xmm2" :: "m"(sse_src_pos[-2]));
					if (UNROLL_TIMES >= 1) __asm__("movdqu %0, %%xmm3" :: "m"(sse_src_pos[-1]));

					if (UNROLL_TIMES >= 4) __asm__("movdqu %%xmm0, %0" : "=m"(sse_dst_pos[-4]));
					if (UNROLL_TIMES >= 3) __asm__("movdqu %%xmm1, %0" : "=m"(sse_dst_pos[-3]));
					if (UNROLL_TIMES >= 2) __asm__("movdqu %%xmm2, %0" : "=m"(sse_dst_pos[-2]));
					if (UNROLL_TIMES >= 1) __asm__("movdqu %%xmm3, %0" : "=m"(sse_dst_pos[-1]));
				}

				istr.position() += size;
			}
			else
			{
				istr.readStrict(reinterpret_cast<char*>(&data[offset - size - 1]), size);
			}
		}

		data[offset - 1] = 0;
	}
}


void DataTypeString::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnString & column_string = typeid_cast<ColumnString &>(column);
	ColumnString::Chars_t & data = column_string.getChars();
	ColumnString::Offsets_t & offsets = column_string.getOffsets();

	/// Выбрано наугад.
	constexpr auto avg_value_size_hint_reserve_multiplier = 1.2;

	double avg_chars_size = (avg_value_size_hint && avg_value_size_hint > sizeof(offsets[0])
		? (avg_value_size_hint - sizeof(offsets[0])) * avg_value_size_hint_reserve_multiplier
		: DBMS_APPROX_STRING_SIZE);

	data.reserve(data.size() + std::ceil(limit * avg_chars_size));

	offsets.reserve(offsets.size() + limit);

	if (avg_chars_size >= 64)
		deserializeBinarySSE2<4>(data, offsets, istr, limit);
	else if (avg_chars_size >= 48)
		deserializeBinarySSE2<3>(data, offsets, istr, limit);
	else if (avg_chars_size >= 32)
		deserializeBinarySSE2<2>(data, offsets, istr, limit);
	else
		deserializeBinarySSE2<1>(data, offsets, istr, limit);
}


void DataTypeString::serializeText(const Field & field, WriteBuffer & ostr) const
{
	writeString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeText(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readString(get<String &>(field), istr);
}


void DataTypeString::serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
{
	writeEscapedString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextEscaped(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readEscapedString(get<String &>(field), istr);
}


void DataTypeString::serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
{
	writeQuotedString(get<const String &>(field), ostr);
}


void DataTypeString::deserializeTextQuoted(Field & field, ReadBuffer & istr) const
{
	field.assignString("", 0);
	readQuotedString(get<String &>(field), istr);
}


void DataTypeString::serializeTextJSON(const Field & field, WriteBuffer & ostr) const
{
	writeJSONString(get<const String &>(field), ostr);
}


ColumnPtr DataTypeString::createColumn() const
{
	return new ColumnString;
}


ColumnPtr DataTypeString::createConstColumn(size_t size, const Field & field) const
{
	return new ColumnConst<String>(size, get<const String &>(field));
}

}
