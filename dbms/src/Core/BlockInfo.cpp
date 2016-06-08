#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/VarInt.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Core/BlockInfo.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_BLOCK_INFO_FIELD;
}


/// Записать значения в бинарном виде. NOTE: Можно было бы использовать protobuf, но он был бы overkill для данного случая.
void BlockInfo::write(WriteBuffer & out) const
{
	/// Набор пар FIELD_NUM, значение в бинарном виде. Затем 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
	writeVarUInt(FIELD_NUM, out); \
	writeBinary(NAME, out);

	APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD);

#undef WRITE_FIELD
	writeVarUInt(0, out);
}

/// Прочитать значения в бинарном виде.
void BlockInfo::read(ReadBuffer & in)
{
	UInt64 field_num = 0;

	while (true)
	{
		readVarUInt(field_num, in);
		if (field_num == 0)
			break;

		switch (field_num)
		{
		#define READ_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
			case FIELD_NUM: \
				readBinary(NAME, in); \
				break;

			APPLY_FOR_BLOCK_INFO_FIELDS(READ_FIELD);

		#undef READ_FIELD
			default:
				throw Exception("Unknown BlockInfo field number: " + toString(field_num), ErrorCodes::UNKNOWN_BLOCK_INFO_FIELD);
		}
	}
}

}
