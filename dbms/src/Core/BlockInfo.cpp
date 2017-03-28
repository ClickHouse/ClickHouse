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


/// Write values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
void BlockInfo::write(WriteBuffer & out) const
{
 /// Set of pairs `FIELD_NUM`, value in binary form. Then 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
	writeVarUInt(FIELD_NUM, out); \
	writeBinary(NAME, out);

	APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD);

#undef WRITE_FIELD
	writeVarUInt(0, out);
}

/// Read values in binary form.
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
