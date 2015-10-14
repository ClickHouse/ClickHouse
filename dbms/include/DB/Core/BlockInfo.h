#pragma once

#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/VarInt.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{

/** Дополнительная информация о блоке.
  */
struct BlockInfo
{
	/** is_overflows:
	  * После выполнения GROUP BY ... WITH TOTALS с настройками max_rows_to_group_by и group_by_overflow_mode = 'any',
	  *  в отдельный блок засовывается строчка с аргегированными значениями, не прошедшими max_rows_to_group_by.
	  * Если это такой блок, то для него is_overflows выставляется в true.
	  */

	/** bucket_num:
	  * При использовании двухуровневого метода агрегации, данные с разными группами ключей раскидываются по разным корзинам.
	  * В таком случае здесь указывается номер корзины. Он используется для оптимизации слияния при распределённой аргегации.
	  * Иначе - -1.
	  */

#define APPLY_FOR_INFO_FIELDS(M) \
	M(bool, 	is_overflows, 	false, 	1) \
	M(Int32,	bucket_num, 	-1, 	2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
	TYPE NAME = DEFAULT;

	APPLY_FOR_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

	/// Записать значения в бинарном виде. NOTE: Можно было бы использовать protobuf, но он был бы overkill для данного случая.
	void write(WriteBuffer & out) const
	{
		/// Набор пар FIELD_NUM, значение в бинарном виде. Затем 0.
	#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
		writeVarUInt(FIELD_NUM, out); \
		writeBinary(NAME, out);

		APPLY_FOR_INFO_FIELDS(WRITE_FIELD);

	#undef WRITE_FIELD
		writeVarUInt(0, out);
	}

	/// Прочитать значения в бинарном виде.
	void read(ReadBuffer & in)
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

				APPLY_FOR_INFO_FIELDS(READ_FIELD);

			#undef READ_FIELD
				default:
					throw Exception("Unknown BlockInfo field number: " + toString(field_num), ErrorCodes::UNKNOWN_BLOCK_INFO_FIELD);
			}
		}
	}

#undef APPLY_FOR_INFO_FIELDS
};

}
