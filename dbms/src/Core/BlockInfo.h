#pragma once

#include <Core/Types.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

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

#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool,     is_overflows,     false,     1) \
    M(Int32,    bucket_num,     -1,     2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Записать значения в бинарном виде. NOTE: Можно было бы использовать protobuf, но он был бы overkill для данного случая.
    void write(WriteBuffer & out) const;

    /// Прочитать значения в бинарном виде.
    void read(ReadBuffer & in);
};

}
