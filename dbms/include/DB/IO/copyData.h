#ifndef DBMS_IO_COPY_DATA_H
#define DBMS_IO_COPY_DATA_H

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>


namespace DB
{

/** Копирует данные из ReadBuffer в WriteBuffer
  */
void copyData(ReadBuffer & from, WriteBuffer & to);

}

#endif
