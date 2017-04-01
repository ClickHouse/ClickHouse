#pragma once

#include <DataStreams/BlockIO.h>


namespace DB
{

/** Интерфейс интерпретаторов разных запросов.
  */
class IInterpreter
{
public:
    /** Для запросов, возвращающих результат (SELECT и похожие), устанавливает в BlockIO поток, из которого можно будет читать этот результат.
      * Для запросов, принимающих данные (INSERT), устанавливает в BlockIO поток, куда можно писать данные.
      * Для запросов, которые не требуют данные и ничего не возвращают, BlockIO будет пустым.
      */
    virtual BlockIO execute() = 0;

    virtual ~IInterpreter() {}
};

}
