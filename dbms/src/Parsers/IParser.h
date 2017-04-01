#pragma once

#include <list>
#include <memory>

#include <Core/Defines.h>
#include <Core/Types.h>
#include <Parsers/IAST.h>


namespace DB
{

using Expected = const char *;


/** Интерфейс для классов-парсеров
  */
class IParser
{
public:
    using Pos = const char *;

    /** Получить текст о том, что парсит этот парсер. */
    virtual const char * getName() const = 0;

    /** Распарсить кусок текста с позиции pos, но не дальше конца строки (end - позиция после конца строки),
      * переместить указатель pos на максимальное место, до которого удалось распарсить,
      * вернуть в случае успеха true и результат в node, если он нужен, иначе false,
      * в expected записать, что ожидалось в максимальной позиции,
      *  до которой удалось распарсить, если парсинг был неуспешным,
      *  или что парсит этот парсер, если парсинг был успешным.
      * Строка, в которую входит диапазон [begin, end) может быть не 0-terminated.
      */
    virtual bool parse(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) = 0;

    bool ignore(Pos & pos, Pos end, Pos & max_parsed_pos, Expected & expected)
    {
        ASTPtr ignore_node;
        return parse(pos, end, ignore_node, max_parsed_pos, expected);
    }

    bool ignore(Pos & pos, Pos end)
    {
        Pos max_parsed_pos = pos;
        Expected expected;
        return ignore(pos, end, max_parsed_pos, expected);
    }

    /** То же самое, но не двигать позицию и не записывать результат в node.
      */
    bool check(Pos & pos, Pos end, Pos & max_parsed_pos, Expected & expected)
    {
        Pos begin = pos;
        ASTPtr node;
        if (!parse(pos, end, node, max_parsed_pos, expected))
        {
            pos = begin;
            return false;
        }
        else
            return true;
    }

    virtual ~IParser() {}
};

using ParserPtr = std::unique_ptr<IParser>;

}
