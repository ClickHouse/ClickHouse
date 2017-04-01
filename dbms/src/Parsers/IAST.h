#pragma once

#include <set>
#include <memory>
#include <ostream>

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Parsers/StringRange.h>


class SipHash;


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_A_COLUMN;
    extern const int UNKNOWN_TYPE_OF_AST_NODE;
    extern const int UNKNOWN_ELEMENT_IN_AST;
}

using IdentifierNameSet = std::set<String>;

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
using ASTs = std::vector<ASTPtr>;

class WriteBuffer;


/** Элемент синтаксического дерева (в дальнейшем - направленного ациклического графа с элементами семантики)
  */
class IAST
{
public:
    ASTs children;
    StringRange range;

    /** Строка с полным запросом.
      * Этот указатель не дает ее удалить, пока range в нее ссылается.
      */
    StringPtr query_string;

    IAST() = default;
    IAST(const StringRange range_) : range(range_) {}
    virtual ~IAST() = default;

    /** Получить каноническое имя столбца, если элемент является столбцом */
    virtual String getColumnName() const { throw Exception("Trying to get name of not a column: " + getID(), ErrorCodes::NOT_A_COLUMN); }

    /** Получить алиас, если он есть, или каноническое имя столбца, если его нет. */
    virtual String getAliasOrColumnName() const { return getColumnName(); }

    /** Получить алиас, если он есть, или пустую строку, если его нет, или если элемент не поддерживает алиасы. */
    virtual String tryGetAlias() const { return String(); }

    /** Установить алиас. */
    virtual void setAlias(const String & to)
    {
        throw Exception("Can't set alias of " + getColumnName(), ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
    }

    /** Получить текст, который идентифицирует этот элемент. */
    virtual String getID() const = 0;

    /** Получить глубокую копию дерева. */
    virtual ASTPtr clone() const = 0;

    /** Get text, describing and identifying this element and its subtree.
      * Usually it consist of element's id and getTreeID of all children.
      */
    String getTreeID() const;
    void getTreeIDImpl(WriteBuffer & out) const;

    /** Get hash code, identifying this element and its subtree.
      */
    using Hash = std::pair<UInt64, UInt64>;
    Hash getTreeHash() const;
    void getTreeHashImpl(SipHash & hash_state) const;

    void dumpTree(std::ostream & ostr, size_t indent = 0) const
    {
        String indent_str(indent, '-');
        ostr << indent_str << getID() << ", " << this << std::endl;
        for (const auto & child : children)
            child->dumpTree(ostr, indent + 1);
    }

    /** Проверить глубину дерева.
      * Если задано max_depth и глубина больше - кинуть исключение.
      * Возвращает глубину дерева.
      */
    size_t checkDepth(size_t max_depth) const
    {
        return checkDepthImpl(max_depth, 0);
    }

    /** То же самое для общего количества элементов дерева.
      */
    size_t checkSize(size_t max_size) const;

    /**  Получить set из имен индентификаторов
     */
    virtual void collectIdentifierNames(IdentifierNameSet & set) const
    {
        for (const auto & child : children)
            child->collectIdentifierNames(set);
    }


    /// Преобразовать в строку.

    /// Настройки формата.
    struct FormatSettings
    {
        std::ostream & ostr;
        bool hilite;
        bool one_line;

        char nl_or_ws;

        FormatSettings(std::ostream & ostr_, bool hilite_, bool one_line_)
            : ostr(ostr_), hilite(hilite_), one_line(one_line_)
        {
            nl_or_ws = one_line ? ' ' : '\n';
        }
    };

    /// Состояние. Например, может запоминаться множество узлов, которых мы уже обошли.
    struct FormatState
    {
        /** Запрос SELECT, в котором найден алиас; идентификатор узла с таким алиасом.
          * Нужно, чтобы когда узел встретился повторно, выводить только алиас.
          */
        std::set<std::pair<const IAST *, std::string>> printed_asts_with_alias;
    };

    /// Состояние, которое копируется при форматировании каждого узла. Например, уровень вложенности.
    struct FormatStateStacked
    {
        UInt8 indent = 0;
        bool need_parens = false;
        const IAST * current_select = nullptr;
    };

    void format(const FormatSettings & settings) const
    {
        FormatState state;
        formatImpl(settings, state, FormatStateStacked());
    }

    virtual void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
    {
        throw Exception("Unknown element in AST: " + getID()
            + ((range.first && (range.second > range.first))
                ? " '" + std::string(range.first, range.second - range.first) + "'"
                : ""),
            ErrorCodes::UNKNOWN_ELEMENT_IN_AST);
    }

    void writeAlias(const String & name, std::ostream & s, bool hilite) const;

protected:
    /// Для подсветки синтаксиса.
    static const char * hilite_keyword;
    static const char * hilite_identifier;
    static const char * hilite_function;
    static const char * hilite_operator;
    static const char * hilite_alias;
    static const char * hilite_none;

private:
    size_t checkDepthImpl(size_t max_depth, size_t level) const;
};


/// Квотировать идентификатор обратными кавычками, если это требуется.
String backQuoteIfNeed(const String & x);


}
