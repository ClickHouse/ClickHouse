#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Идентификатор (столбца или алиас)
  */
class ASTIdentifier : public ASTWithAlias
{
public:
    enum Kind    /// TODO This is semantic, not syntax. Remove it.
    {
        Column,
        Database,
        Table,
        Format,
    };

    /// имя. У составного идентификатора здесь будет конкатенированное имя (вида a.b.c), а отдельные составляюшие будут доступны внутри children.
    String name;

    /// чего идентифицирует этот идентификатор
    Kind kind;

    ASTIdentifier() = default;
    ASTIdentifier(const StringRange range_, const String & name_, const Kind kind_ = Column)
        : ASTWithAlias(range_), name(name_), kind(kind_) {}

    String getColumnName() const override { return name; }

    /** Получить текст, который идентифицирует этот элемент. */
    String getID() const override { return "Identifier_" + name; }

    ASTPtr clone() const override { return std::make_shared<ASTIdentifier>(*this); }

    void collectIdentifierNames(IdentifierNameSet & set) const override
    {
        set.insert(name);
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
