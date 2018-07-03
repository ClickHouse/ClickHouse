#pragma once

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/** Identifier (column or alias)
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

    /// name. The composite identifier here will have a concatenated name (of the form a.b.c), and individual components will be available inside the children.
    String name;

    /// what this identifier identifies
    Kind kind;

    ASTIdentifier(const String & name_, const Kind kind_ = Column)
        : name(name_), kind(kind_) { range = StringRange(name.data(), name.data() + name.size()); }

    /** Get the text that identifies this element. */
    String getID() const override { return "Identifier_" + name; }

    ASTPtr clone() const override { return std::make_shared<ASTIdentifier>(*this); }

    void collectIdentifierNames(IdentifierNameSet & set) const override
    {
        set.insert(name);
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

}
