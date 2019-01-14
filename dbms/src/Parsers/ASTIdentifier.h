#pragma once

#include <optional>

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Identifier (column, table or alias)
class ASTIdentifier : public ASTWithAlias
{
    enum Kind    /// TODO This is semantic, not syntax. Remove it.
    {
        General,
        Special, // Database, Table, Format
    };

public:
    /// name. The composite identifier here will have a concatenated name (of the form a.b.c), and individual components will be available inside the children.
    String name;

    ASTIdentifier(const String & name_, const Kind kind_ = General)
        : name(name_), kind(kind_) { range = StringRange(name.data(), name.data() + name.size()); }

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Identifier" + (delim + name); }

    ASTPtr clone() const override { return std::make_shared<ASTIdentifier>(*this); }

    void collectIdentifierNames(IdentifierNameSet & set) const override
    {
        set.insert(name);
    }

    static std::shared_ptr<ASTIdentifier> createSpecial(const String & name_)
    {
        return std::make_shared<ASTIdentifier>(name_, ASTIdentifier::Special);
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    Kind kind;

    void setSpecial() { kind = Special; }
    bool special() const { return kind == Special; }

    friend void setIdentifierSpecial(ASTPtr &);
    friend std::optional<String> getColumnIdentifierName(const ASTIdentifier & node);
    friend std::optional<String> getColumnIdentifierName(const ASTPtr & ast);
    friend std::optional<String> getTableIdentifierName(const ASTIdentifier & node);
    friend std::optional<String> getTableIdentifierName(const ASTPtr & ast);
};


/// ASTIdentifier Helpers: hide casts and semantic.

bool isIdentifier(const IAST * const ast);
inline bool isIdentifier(const ASTPtr & ast) { return isIdentifier(ast.get()); }

std::optional<String> getIdentifierName(const IAST * const ast);
inline std::optional<String> getIdentifierName(const ASTPtr & ast) { return getIdentifierName(ast.get()); }
bool getIdentifierName(const ASTPtr & ast, String & name);

/// @returns name for column identifiers
std::optional<String> getColumnIdentifierName(const ASTIdentifier & node);
std::optional<String> getColumnIdentifierName(const ASTPtr & ast);

/// @returns name for 'not a column' identifiers
std::optional<String> getTableIdentifierName(const ASTIdentifier & node);
std::optional<String> getTableIdentifierName(const ASTPtr & ast);

void setIdentifierSpecial(ASTPtr & ast);

}
