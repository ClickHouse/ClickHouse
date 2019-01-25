#pragma once

#include <optional>

#include <Parsers/ASTWithAlias.h>


namespace DB
{

/// Identifier (column, table or alias)
class ASTIdentifier : public ASTWithAlias
{
public:
    /// The composite identifier will have a concatenated name (of the form a.b.c),
    /// and individual components will be available inside the name_parts.
    String name;
    std::vector<String> name_parts;

    ASTIdentifier(const String & name_, std::vector<String> && name_parts_ = {})
        : name(name_)
        , name_parts(name_parts_)
        , special(false)
    {
    }

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Identifier" + (delim + name); }

    ASTPtr clone() const override { return std::make_shared<ASTIdentifier>(*this); }

    void collectIdentifierNames(IdentifierNameSet & set) const override
    {
        set.insert(name);
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    using ASTWithAlias::children; /// ASTIdentifier is child free

    bool special; /// TODO: it would be ptr to semantic here

    static std::shared_ptr<ASTIdentifier> createSpecial(const String & name, std::vector<String> && name_parts = {})
    {
        auto ret = std::make_shared<ASTIdentifier>(name, std::move(name_parts));
        ret->special = true;
        return ret;
    }

    void setSpecial() { special = true; }

    friend void setIdentifierSpecial(ASTPtr &);
    friend std::optional<String> getColumnIdentifierName(const ASTIdentifier & node);
    friend std::optional<String> getColumnIdentifierName(const ASTPtr & ast);
    friend std::optional<String> getTableIdentifierName(const ASTIdentifier & node);
    friend std::optional<String> getTableIdentifierName(const ASTPtr & ast);
    friend ASTPtr createTableIdentifier(const String & database_name, const String & table_name);
};


/// ASTIdentifier Helpers: hide casts and semantic.

ASTPtr createTableIdentifier(const String & database_name, const String & table_name);

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
void addIdentifierQualifier(ASTIdentifier & identifier, const String & database, const String & table, const String & alias);
bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & table_or_alias);
bool doesIdentifierBelongTo(const ASTIdentifier & identifier, const String & database, const String & table);

}
