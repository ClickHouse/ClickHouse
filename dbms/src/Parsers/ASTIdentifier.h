#pragma once

#include <optional>

#include <Parsers/ASTWithAlias.h>


namespace DB
{

struct IdentifierSemantic;
struct IdentifierSemanticImpl;
struct DatabaseAndTableWithAlias;


/// Identifier (column, table or alias)
class ASTIdentifier : public ASTWithAlias
{
public:
    /// The composite identifier will have a concatenated name (of the form a.b.c),
    /// and individual components will be available inside the name_parts.
    String name;

    ASTIdentifier(const String & name_, std::vector<String> && name_parts_ = {});
    ASTIdentifier(std::vector<String> && name_parts_);

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Identifier" + (delim + name); }

    ASTPtr clone() const override;

    void collectIdentifierNames(IdentifierNameSet & set) const override
    {
        set.insert(name);
    }

    bool compound() const { return !name_parts.empty(); }
    bool isShort() const { return name_parts.empty() || name == name_parts.back(); }

    void setShortName(const String & new_name);

    const String & shortName() const
    {
        if (!name_parts.empty())
            return name_parts.back();
        return name;
    }

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    using ASTWithAlias::children; /// ASTIdentifier is child free

    std::vector<String> name_parts;
    std::shared_ptr<IdentifierSemanticImpl> semantic; /// pimpl

    static std::shared_ptr<ASTIdentifier> createSpecial(const String & name, std::vector<String> && name_parts = {});

    friend struct IdentifierSemantic;
    friend ASTPtr createTableIdentifier(const String & database_name, const String & table_name);
    friend void setIdentifierSpecial(ASTPtr & ast);
};


/// ASTIdentifier Helpers: hide casts and semantic.

ASTPtr createTableIdentifier(const String & database_name, const String & table_name);
void setIdentifierSpecial(ASTPtr & ast);

std::optional<String> getIdentifierName(const IAST * const ast);
inline std::optional<String> getIdentifierName(const ASTPtr & ast) { return getIdentifierName(ast.get()); }
bool getIdentifierName(const ASTPtr & ast, String & name);


}
