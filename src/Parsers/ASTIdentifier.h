#pragma once

#include <optional>

#include <Parsers/ASTWithAlias.h>
#include <Core/UUID.h>


namespace DB
{

struct IdentifierSemantic;
struct IdentifierSemanticImpl;
struct StorageID;


/// Identifier (column, table or alias)
class ASTIdentifier : public ASTWithAlias
{
public:
    /// The composite identifier will have a concatenated name (of the form a.b.c),
    /// and individual components will be available inside the name_parts.
    String name;
    UUID uuid = UUIDHelpers::Nil;

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

    /// Restore name field from name_parts in case it was cropped by analyzer but we need a full form for future (re)analyze.
    void restoreCompoundName();

    const String & shortName() const
    {
        if (!name_parts.empty())
            return name_parts.back();
        return name;
    }

    void resetTable(const String & database_name, const String & table_name);

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    using ASTWithAlias::children; /// ASTIdentifier is child free

    std::vector<String> name_parts;
    std::shared_ptr<IdentifierSemanticImpl> semantic; /// pimpl

    static std::shared_ptr<ASTIdentifier> createSpecial(const String & name, std::vector<String> && name_parts = {});

    friend struct IdentifierSemantic;
    friend ASTPtr createTableIdentifier(const StorageID & table_id);
    friend void setIdentifierSpecial(ASTPtr & ast);
    friend StorageID getTableIdentifier(const ASTPtr & ast);
};


/// ASTIdentifier Helpers: hide casts and semantic.

ASTPtr createTableIdentifier(const String & database_name, const String & table_name);
ASTPtr createTableIdentifier(const StorageID & table_id);
void setIdentifierSpecial(ASTPtr & ast);

String getIdentifierName(const IAST * ast);
std::optional<String> tryGetIdentifierName(const IAST * ast);
bool tryGetIdentifierNameInto(const IAST * ast, String & name);
StorageID getTableIdentifier(const ASTPtr & ast);

inline String getIdentifierName(const ASTPtr & ast) { return getIdentifierName(ast.get()); }
inline std::optional<String> tryGetIdentifierName(const ASTPtr & ast) { return tryGetIdentifierName(ast.get()); }
inline bool tryGetIdentifierNameInto(const ASTPtr & ast, String & name) { return tryGetIdentifierNameInto(ast.get(), name); }

}
