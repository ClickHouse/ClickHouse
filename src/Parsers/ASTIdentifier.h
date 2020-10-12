#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTWithAlias.h>

#include <optional>


namespace DB
{

struct IdentifierSemantic;
struct IdentifierSemanticImpl;
struct StorageID;


/// Identifier (column, table or alias)
class ASTIdentifier : public ASTWithAlias
{
public:
    UUID uuid = UUIDHelpers::Nil;

    explicit ASTIdentifier(const String & short_name);
    explicit ASTIdentifier(std::vector<String> && name_parts_);

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Identifier" + (delim + fullName()); }

    ASTPtr clone() const override;

    void collectIdentifierNames(IdentifierNameSet & set) const override { set.insert(fullName()); }

    bool compound() const { return name_parts.size() > 1; }
    bool isShort() const { return name_parts.size() == 1; }

    void setShortName(const String & new_name);

    /// The composite identifier will have a concatenated name (of the form a.b.c),
    /// and individual components will be available inside the name_parts.
    const String & fullName() const;
    const String & shortName() const { return name_parts.back(); }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    mutable String name;  // cached full name constructed from name parts
    std::vector<String> name_parts;

    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    using ASTWithAlias::children; /// ASTIdentifier is child free

    std::shared_ptr<IdentifierSemanticImpl> semantic; /// pimpl

    friend struct IdentifierSemantic;
    friend void setIdentifierSpecial(ASTPtr & ast);
    friend StorageID getTableIdentifier(const ASTPtr & ast);
};

class ASTTableIdentifier : public ASTIdentifier
{
    public:
        explicit ASTTableIdentifier(const String & table);
        explicit ASTTableIdentifier(const StorageID & id);
        ASTTableIdentifier(const String & database, const String & table);

        String getID(char delim) const override { return "TableIdentifier" + (delim + fullName()); }

        StorageID getStorageId() const;

        void setDatabase(const String & database);
        void setTable(const String & table);
};


/// ASTIdentifier Helpers: hide casts and semantic.

void setIdentifierSpecial(ASTPtr & ast);

String getIdentifierName(const IAST * ast);
std::optional<String> tryGetIdentifierName(const IAST * ast);
bool tryGetIdentifierNameInto(const IAST * ast, String & name);

inline String getIdentifierName(const ASTPtr & ast) { return getIdentifierName(ast.get()); }
inline std::optional<String> tryGetIdentifierName(const ASTPtr & ast) { return tryGetIdentifierName(ast.get()); }
inline bool tryGetIdentifierNameInto(const ASTPtr & ast, String & name) { return tryGetIdentifierNameInto(ast.get(), name); }

}
