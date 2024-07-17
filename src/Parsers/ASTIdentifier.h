#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

struct IdentifierSemantic;
struct IdentifierSemanticImpl;
struct StorageID;

class ASTTableIdentifier;

/// FIXME: rewrite code about params - they should be substituted at the parsing stage,
///        or parsed as a separate AST entity.

/// Generic identifier. ASTTableIdentifier - for table identifier.
class ASTIdentifier : public ASTWithAlias
{
public:
    explicit ASTIdentifier(const String & short_name, ASTPtr && name_param = {});
    explicit ASTIdentifier(std::vector<String> && name_parts, bool special = false, ASTs && name_params = {});

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "Identifier" + (delim + name()); }

    /** Get the query param out of a non-compound identifier. */
    ASTPtr getParam() const;

    ASTPtr clone() const override;

    void collectIdentifierNames(IdentifierNameSet & set) const override { set.insert(name()); }

    bool compound() const { return name_parts.size() > 1; }
    bool isShort() const { return name_parts.size() == 1; }
    bool supposedToBeCompound() const;  // TODO(ilezhankin): get rid of this

    void setShortName(const String & new_name);

    /// The composite identifier will have a concatenated name (of the form a.b.c),
    /// and individual components will be available inside the name_parts.
    const String & shortName() const { return name_parts.back(); }
    const String & name() const;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_alias) const override;

    void restoreTable();  // TODO(ilezhankin): get rid of this
    std::shared_ptr<ASTTableIdentifier> createTable() const;  // returns |nullptr| if identifier is not table.

    String full_name;
    std::vector<String> name_parts;

protected:
    std::shared_ptr<IdentifierSemanticImpl> semantic; /// pimpl

    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    using ASTWithAlias::children; /// ASTIdentifier is child free

    friend class ASTTableIdentifier;
    friend class ReplaceQueryParameterVisitor;
    friend struct IdentifierSemantic;
    friend void setIdentifierSpecial(ASTPtr & ast);

    void resetFullName();
};

class ASTTableIdentifier : public ASTIdentifier
{
public:
    explicit ASTTableIdentifier(const String & table_name, ASTs && name_params = {});
    explicit ASTTableIdentifier(const StorageID & table_id, ASTs && name_params = {});
    ASTTableIdentifier(const String & database_name, const String & table_name, ASTs && name_params = {});

    String getID(char delim) const override { return "TableIdentifier" + (delim + name()); }
    ASTPtr clone() const override;

    UUID uuid = UUIDHelpers::Nil;  // FIXME(ilezhankin): make private

    StorageID getTableId() const;
    String getDatabaseName() const;

    ASTPtr getTable() const;
    ASTPtr getDatabase() const;

    // FIXME: used only when it's needed to rewrite distributed table name to real remote table name.
    void resetTable(const String & database_name, const String & table_name);  // TODO(ilezhankin): get rid of this

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
};

}
