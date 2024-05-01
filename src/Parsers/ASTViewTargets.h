#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

/// Information about the target table for a materialized view or a window view.
struct ViewTarget
{
    enum Kind
    {
        /// Target table for a materialized view or a window view.
        To,

        /// Table with intermediate results for a window view.
        Inner,
    };

    Kind kind = To;

    /// StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    StorageID table_id = StorageID::createEmpty();

    /// UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    UUID inner_uuid = UUIDHelpers::Nil;

    /// Table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    std::shared_ptr<ASTStorage> inner_engine;
};

/// Converts ViewTarget::Kind to a string.
std::string_view toString(ViewTarget::Kind kind);
void parseFromString(ViewTarget::Kind & out, std::string_view str);


/// Information about all the target tables for a view.
class ASTViewTargets : public IAST
{
public:
    std::vector<ViewTarget> targets;

    /// Sets the StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    void setTableID(ViewTarget::Kind kind, const StorageID & table_id_);
    StorageID getTableID(ViewTarget::Kind kind) const;
    bool hasTableID(ViewTarget::Kind kind) const;

    /// Replaces an empty database in the StorageID of the target table with a specified database.
    void setCurrentDatabase(const String & current_database);

    /// Sets the UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    void setInnerUUID(ViewTarget::Kind kind, const UUID & inner_uuid_);
    UUID getInnerUUID(ViewTarget::Kind kind) const;
    bool hasInnerUUID(ViewTarget::Kind kind) const;

    void resetInnerUUIDs();
    bool hasInnerUUIDs() const;

    /// Sets the table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    void setInnerEngine(ViewTarget::Kind kind, ASTPtr storage_def);
    std::shared_ptr<ASTStorage> getInnerEngine(ViewTarget::Kind kind) const;
    std::vector<std::shared_ptr<ASTStorage>> getInnerEngines() const;

    /// Returns a list of all kinds of views in this ASTViewTargets.
    std::vector<ViewTarget::Kind> getKinds() const;

    /// Returns information about a target table.
    /// The function returns null if such target doesn't exist.
    const ViewTarget * tryGetTarget(ViewTarget::Kind kind) const;

    String getID(char) const override { return "ViewTargets"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    /// Formats information only about a specific target table.
    void formatTarget(ViewTarget::Kind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const;
    static void formatTarget(const ViewTarget & target, const FormatSettings & s, FormatState & state, FormatStateStacked frame);

    /// Helper functions for class ParserViewTargets. Returns a prefix keyword matching a specified target kind.
    static std::optional<Keyword> getKeywordForTableID(ViewTarget::Kind kind);
    static std::optional<Keyword> getKeywordForInnerUUID(ViewTarget::Kind kind);
    static std::optional<Keyword> getKeywordForInnerStorage(ViewTarget::Kind kind);

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

}
