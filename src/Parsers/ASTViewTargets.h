#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

/// Information about target tables (external or inner) of a materialized view or a window view or a TimeSeries table.
/// See ASTViewTargets for more details.
class ASTViewTarget : public IAST
{
public:
    /// Identifies the type of view target
    enum Kind
    {
        /// If `kind == ASTViewTarget::Kind::To` then `ASTViewTarget` contains information about the "TO" table of a materialized view or a window view:
        ///     CREATE MATERIALIZED VIEW db.mv_name {TO [db.]to_target | ENGINE to_engine} AS SELECT ...
        /// or
        ///     CREATE WINDOW VIEW db.wv_name {TO [db.]to_target | ENGINE to_engine} AS SELECT ...
        To,

        /// If `kind == ASTViewTarget::Kind::Inner` then `ASTViewTarget` contains information about the "INNER" table of a window view:
        ///     CREATE WINDOW VIEW db.wv_name {INNER ENGINE inner_engine} AS SELECT ...
        Inner,

        /// The "data" table for a TimeSeries table, contains time series.
        Data,

        /// The "tags" table for a TimeSeries table, contains identifiers for each combination of a metric name and tags (labels).
        Tags,

        /// The "metrics" table for a TimeSeries table, contains general information (metadata) about metrics.
        Metrics,
    };

    explicit ASTViewTarget(Kind kind_) : kind(kind_) {}

    Kind kind = To;

    /// StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    ASTTableIdentifier * table_identifier = nullptr;

    /// UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    UUID inner_uuid = UUIDHelpers::Nil;

    /// Table engine of the target table, if it's inner.
    /// This will be in the children array if set
    ASTStorage * inner_engine = nullptr;

    /// Gets the StorageID from the table_identifier
    StorageID getTableID() const;
    bool hasTableID() const { return table_identifier != nullptr; }

    /// Check if this target has an inner UUID set
    bool hasInnerUUID() const { return inner_uuid != UUIDHelpers::Nil; }

    /// Check if this target has an inner engine defined
    bool hasInnerEngine() const { return inner_engine != nullptr; }

    String getID(char) const override { return "ViewTarget"; }
    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

/// Converts ViewTarget::Kind to a string.
std::string_view toString(ASTViewTarget::Kind kind);
void parseFromString(ASTViewTarget::Kind & out, std::string_view str);

/// Information about all target tables (external or inner) of a view.
///
/// For example, for a materialized view:
///     CREATE MATERIALIZED VIEW db.mv_name [TO [db.]to_target | ENGINE to_engine] AS SELECT ...
/// this class contains information about the "TO" table: its name and database (if it's external), its UUID and engine (if it's inner).
///
/// For a window view:
///     CREATE WINDOW VIEW db.wv_name [TO [db.]to_target | ENGINE to_engine] [INNER ENGINE inner_engine] AS SELECT ...
/// this class contains information about both the "TO" table and the "INNER" table.
class ASTViewTargets : public IAST
{
public:
    /// Sets the TableIdentifier of the target table.
    /// That table can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    void setTableID(ASTViewTarget::Kind kind, ASTPtr table_identifier);

    /// For backward compatibility - converts StorageID to a table identifier and sets it
    void setTableID(ASTViewTarget::Kind kind, const StorageID & table_id_);

    /// Get the StorageID for the specified kind of target
    StorageID getTableID(ASTViewTarget::Kind kind) const;
    bool hasTableID(ASTViewTarget::Kind kind) const;

    /// Replaces an empty database in the table identifiers of the targets with a specified database.
    void setCurrentDatabase(const String & current_database);

    /// Sets the UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    void setInnerUUID(ASTViewTarget::Kind kind, const UUID & inner_uuid_);
    UUID getInnerUUID(ASTViewTarget::Kind kind) const;
    bool hasInnerUUID(ASTViewTarget::Kind kind) const;

    void resetInnerUUIDs();
    bool hasInnerUUIDs() const;

    /// Sets the table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    void setInnerEngine(ASTViewTarget::Kind kind, ASTPtr storage_def);
    std::shared_ptr<ASTStorage> getInnerEngine(ASTViewTarget::Kind kind) const;
    std::vector<std::shared_ptr<ASTStorage>> getInnerEngines() const;

    /// Returns a list of all kinds of views in this ASTViewTargets.
    std::vector<ASTViewTarget::Kind> getKinds() const;

    /// Returns information about a target table.
    /// The function returns null if such target doesn't exist.
    const ASTViewTarget * tryGetTarget(ASTViewTarget::Kind kind) const;

    String getID(char) const override { return "ViewTargets"; }

    ASTPtr clone() const override;

    /// Formats information only about a specific target table.
    void formatTarget(ASTViewTarget::Kind kind, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const;
    static void formatTarget(const ASTViewTarget & target, WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame);

    /// Helper functions for class ParserViewTargets. Returns a prefix keyword matching a specified target kind.
    static std::optional<Keyword> getKeywordForTableID(ASTViewTarget::Kind kind);
    static std::optional<Keyword> getKeywordForInnerUUID(ASTViewTarget::Kind kind);
    static std::optional<Keyword> getKeywordForInnerStorage(ASTViewTarget::Kind kind);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
