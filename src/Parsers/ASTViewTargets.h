#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

/// Information about target tables (external or inner) of a materialized view or a window view or a TimeSeries table.
/// See ASTViewTargets for more details.
struct ViewTarget
{
    enum Kind
    {
        /// If `kind == ViewTarget::To` then `ViewTarget` contains information about the "TO" table of a materialized view or a window view:
        ///     CREATE MATERIALIZED VIEW db.mv_name {TO [db.]to_target | ENGINE to_engine} AS SELECT ...
        /// or
        ///     CREATE WINDOW VIEW db.wv_name {TO [db.]to_target | ENGINE to_engine} AS SELECT ...
        To,

        /// If `kind == ViewTarget::Inner` then `ViewTarget` contains information about the "INNER" table of a window view:
        ///     CREATE WINDOW VIEW db.wv_name {INNER ENGINE inner_engine} AS SELECT ...
        Inner,

        /// The "data" table for a TimeSeries table, contains time series.
        Data,

        /// The "tags" table for a TimeSeries table, contains identifiers for each combination of a metric name and tags (labels).
        Tags,

        /// The "metrics" table for a TimeSeries table, contains general information (metadata) about metrics.
        Metrics,
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
