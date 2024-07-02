#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

/// Information about the target table for a materialized view, or a window view, or a TimeSeries table.
struct ViewTarget
{
    enum class Kind
    {
        /// Target table for a materialized view or a window view.
        Default,

        /// Table with intermediate results for a window view.
        Inner,

        /// The "data" table for a TimeSeries table, contains time series.
        Data,

        /// The "tags" table for a TimeSeries table, contains identifiers for each combination of a metric name and tags (labels).
        Tags,

        /// The "metrics" table for a TimeSeries table, contains general information (metadata) about metrics.
        Metrics,
    };

    Kind kind = Kind::Default;

    /// StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    StorageID table_id = StorageID::createEmpty();

    /// UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    UUID inner_uuid = UUIDHelpers::Nil;

    /// Table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    std::shared_ptr<ASTStorage> table_engine;
};

/// Converts ViewTarget::Kind to a string.
std::string_view toString(ViewTarget::Kind kind);
void parseFromString(ViewTarget::Kind & out, std::string_view str);


/// Information about all the target tables for a view.
class ASTViewTargets : public IAST
{
public:
    using TargetKind = ViewTarget::Kind;
    std::vector<ViewTarget> targets;

    /// Returns information about a target table.
    /// The function returns null if such target doesn't exist.
    const ViewTarget * tryGetTarget(TargetKind kind = TargetKind::Default) const;

    /// Sets the StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    void setTableID(TargetKind kind, const StorageID & table_id_);
    void setTableID(const StorageID & table_id_) { setTableID(TargetKind::Default, table_id_); }
    StorageID getTableID(TargetKind kind = TargetKind::Default) const;
    bool hasTableID(TargetKind kind = TargetKind::Default) const;

    /// Replaces an empty database in the StorageID of the target table with a specified database.
    void setCurrentDatabase(const String & current_database);

    /// Sets the UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    void setInnerUUID(TargetKind kind, const UUID & inner_uuid_);
    void setInnerUUID(const UUID & inner_uuid_) { setInnerUUID(TargetKind::Default, inner_uuid_); }
    UUID getInnerUUID(TargetKind kind = TargetKind::Default) const;
    void resetInnerUUIDs();

    /// Sets the table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    void setTableEngine(TargetKind kind, ASTPtr storage_def);
    void setTableEngine(ASTPtr storage_def) { setTableEngine(TargetKind::Default, storage_def); }
    std::shared_ptr<ASTStorage> getTableEngine(TargetKind kind = TargetKind::Default) const;

    String getID(char) const override { return "ViewTargets"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    /// Formats information only about a specific target table.
    void formatTarget(TargetKind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const;
    static void formatTarget(const ViewTarget & target, const FormatSettings & s, FormatState & state, FormatStateStacked frame);

    /// Helper functions for class ParserViewTargets. Assumes the kind is Data or Tags or Metrics.
    static Keyword kindToKeywordForTableID(TargetKind kind);
    static Keyword kindToKeywordForInnerUUID(TargetKind kind);
    static Keyword kindToPrefixForInnerStorage(TargetKind kind);

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

}
