#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ASTViewTargets.h>
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ASTRefreshStrategy.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class ASTFunction;
class ASTSetQuery;
class ASTSelectWithUnionQuery;
struct CreateQueryUUIDs;


class ASTStorage : public IAST
{
public:
    ASTFunction * engine = nullptr;
    IAST * partition_by = nullptr;
    IAST * primary_key = nullptr;
    IAST * order_by = nullptr;
    IAST * sample_by = nullptr;
    IAST * ttl_table = nullptr;
    ASTSetQuery * settings = nullptr;

    String getID(char) const override { return "Storage definition"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    bool isExtendedStorageDefinition() const;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&engine));
        f(reinterpret_cast<void **>(&partition_by));
        f(reinterpret_cast<void **>(&primary_key));
        f(reinterpret_cast<void **>(&order_by));
        f(reinterpret_cast<void **>(&sample_by));
        f(reinterpret_cast<void **>(&ttl_table));
        f(reinterpret_cast<void **>(&settings));
    }
};


class ASTExpressionList;

class ASTColumns : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;
    ASTExpressionList * projections = nullptr;
    IAST              * primary_key = nullptr;
    IAST              * primary_key_from_columns = nullptr;

    String getID(char) const override { return "Columns definition"; }

    ASTPtr clone() const override;

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    bool empty() const
    {
        return (!columns || columns->children.empty()) && (!indices || indices->children.empty()) && (!constraints || constraints->children.empty())
            && (!projections || projections->children.empty());
    }

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&columns));
        f(reinterpret_cast<void **>(&indices));
        f(reinterpret_cast<void **>(&primary_key));
        f(reinterpret_cast<void **>(&constraints));
        f(reinterpret_cast<void **>(&projections));
        f(reinterpret_cast<void **>(&primary_key_from_columns));
    }
};


/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool attach{false};    /// Query ATTACH TABLE, not CREATE TABLE.
    bool if_not_exists{false};
    bool is_ordinary_view{false};
    bool is_materialized_view{false};
    bool is_live_view{false};
    bool is_window_view{false};
    bool is_time_series_table{false}; /// CREATE TABLE ... ENGINE=TimeSeries() ...
    bool is_populate{false};
    bool is_create_empty{false};    /// CREATE TABLE ... EMPTY AS SELECT ...
    bool is_clone_as{false};    /// CREATE TABLE ... CLONE AS ...
    bool replace_view{false}; /// CREATE OR REPLACE VIEW
    bool has_uuid{false}; // CREATE TABLE x UUID '...'

    ASTColumns * columns_list = nullptr;
    ASTStorage * storage = nullptr;

    ASTPtr watermark_function;
    ASTPtr lateness_function;
    String as_database;
    String as_table;
    IAST * as_table_function = nullptr;
    ASTSelectWithUnionQuery * select = nullptr;
    ASTViewTargets * targets = nullptr;
    IAST * comment = nullptr;
    ASTPtr sql_security = nullptr;

    ASTTableOverrideList * table_overrides = nullptr; /// For CREATE DATABASE with engines that automatically create tables

    bool is_dictionary{false}; /// CREATE DICTIONARY
    ASTExpressionList * dictionary_attributes_list = nullptr; /// attributes of
    ASTDictionary * dictionary = nullptr; /// dictionary definition (layout, primary key, etc.)

    ASTRefreshStrategy * refresh_strategy = nullptr; // For CREATE MATERIALIZED VIEW ... REFRESH ...

    bool is_watermark_strictly_ascending{false}; /// STRICTLY ASCENDING WATERMARK STRATEGY FOR WINDOW VIEW
    bool is_watermark_ascending{false}; /// ASCENDING WATERMARK STRATEGY FOR WINDOW VIEW
    bool is_watermark_bounded{false}; /// BOUNDED OUT OF ORDERNESS WATERMARK STRATEGY FOR WINDOW VIEW
    bool allowed_lateness{false}; /// ALLOWED LATENESS FOR WINDOW VIEW

    bool attach_short_syntax{false};

    std::optional<String> attach_from_path = std::nullopt;

    std::optional<bool> attach_as_replicated = std::nullopt;

    bool replace_table{false};
    bool create_or_replace{false};

    /** Get the text that identifies this element. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override
    {
        return removeOnCluster<ASTCreateQuery>(clone(), params.default_database);
    }

    bool isView() const { return is_ordinary_view || is_materialized_view || is_live_view || is_window_view; }

    bool isParameterizedView() const;

    bool supportSQLSecurity() const { return is_ordinary_view || is_materialized_view; }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    /// Generates a random UUID for this create query if it's not specified already.
    /// The function also generates random UUIDs for inner target tables if this create query implies that
    /// (for example, if it's a `CREATE MATERIALIZED VIEW` query with an inner storage).
    void generateRandomUUIDs();

    /// Removes UUID from this create query.
    /// The function also removes UUIDs for inner target tables from this create query (see also generateRandomUUID()).
    void resetUUIDs();

    /// Returns information about a target table.
    /// If that information isn't specified in this create query (or even not allowed) then the function returns an empty value.
    StorageID getTargetTableID(ViewTarget::Kind target_kind) const;
    bool hasTargetTableID(ViewTarget::Kind target_kind) const;
    UUID getTargetInnerUUID(ViewTarget::Kind target_kind) const;
    bool hasInnerUUIDs() const;
    std::shared_ptr<ASTStorage> getTargetInnerEngine(ViewTarget::Kind target_kind) const;
    void setTargetInnerEngine(ViewTarget::Kind target_kind, ASTPtr storage_def);

    bool is_materialized_view_with_external_target() const { return is_materialized_view && hasTargetTableID(ViewTarget::To); }
    bool is_materialized_view_with_inner_table() const { return is_materialized_view && !hasTargetTableID(ViewTarget::To); }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&columns_list));
        f(reinterpret_cast<void **>(&storage));
        f(reinterpret_cast<void **>(&targets));
        f(reinterpret_cast<void **>(&as_table_function));
        f(reinterpret_cast<void **>(&select));
        f(reinterpret_cast<void **>(&comment));
        f(reinterpret_cast<void **>(&table_overrides));
        f(reinterpret_cast<void **>(&dictionary_attributes_list));
        f(reinterpret_cast<void **>(&dictionary));
    }
};

}
