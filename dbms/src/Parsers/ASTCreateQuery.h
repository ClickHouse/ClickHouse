#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithTableOrDictionaryAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTFunction;
class ASTSetQuery;

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

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

// LIFETIME(MIN 10, MAX 100)
class ASTDictionaryLifetime : public IAST
{
public:
    uint64_t min_sec;
    uint64_t max_sec;

    String getID(char delimiter) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings,
                    FormatState & state,
                    FormatStateStacked frame) const override;
};


// RANGE(MIN startDate, MAX endDate)
class ASTDictionaryRange : public IAST
{
public:
    String min_column_name;
    String max_column_name;

    String getID(char delimiter) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings,
                    FormatState & state,
                    FormatStateStacked frame) const override;
};


class ASTDictionarySource : public IAST
{
public:
    ASTKeyValueFunction * source = nullptr;
    IAST * primary_key = nullptr;
    IAST * composite_key = nullptr;
    ASTDictionaryLifetime * lifetime = nullptr;
    ASTKeyValueFunction * layout;
    ASTDictionaryRange * range = nullptr;

    String getID(char delimiter) const override;

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTExpressionList;

class ASTColumns : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;

    String getID(char) const override { return "Columns definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


class ASTSelectWithUnionQuery;

/// CREATE TABLE or ATTACH TABLE or CREATE DICTIONARY query
class ASTCreateQuery : public ASTQueryWithTableOrDictionaryAndOutput, public ASTQueryWithOnCluster
{
public:
    bool attach{false};    /// Query ATTACH TABLE, not CREATE TABLE.
    bool or_replace{false};   /// Query CREATE OR REPLACE DICTIONARY.
    bool if_not_exists{false};
    bool is_view{false};
    bool is_materialized_view{false};
    bool is_populate{false};
    bool replace_view{false}; /// CREATE OR REPLACE VIEW
    ASTColumns * columns_list = nullptr;
    String to_database;   /// For CREATE MATERIALIZED VIEW mv TO table.
    String to_table;
    ASTStorage * storage = nullptr;
    String as_database;
    String as_table;
    ASTSelectWithUnionQuery * select = nullptr;
    ASTDictionarySource * dictionary_source = nullptr;


    /** Get the text that identifies this element. */
    String getID(char delim) const override { return (attach ? "AttachQuery" : "CreateQuery") + (delim + database) + delim + table; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTCreateQuery>(clone(), new_database);
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
