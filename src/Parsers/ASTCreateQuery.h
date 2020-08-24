#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTDictionary.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/StorageID.h>


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


class ASTExpressionList;

class ASTColumns : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;

    String getID(char) const override { return "Columns definition"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};


/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    bool attach{false};    /// Query ATTACH TABLE, not CREATE TABLE.
    bool if_not_exists{false};
    bool is_view{false};
    bool is_materialized_view{false};
    bool is_live_view{false};
    bool is_populate{false};
    bool is_dictionary{false}; /// CREATE DICTIONARY
    bool replace_view{false}; /// CREATE OR REPLACE VIEW
    ASTColumns * columns_list = nullptr;
    ASTExpressionList * dictionary_attributes_list = nullptr; /// attributes of dictionary
    ASTExpressionList * tables = nullptr;
    //FIXME
    StorageID to_table_id = StorageID::createEmpty();   /// For CREATE MATERIALIZED VIEW mv TO table.
    ASTStorage * storage = nullptr;
    String as_database;
    String as_table;
    ASTPtr as_table_function;
    ASTSelectWithUnionQuery * select = nullptr;
    ASTDictionary * dictionary = nullptr; /// dictionary definition (layout, primary key, etc.)
    std::optional<UInt64> live_view_timeout;    /// For CREATE LIVE VIEW ... WITH TIMEOUT ...
    bool attach_short_syntax{false};

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
