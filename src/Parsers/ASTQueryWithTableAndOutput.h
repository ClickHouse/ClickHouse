#pragma once

#include <base/defines.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Core/UUID.h>


namespace DB
{


/** Query specifying table name and, possibly, the database and the FORMAT section.
  */
class ASTQueryWithTableAndOutput : public ASTQueryWithOutput
{
public:
    IAST * database = nullptr;
    IAST * table = nullptr;

    UUID uuid = UUIDHelpers::Nil;
    bool temporary{false};

    String getDatabase() const;
    String getTable() const;

    // Once database or table are set they cannot be assigned with empty value
    void setDatabase(const String & name);
    void setTable(const String & name);

    void cloneTableOptions(ASTQueryWithTableAndOutput & cloned) const;

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override;
};


template <typename AstIDAndQueryNames>
class ASTQueryWithTableAndOutputImpl : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const override { return AstIDAndQueryNames::ID + (delim + getDatabase()) + delim + getTable(); }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

struct ASTExistsDatabaseQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsDatabaseQuery";
    static constexpr auto Query = "EXISTS DATABASE";
    /// No temporary databases are supported, just for parsing
    static constexpr auto QueryTemporary = "";
};

struct ASTExistsTableQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsTableQuery";
    static constexpr auto Query = "EXISTS TABLE";
    static constexpr auto QueryTemporary = "EXISTS TEMPORARY TABLE";
};

struct ASTExistsViewQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsViewQuery";
    static constexpr auto Query = "EXISTS VIEW";
    /// No temporary view are supported, just for parsing
    static constexpr auto QueryTemporary = "";
};


struct ASTExistsDictionaryQueryIDAndQueryNames
{
    static constexpr auto ID = "ExistsDictionaryQuery";
    static constexpr auto Query = "EXISTS DICTIONARY";
    /// No temporary dictionaries are supported, just for parsing
    static constexpr auto QueryTemporary = "EXISTS TEMPORARY DICTIONARY";
};

struct ASTShowCreateTableQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateTableQuery";
    static constexpr auto Query = "SHOW CREATE TABLE";
    static constexpr auto QueryTemporary = "SHOW CREATE TEMPORARY TABLE";
};

struct ASTShowCreateViewQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateViewQuery";
    static constexpr auto Query = "SHOW CREATE VIEW";
    /// No temporary view are supported, just for parsing
    static constexpr auto QueryTemporary = "";
};

struct ASTShowCreateDatabaseQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateDatabaseQuery";
    static constexpr auto Query = "SHOW CREATE DATABASE";
    static constexpr auto QueryTemporary = "SHOW CREATE TEMPORARY DATABASE";
};

struct ASTShowCreateDictionaryQueryIDAndQueryNames
{
    static constexpr auto ID = "ShowCreateDictionaryQuery";
    static constexpr auto Query = "SHOW CREATE DICTIONARY";
    /// No temporary dictionaries are supported, just for parsing
    static constexpr auto QueryTemporary = "SHOW CREATE TEMPORARY DICTIONARY";
};

struct ASTDescribeQueryExistsQueryIDAndQueryNames
{
    static constexpr auto ID = "DescribeQuery";
    static constexpr auto Query = "DESCRIBE TABLE";
    static constexpr auto QueryTemporary = "DESCRIBE TEMPORARY TABLE";
};

using ASTExistsTableQuery = ASTQueryWithTableAndOutputImpl<ASTExistsTableQueryIDAndQueryNames>;
using ASTExistsViewQuery = ASTQueryWithTableAndOutputImpl<ASTExistsViewQueryIDAndQueryNames>;
using ASTExistsDictionaryQuery = ASTQueryWithTableAndOutputImpl<ASTExistsDictionaryQueryIDAndQueryNames>;
using ASTShowCreateTableQuery = ASTQueryWithTableAndOutputImpl<ASTShowCreateTableQueryIDAndQueryNames>;
using ASTShowCreateViewQuery = ASTQueryWithTableAndOutputImpl<ASTShowCreateViewQueryIDAndQueryNames>;
using ASTShowCreateDictionaryQuery = ASTQueryWithTableAndOutputImpl<ASTShowCreateDictionaryQueryIDAndQueryNames>;

extern template class ASTQueryWithTableAndOutputImpl<ASTExistsTableQueryIDAndQueryNames>;
extern template class ASTQueryWithTableAndOutputImpl<ASTExistsViewQueryIDAndQueryNames>;
extern template class ASTQueryWithTableAndOutputImpl<ASTExistsDictionaryQueryIDAndQueryNames>;
extern template class ASTQueryWithTableAndOutputImpl<ASTShowCreateTableQueryIDAndQueryNames>;
extern template class ASTQueryWithTableAndOutputImpl<ASTShowCreateViewQueryIDAndQueryNames>;
extern template class ASTQueryWithTableAndOutputImpl<ASTShowCreateDictionaryQueryIDAndQueryNames>;

class ASTExistsDatabaseQuery : public ASTQueryWithTableAndOutputImpl<ASTExistsDatabaseQueryIDAndQueryNames>
{
public:
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    QueryKind getQueryKind() const override { return QueryKind::Exists; }

    /// There is no forEachPointerToChild, because this class doesn't have additional fields and children.
};

class ASTShowCreateDatabaseQuery : public ASTQueryWithTableAndOutputImpl<ASTShowCreateDatabaseQueryIDAndQueryNames>
{
public:
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    /// There is no forEachPointerToChild, because this class doesn't have additional fields and children.
};

class ASTDescribeQuery : public ASTQueryWithOutput
{
public:
    IAST * table_expression;

    String getID(char) const override { return "DescribeQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Describe; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(void**)> f) override;

};


}
