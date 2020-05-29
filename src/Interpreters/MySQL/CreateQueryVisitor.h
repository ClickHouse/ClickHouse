#pragma once

#include <Core/NamesAndTypes.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTCreateQuery.h>
#include <Parsers/MySQL/ASTDeclareColumn.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclarePartitionOptions.h>
#include <Parsers/parseQuery.h>

namespace DB
{

struct MySQLTableStruct
{
    bool if_not_exists;
    String table_name;
    String database_name;
    ASTs primary_keys;
    ASTs partition_keys;
    NamesAndTypesList columns_name_and_type;

    MySQLTableStruct() {}

    MySQLTableStruct(const ASTs & primary_keys_, const ASTs & partition_keys_, const NamesAndTypesList & columns_name_and_type_)
        : primary_keys(primary_keys_), partition_keys(partition_keys_), columns_name_and_type(columns_name_and_type_)
    {}

    bool operator==(const MySQLTableStruct & other) const;
};

namespace MySQLVisitor
{

/// Convert MySQL CREATE query to https://clickhouse.tech/docs/en/sql-reference/statements/create/
class CreateQueryMatcher
{
public:
    using Visitor = InDepthNodeVisitor<CreateQueryMatcher, false>;

    struct Data : public MySQLTableStruct
    {
        const Context & context;
        size_t max_ranges;
        size_t min_rows_pre_range;

        Data(const Context & context_) : MySQLTableStruct(), context(context_) {}

        void addPrimaryKey(const ASTPtr & primary_key);

        void addPartitionKey(const ASTPtr & partition_key);
    };

    static void visit(ASTPtr & ast, Data & data);

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return false; }
private:
    static void visit(const MySQLParser::ASTCreateQuery & create, const ASTPtr &, Data & data);

    static void visit(const MySQLParser::ASTDeclareIndex & declare_index, const ASTPtr &, Data & data);

    static void visit(const MySQLParser::ASTCreateDefines & create_defines, const ASTPtr &, Data & data);

    static void visit(const MySQLParser::ASTDeclareColumn & declare_column, const ASTPtr &, Data & data);

    static void visit(const MySQLParser::ASTDeclarePartitionOptions & declare_partition_options, const ASTPtr &, Data & data);
};

using CreateQueryVisitor = CreateQueryMatcher::Visitor;

}

MySQLTableStruct visitCreateQuery(ASTPtr & create_query, const Context & context, const std::string & new_database);

MySQLTableStruct visitCreateQuery(const String & create_query, const Context & context, const std::string & new_database);

}
