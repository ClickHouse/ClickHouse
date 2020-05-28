#pragma once

#include <Core/Types.h>

#include <memory>
#include <vector>
#include <Core/Field.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct StorageMetadataSkipIndexField
{
    ASTPtr definition_ast;

    ASTPtr expression_list_ast;

    String name;

    String type;

    ExpressionActionsPtr expression;

    FieldVector arguments;

    Names column_names;

    DataTypes data_types;

    Block sample_block;

    size_t granularity;

    static StorageMetadataSkipIndexField
    getSkipIndexFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context);
};

struct IndicesDescription : public std::vector<StorageMetadataSkipIndexField>
{
    bool has(const String & name) const;
    String toString() const;

    static IndicesDescription parse(const String & str, const ColumnsDescription & columns, const Context & context);
};

}
