#include <Interpreters/TableOverrideUtils.h>

#include <Common/quoteString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/IAST.h>
#include <Storages/ColumnDefault.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_TABLE_OVERRIDE;
}

namespace
{

class MaybeNullableColumnsMatcher
{
public:
    using Visitor = ConstInDepthNodeVisitor<MaybeNullableColumnsMatcher, false>;
    using Data = RequiredSourceColumnsData;

    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child)
    {
        if (const auto * f = node->as<ASTFunction>(); f && f->name == "assumeNotNull")
            return false;
        return RequiredSourceColumnsMatcher::needChildVisit(node, child);
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        RequiredSourceColumnsMatcher::visit(ast, data);
    }
};

using MaybeNullableColumnsVisitor = MaybeNullableColumnsMatcher::Visitor;

}

static void checkRequiredColumns(const IAST * ast, const NameToTypeMap & existing_types, NamesAndTypes & used_columns, const String & what, bool allow_nulls = false)
{
    if (!ast)
        return;
    RequiredSourceColumnsData columns_data;
    RequiredSourceColumnsVisitor(columns_data).visit(ast->clone());
    auto required_columns = columns_data.requiredColumns();
    for (const auto & column : required_columns)
    {
        auto type = existing_types.find(column);
        if (type == existing_types.end())
            throw Exception(ErrorCodes::INVALID_TABLE_OVERRIDE, "{} override refers to unknown column {}", what, backQuote(column));
    }
    if (!allow_nulls)
    {
        RequiredSourceColumnsData nullable_data;
        MaybeNullableColumnsVisitor(nullable_data).visit(ast->clone());
        for (const auto & column : nullable_data.requiredColumns())
        {
            if (existing_types.find(column)->second->isNullable())
                throw Exception(
                    ErrorCodes::INVALID_TABLE_OVERRIDE,
                    "{} override refers to nullable column {} (use assumeNotNull() if the column does not in fact contain NULL values)",
                    what,
                    backQuote(column));
        }
    }
    for (const auto & col : required_columns)
    {
        used_columns.push_back({col, existing_types.find(col)->second});
    }
}

void TableOverrideAnalyzer::analyze(const StorageInMemoryMetadata & metadata, Result & result) const
{
    for (const auto & column : metadata.columns)
        result.existing_types[column.name] = column.type;
    checkRequiredColumns(override->storage->order_by, result.existing_types, result.order_by_columns, "ORDER BY");
    checkRequiredColumns(override->storage->primary_key, result.existing_types, result.primary_key_columns, "PRIMARY KEY");
    checkRequiredColumns(override->storage->partition_by, result.existing_types, result.partition_by_columns, "PARTITION BY");
    checkRequiredColumns(override->storage->sample_by, result.existing_types, result.sample_by_columns, "SAMPLE BY");
    checkRequiredColumns(override->storage->ttl_table, result.existing_types, result.ttl_columns, "TTL");
    if (override->columns && override->columns->columns)
    {
        for (const auto & column_ast : override->columns->columns->children)
        {
            auto * override_column = column_ast->as<ASTColumnDeclaration>();
            auto override_type = DataTypeFactory::instance().get(override_column->type);
            auto found = metadata.columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, override_column->name);
            std::optional<ColumnDefaultKind> override_default_kind;
            if (!override_column->default_specifier.empty())
                override_default_kind = columnDefaultKindFromString(override_column->default_specifier);
            if (found)
            {
                std::optional<ColumnDefaultKind> existing_default_kind;
                if (auto col_default = metadata.columns.getDefault(found->name))
                    existing_default_kind = col_default->kind;
                if (existing_default_kind != override_default_kind)
                    throw Exception(ErrorCodes::INVALID_TABLE_OVERRIDE, "column {}: modifying default specifier is not allowed", backQuote(override_column->name));
                result.modified_columns.push_back({found->name, override_type});
                /// TODO: validate that the original type can be converted to the overridden type
            }
            else
            {
                if (override_default_kind && *override_default_kind == ColumnDefaultKind::Alias)
                    result.added_columns.push_back({override_column->name, override_type});
                else
                    throw Exception(ErrorCodes::INVALID_TABLE_OVERRIDE, "column {}: can only add ALIAS columns", backQuote(override_column->name));
            }
            /// TODO: validate default and materialized expressions (use checkRequiredColumns, allowing nulls)
        }
    }
}

void TableOverrideAnalyzer::Result::appendTo(WriteBuffer & ostr)
{
    const auto & format_names = [&](const NamesAndTypes & names) -> String
    {
        WriteBufferFromOwnString buf;
        bool first = true;
        for (const auto & name : names)
        {
            if (!first)
                buf << ", ";
            first = false;
            buf << backQuote(name.name) << " ";
            auto old_type = existing_types.find(name.name);
            if (old_type != existing_types.end() && old_type->second != name.type)
                buf << old_type->second->getName() << " -> ";
            buf << name.type->getName();
        }
        return buf.str();
    };
    if (!modified_columns.empty())
    {
        ostr << "Modified columns: " << format_names(modified_columns) << "\n";
    }
    if (!added_columns.empty())
    {
        ostr << "Added columns: " << format_names(added_columns) << "\n";
    }
    if (!order_by_columns.empty())
    {
        ostr << "ORDER BY uses columns: " << format_names(order_by_columns) << "\n";
    }
    if (!primary_key_columns.empty())
    {
        ostr << "PRIMARY KEY uses columns: " << format_names(primary_key_columns) << "\n";
    }
    if (!partition_by_columns.empty())
    {
        ostr << "PARTITION BY uses columns: " << format_names(partition_by_columns) << "\n";
    }
    if (!sample_by_columns.empty())
    {
        ostr << "SAMPLE BY uses columns: " << format_names(sample_by_columns) << "\n";
    }
    if (!ttl_columns.empty())
    {
        ostr << "TTL uses columns: " << format_names(ttl_columns) << "\n";
    }
}

}
