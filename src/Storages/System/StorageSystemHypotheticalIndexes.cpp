#include <Storages/System/StorageSystemHypotheticalIndexes.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/HypotheticalIndexStore.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

ColumnsDescription StorageSystemHypotheticalIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Index name."},
        {"database", std::make_shared<DataTypeString>(), "Database name."},
        {"table", std::make_shared<DataTypeString>(), "Table name."},
        {"type", std::make_shared<DataTypeString>(), "Index type (minmax, set, bloom_filter, etc.)."},
        {"expression", std::make_shared<DataTypeString>(), "Index expression."},
        {"granularity", std::make_shared<DataTypeUInt64>(), "Index granularity."},
        {"scope", std::make_shared<DataTypeString>(), "Scope (always 'session')."},
    };
}

void StorageSystemHypotheticalIndexes::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto & store = context->getHypotheticalIndexStore();
    auto entries = store.getAll();

    for (const auto & entry : entries)
    {
        size_t col = 0;
        res_columns[col++]->insert(entry.index.name);
        res_columns[col++]->insert(entry.table_id.getDatabaseName());
        res_columns[col++]->insert(entry.table_id.getTableName());
        res_columns[col++]->insert(entry.index.type);

        /// Format expression from AST.
        String expr_str;
        if (entry.index.expression_list_ast)
        {
            WriteBufferFromString buf(expr_str);
            IAST::FormatSettings fmt_settings(buf, /* one_line = */ true);
            entry.index.expression_list_ast->format(buf, fmt_settings, {}, {});
        }
        res_columns[col++]->insert(expr_str);

        res_columns[col++]->insert(entry.index.granularity);
        res_columns[col++]->insert("session");
    }
}

}
