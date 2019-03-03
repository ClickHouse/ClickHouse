#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Formats/FormatSchemaLoader.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/System/StorageSystemFormatSchemas.h>


namespace DB
{
namespace
{
    bool extractFilterByPathFromWhereExpression(const IAST & where_expression, String & filter_by_path)
    {
        const ASTFunction* function = typeid_cast<const ASTFunction*>(&where_expression);
        if (!function || (function->name != "equals") || !function->arguments || (function->arguments->children.size() != 2))
            return false;
        const ASTIdentifier* identifier = typeid_cast<const ASTIdentifier*>(function->arguments->children[0].get());
        const ASTLiteral* literal = typeid_cast<const ASTLiteral*>(function->arguments->children[1].get());
        if (!identifier || (identifier->name != "path") || !literal || (literal->value.getType() != Field::Types::String))
            return false;
        filter_by_path = literal->value.get<String>();
        return true;
    }
}


StorageSystemFormatSchemas::StorageSystemFormatSchemas(const String & name_) : name(name_)
{
    NamesAndTypesList names_and_types{
        {"path", std::make_shared<DataTypeString>()},
        {"schema", std::make_shared<DataTypeString>()},
    };
    setColumns(ColumnsDescription(names_and_types));
}

BlockInputStreams StorageSystemFormatSchemas::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    check(column_names);
    bool only_path_column = (column_names.size() == 1 && column_names[0] == "path");

    Block sample_block = only_path_column ? getSampleBlockForColumns({"path"}) : getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    FormatSchemaLoader & format_schema_loader = context.getFormatSchemaLoader();

    std::vector<String> paths;
    ASTSelectQuery query = typeid_cast<ASTSelectQuery &>(*query_info.query);
    String filter_by_path;
    if (query.where_expression && extractFilterByPathFromWhereExpression(*query.where_expression, filter_by_path))
        paths.emplace_back(filter_by_path);
    else
        paths = format_schema_loader.getAllPaths();

    if (only_path_column)
    {
        for (const String & path : paths)
            res_columns[0]->insert(path);
    }
    else
    {
        for (const String & path : paths)
        {
            String schema;
            if (format_schema_loader.tryGetSchema(path, schema))
            {
                res_columns[0]->insert(path);
                res_columns[1]->insert(schema);
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns))));
}

}
