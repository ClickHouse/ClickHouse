#include <algorithm>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageProjection.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


StorageProjection::StorageProjection(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const StorageID & source_table_id_,
    const String & projection_name_,
    const Context & context_)
    : IStorage(table_id_), source_table_id(source_table_id_), projection_name(projection_name_), global_context(context_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

void registerStorageProjection(StorageFactory & factory)
{
    factory.registerStorage("Projection", [](const StorageFactory::Arguments & args)
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(
                "Storage Projection requires exactly 2 parameters: "
                "name of the source table and name of the projection",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto source_table_id = IdentifierSemantic::extractDatabaseAndTable(engine_args[0]->as<const ASTIdentifier &>());
        if (source_table_id.database_name.empty())
            source_table_id.database_name = args.local_context.getConfigRef().getString("default_database", "default");
        String projection_name = engine_args[1]->as<const ASTLiteral &>().value.safeGet<String>();

        return StorageProjection::create(args.table_id, args.columns, source_table_id, projection_name, args.context);
    });
}

NamesAndTypesList StorageProjection::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_part", std::make_shared<DataTypeString>()),
        NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
        NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
        NameAndTypePair("_projections", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())),
    };
}

bool StorageProjection::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, const Context & context, const StorageMetadataPtr & metadata_snapshot) const
{
    auto storage = DatabaseCatalog::instance().getTable(source_table_id, context);
    return storage->mayBenefitFromIndexForIn(left_in_operand, context, metadata_snapshot);
}

}
