#include <memory>
#include <string_view>
#include <vector>
#include <regex>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/StorageSystemFoundationDB.h>
#include <base/types.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/MetadataStoreFoundationDB.h>
#include <Common/FoundationDB/protos/MetadataConfigParam.pb.h>
#include <Common/FoundationDB/protos/MergeTreePartInfo.pb.h>
#include <Common/FoundationDB/protos/MetadataACLEntry.pb.h>
#include <Common/FoundationDB/protos/MetadataDatabase.pb.h>
#include <Common/FoundationDB/protos/MetadataTable.pb.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Proto = FoundationDB::Proto;

template <typename Type, typename IdentifierMatcher>
static bool extractEqualColumnFromFunction(ContextPtr context, const IAST & elem, const IdentifierMatcher & ident_matcher, Type & res)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (extractEqualColumnFromFunction<Type, IdentifierMatcher>(context, *child, ident_matcher, res))
                return true;

        return false;
    }

    const auto & args = function->arguments->as<ASTExpressionList &>();
    if (args.children.size() != 2)
        return false;

    if (function->name == "equals")
    {
        ASTPtr value;
        if (ident_matcher(args.children.at(0)))
            value = args.children.at(1);
        else if (ident_matcher(args.children.at(1)))
            value = args.children.at(0);
        else
            return false;

        auto evaluated = evaluateConstantExpressionAsLiteral(value, context);
        const auto * literal = evaluated->as<ASTLiteral>();
        if (!literal)
            return false;

        if (literal->value.getType() != Field::TypeToEnum<Type>::value)
            return false;

        res = literal->value.safeGet<Type>();
        return true;
    }

    return false;
}

template <typename Type>
static bool extractEqualColumn(ContextPtr context, const ASTPtr & query, const String & column_name, Type & value)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
        return false;

    std::function<bool(ASTPtr)> ident_matcher;

    std::regex array_element_re(R"(([a-zA-Z_]+)\[(\d*)\])");
    std::smatch array_element_match;
    if (std::regex_match(column_name, array_element_match, array_element_re))
    {
        if (array_element_match.size() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code");

        auto array_column_name = array_element_match[1].str();
        auto array_idx = static_cast<UInt64>(std::stoi(array_element_match[2].str()));
        ident_matcher = [array_column_name, array_idx](ASTPtr ast) {
            auto * func = ast->as<ASTFunction>();
            if (!func || func->name != "arrayElement")
                return false;

            auto args = func->arguments;
            if (args->children.size() != 2)
                return false;

            ASTIdentifier * ident;
            if (!(ident = args->children[0]->as<ASTIdentifier>()))
                return false;

            ASTLiteral * idx_literal;
            if (!(idx_literal = args->children[1]->as<ASTLiteral>()))
                return false;
            auto idx = idx_literal->value.safeGet<UInt64>();

            return ident->name() == array_column_name && idx == array_idx;
        };
    }
    else
    {
        ident_matcher = [&column_name](ASTPtr ast) {
            auto * ident = ast->as<ASTIdentifier>();
            if (!ident)
                return false;

            return ident->name() == column_name;
        };
    }

    return extractEqualColumnFromFunction<Type>(context, *select.where(), ident_matcher, value);
}

String pbJson(const google::protobuf::Message & x)
{
    String json;
    google::protobuf::util::MessageToJsonString(x, &json);
    return json;
}

static inline UUID toUUID(const String & uuid_str)
{
    UUID uuid;
    ReadBufferFromString rb(uuid_str);
    readUUIDText(uuid, rb);
    return uuid;
}

NamesAndTypesList StorageSystemFoundationDB::getNamesAndTypes()
{
    return {
        {"type", std::make_shared<DataTypeString>()},
        {"key", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"value", std::make_shared<DataTypeString>()}};
}

template <typename MetaType>
static constexpr std::string_view MetaTypeName;

#define TypeName(Type, Name) \
    template <> \
    static constexpr std::string_view MetaTypeName<Type> = Name

template <typename MetaType>
static void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info);

TypeName(Proto::MetadataDatabase, "database");
template <>
void fillData<Proto::MetadataDatabase>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &)
{
    const auto meta_store = context->getMetadataStoreFoundationDB();
    const auto db_metas = meta_store->listDatabases();

    for (const auto & db_meta : db_metas)
    {
        res_columns[0]->insert(MetaTypeName<Proto::MetadataDatabase>);
        res_columns[1]->insert(Array{db_meta->name()});
        res_columns[2]->insert(pbJson(*db_meta));
    }
}

TypeName(Proto::MetadataTable, "table");
template <>
void fillData<Proto::MetadataTable>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info)
{
    /// TODO: Support dropped and detached tables
    String db_uuid_str;
    if (!extractEqualColumn(context, query_info.query, "key[1]", db_uuid_str))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SELECT tables from system.foundationdb table must specifiy database name,"
            "e.g. key[1] = '68be117f-a13b-464f-825c-ae75bf6baa24'");
    UUID db_uuid = toUUID(db_uuid_str);
    const auto meta_store = context->getMetadataStoreFoundationDB();
    const auto table_metas = meta_store->listAllTableMeta(db_uuid);

    for (const auto & table_meta : table_metas)
    {
        res_columns[0]->insert(MetaTypeName<Proto::MetadataTable>);
        res_columns[1]->insert(Array{db_uuid_str}); /// TODO: Extract table name
        res_columns[2]->insert(pbJson(*table_meta));
    }
}

TypeName(Proto::MetadataConfigParam, "config");
template <>
void fillData<Proto::MetadataConfigParam>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &)
{
    const auto meta_store = context->getMetadataStoreFoundationDB();
    const auto configs = meta_store->listAllConfigParamMeta();

    for (const auto & config : configs)
    {
        res_columns[0]->insert(MetaTypeName<Proto::MetadataConfigParam>);
        res_columns[1]->insert(Array{config->name()});
        res_columns[2]->insert(config->value());
    }
}

TypeName(Proto::MergeTreePartMeta, "part");
template <>
void fillData<Proto::MergeTreePartMeta>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info)
{
    String table_uuid_str;
    if (!extractEqualColumn(context, query_info.query, "key[1]", table_uuid_str))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SELECT parts from system.foundationdb table must specify table_uuid and part_name, "
            "e.g. key[1] = '68be117f-a13b-464f-825c-ae75bf6baa24'");
    UUID table_uuid = toUUID(table_uuid_str);

    const auto meta_store = context->getMetadataStoreFoundationDB();

    std::vector<String> part_names;
    part_names.resize(1);
    if (!extractEqualColumn(context, query_info.query, "key[2]", part_names[0]))
    {
        // Get all parts of table
	part_names.clear();
        for (const auto & part_disk : meta_store->listParts(table_uuid))
            part_names.emplace_back(part_disk->part_name());
    }

    for (const auto & part_name : part_names)
    {
        if (!meta_store->isExistsPart({table_uuid, part_name}))
	    continue;

        const auto part_meta = meta_store->getPartMeta({table_uuid, part_name});

        res_columns[0]->insert(MetaTypeName<Proto::MergeTreePartMeta>);
        res_columns[1]->insert(Array{table_uuid_str, part_name});
        res_columns[2]->insert(pbJson(*part_meta));
    }
}

TypeName(Proto::MergeTreePartDiskMeta, "part_disk");
template <>
void fillData<Proto::MergeTreePartDiskMeta>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info)
{
    String table_uuid_str;
    if (!extractEqualColumn(context, query_info.query, "key[1]", table_uuid_str))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SELECT parts_disk from system.foundationdb table must specify table uuid, "
            "e.g. key[1] = '68be117f-a13b-464f-825c-ae75bf6baa24'");
    UUID table_uuid = toUUID(table_uuid_str);

    const auto meta_store = context->getMetadataStoreFoundationDB();

    const auto part_metas = meta_store->listParts(table_uuid);

    for (const auto & part_meta : part_metas)
    {
        res_columns[0]->insert(MetaTypeName<Proto::MergeTreePartDiskMeta>);
        res_columns[1]->insert(Array{table_uuid_str, part_meta->part_name()});
        res_columns[2]->insert(pbJson(*part_meta));
    }
}

TypeName(Proto::MergeTreeDetachedPartMeta, "detached_part");
template <>
void fillData<Proto::MergeTreeDetachedPartMeta>(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info)
{
    String table_uuid_str;
    if (!extractEqualColumn(context, query_info.query, "key[1]", table_uuid_str))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SELECT detached_parts from system.foundationdb table must specify table uuid, "
            "e.g. key[1] = '68be117f-a13b-464f-825c-ae75bf6baa24'");
    UUID table_uuid = toUUID(table_uuid_str);

    const auto meta_store = context->getMetadataStoreFoundationDB();
    const auto part_metas = meta_store->listDetachedParts(table_uuid);
    for (const auto & part_meta : part_metas)
    {
        res_columns[0]->insert(MetaTypeName<Proto::MergeTreeDetachedPartMeta>);
        res_columns[1]->insert(Array{table_uuid_str, part_meta->part_name()}); /// TODO: Part name
        res_columns[2]->insert(pbJson(*part_meta));
    }
}

void StorageSystemFoundationDB::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const
{
    /// `systems.foundationdb` should not be available when foundationdb is not enabled
    if (!context->hasMetadataStoreFoundationDB())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FoundationDB is not enabled.");

    String select_type;
    if (!extractEqualColumn(context, query_info.query, "type", select_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SELECT from system.foundationdb table must contain condition like type = 'database'");

    if (select_type == MetaTypeName<Proto::MetadataDatabase>)
        fillData<Proto::MetadataDatabase>(res_columns, context, query_info);
    else if (select_type == MetaTypeName<Proto::MetadataTable>)
        fillData<Proto::MetadataTable>(res_columns, context, query_info);
    // TODO: Support Access Entity
    // else if (select_type == MetaTypeName<Proto::AccessEntity>)
    //     fillData<Proto::AccessEntity>(res_columns, context, query_info);
    else if (select_type == MetaTypeName<Proto::MetadataConfigParam>)
        fillData<Proto::MetadataConfigParam>(res_columns, context, query_info);
    else if (select_type == MetaTypeName<Proto::MergeTreePartMeta>)
        fillData<Proto::MergeTreePartMeta>(res_columns, context, query_info);
    else if (select_type == MetaTypeName<Proto::MergeTreePartDiskMeta>)
        fillData<Proto::MergeTreePartDiskMeta>(res_columns, context, query_info);
    else if (select_type == MetaTypeName<Proto::MergeTreeDetachedPartMeta>)
        fillData<Proto::MergeTreeDetachedPartMeta>(res_columns, context, query_info);
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid type: {}, type should be one of {}",
            select_type,
            toString(Array{
                MetaTypeName<Proto::MetadataDatabase>,
                MetaTypeName<Proto::MetadataTable>,
                MetaTypeName<Proto::AccessEntity>,
                MetaTypeName<Proto::MetadataConfigParam>,
                MetaTypeName<Proto::MergeTreePartMeta>,
                MetaTypeName<Proto::MergeTreePartDiskMeta>,
                MetaTypeName<Proto::MergeTreeDetachedPartMeta>,
            }));
}

}
