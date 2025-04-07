#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/resolveStorages.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/TableNode.h>
#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Storages/StorageSet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

namespace Setting
{
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
}

enum class SetSerializationKind : UInt8
{
    StorageSet = 1,
    TupleValues = 2,
    SubqueryPlan = 3,
};

QueryPlanAndSets::QueryPlanAndSets() = default;
QueryPlanAndSets::~QueryPlanAndSets() = default;
QueryPlanAndSets::QueryPlanAndSets(QueryPlanAndSets &&) noexcept = default;

struct QueryPlanAndSets::Set
{
    CityHash_v1_0_2::uint128 hash;
    std::list<ColumnSet *> columns;
};
struct QueryPlanAndSets::SetFromStorage : public QueryPlanAndSets::Set
{
    std::string storage_name;
};

struct QueryPlanAndSets::SetFromTuple : public QueryPlanAndSets::Set
{
    ColumnsWithTypeAndName set_columns;
};

struct QueryPlanAndSets::SetFromSubquery : public QueryPlanAndSets::Set
{
    QueryPlanAndSets plan_and_sets;
};

void QueryPlan::serializeSets(SerializedSetsRegistry & registry, WriteBuffer & out, const SerializationFlags & flags)
{
    writeVarUInt(registry.sets.size(), out);
    for (const auto & [hash, set] : registry.sets)
    {
        writeBinary(hash, out);

        if (auto * from_storage = typeid_cast<FutureSetFromStorage *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::StorageSet, out);
            const auto & storage_id = from_storage->getStorageID();
            if (!storage_id)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "FutureSetFromStorage without storage id");

            auto storage_name = storage_id->getFullTableName();
            writeStringBinary(storage_name, out);
        }
        else if (auto * from_tuple = typeid_cast<FutureSetFromTuple *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::TupleValues, out);

            auto types = from_tuple->getTypes();
            auto columns = from_tuple->getKeyColumns();

            if (columns.size() != types.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Invalid number of columns for Set. Expected {} got {}",
                    columns.size(), types.size());

            UInt64 num_columns = columns.size();
            UInt64 num_rows = num_columns > 0 ? columns.front()->size() : 0;

            writeVarUInt(num_columns, out);
            writeVarUInt(num_rows, out);

            for (size_t col = 0; col < num_columns; ++col)
            {
                if (columns[col]->size() != num_rows)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Invalid number of rows in column of Set. Expected {} got {}",
                        num_rows, columns[col]->size());

                encodeDataType(types[col], out);
                auto serialization = types[col]->getSerialization(ISerialization::Kind::DEFAULT);
                serialization->serializeBinaryBulk(*columns[col], out, 0, num_rows);
            }
        }
        else if (auto * from_subquery = typeid_cast<FutureSetFromSubquery *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::SubqueryPlan, out);
            const auto * plan = from_subquery->getQueryPlan();
            if (!plan)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize FutureSetFromSubquery with no query plan");

            plan->serialize(out, flags);
        }
        else
        {
            const auto & set_ref = *set;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown FutureSet type {}", typeid(set_ref).name());
        }
    }
}

QueryPlanAndSets QueryPlan::deserializeSets(
    QueryPlan plan,
    DeserializedSetsRegistry & registry,
    ReadBuffer & in,
    const SerializationFlags & flags,
    const ContextPtr & context)
{
    UInt64 num_sets;
    readVarUInt(num_sets, in);

    QueryPlanAndSets res;
    res.plan = std::move(plan);

    for (size_t i = 0; i < num_sets; ++i)
    {
        PreparedSets::Hash hash;
        readBinary(hash, in);

        auto it = registry.sets.find(hash);
        if (it == registry.sets.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is not registered", hash.low64, hash.high64);

        auto & columns = it->second;
        if (columns.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is serialized twice", hash.low64, hash.high64);

        UInt8 kind;
        readVarUInt(kind, in);
        if (kind == UInt8(SetSerializationKind::StorageSet))
        {
            String storage_name;
            readStringBinary(storage_name, in);
            res.sets_from_storage.emplace_back(QueryPlanAndSets::SetFromStorage{{hash, std::move(columns)}, std::move(storage_name)});
        }
        else if (kind == UInt8(SetSerializationKind::TupleValues))
        {
            UInt64 num_columns;
            UInt64 num_rows;
            readVarUInt(num_columns, in);
            readVarUInt(num_rows, in);

            ColumnsWithTypeAndName set_columns;
            set_columns.reserve(num_columns);

            for (size_t col = 0; col < num_columns; ++col)
            {
                auto type = decodeDataType(in);
                auto serialization = type->getSerialization(ISerialization::Kind::DEFAULT);
                auto column = type->createColumn();
                serialization->deserializeBinaryBulk(*column, in, 0, num_rows, 0);

                set_columns.emplace_back(std::move(column), std::move(type), String{});
            }

            res.sets_from_tuple.emplace_back(QueryPlanAndSets::SetFromTuple{{hash, std::move(columns)}, std::move(set_columns)});
        }
        else if (kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            auto plan_for_set = QueryPlan::deserialize(in, context, flags);

            res.sets_from_subquery.emplace_back(QueryPlanAndSets::SetFromSubquery{
                {hash, std::move(columns)},
                std::move(plan_for_set)});
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} has unknown kind {}",
                hash.low64, hash.high64, int(kind));
    }

    return res;
}

static void makeSetsFromStorage(std::list<QueryPlanAndSets::SetFromStorage> sets, const ContextPtr & context)
{
    for (auto & set : sets)
    {
        Identifier identifier = parseTableIdentifier(set.storage_name, context);
        auto * table_node = resolveTable(identifier, context);
        const auto * storage_set = typeid_cast<const StorageSet *>(table_node->getStorage().get());
        if (!storage_set)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Table {} is not a StorageSet", set.storage_name);

        auto future_set = std::make_shared<FutureSetFromStorage>(set.hash, nullptr, storage_set->getSet(), table_node->getStorageID());
        for (auto * column : set.columns)
            column->setData(future_set);
    }
}

static void makeSetsFromTuple(std::list<QueryPlanAndSets::SetFromTuple> sets, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    for (auto & set : sets)
    {
        SizeLimits size_limits = PreparedSets::getSizeLimitsForSet(settings);
        bool transform_null_in = settings[Setting::transform_null_in];

        auto future_set = std::make_shared<FutureSetFromTuple>(set.hash, nullptr, std::move(set.set_columns), transform_null_in, size_limits);
        for (auto * column : set.columns)
            column->setData(future_set);
    }
}

static void makeSetsFromSubqueries(QueryPlan & plan, std::list<QueryPlanAndSets::SetFromSubquery> sets, const ContextPtr & context)
{
    if (sets.empty())
        return;

    const auto & settings = context->getSettingsRef();

    PreparedSets::Subqueries subqueries;
    subqueries.reserve(sets.size());
    for (auto & set : sets)
    {
        auto subquery_plan = QueryPlan::makeSets(std::move(set.plan_and_sets), context);

        SizeLimits size_limits = PreparedSets::getSizeLimitsForSet(settings);
        bool transform_null_in = settings[Setting::transform_null_in];
        size_t max_size_for_index = settings[Setting::use_index_for_in_with_subqueries_max_values];

        auto future_set = std::make_shared<FutureSetFromSubquery>(
            set.hash, nullptr, std::make_unique<QueryPlan>(std::move(subquery_plan)),
            nullptr, nullptr,
            transform_null_in, size_limits, max_size_for_index);

        for (auto * column : set.columns)
            column->setData(future_set);

        subqueries.push_back(std::move(future_set));
    }

    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();

    auto step = std::make_unique<DelayedCreatingSetsStep>(
        plan.getCurrentHeader(),
        std::move(subqueries),
        network_transfer_limits,
        prepared_sets_cache);

    plan.addStep(std::move(step));
}

QueryPlan QueryPlan::makeSets(QueryPlanAndSets plan_and_sets, const ContextPtr & context)
{
    auto & plan = plan_and_sets.plan;

    makeSetsFromStorage(std::move(plan_and_sets.sets_from_storage), context);
    makeSetsFromTuple(std::move(plan_and_sets.sets_from_tuple), context);
    makeSetsFromSubqueries(plan, std::move(plan_and_sets.sets_from_subquery), context);

    return std::move(plan);
}

}
