#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/resolveStorages.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Analyzer/Identifier.h>
#include <Analyzer/TableNode.h>
#include <Columns/ColumnSet.h>
#include <Interpreters/Set.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Formats/NativeReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeWriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Storages/StorageSet.h>
#include <Storages/StorageProxy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_DEEP_RECURSION;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
}

/// The maximum nesting depth of plans for sets from subqueries in a deserialized query plan.
/// The serialized plan may come from a client, and the deserialization is recursive.
static constexpr size_t MAX_SUBQUERY_PLAN_NESTING_DEPTH = 1000;

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
    std::optional<bool> source_is_empty;
    bool source_has_null = false;
};

struct QueryPlanAndSets::SetFromSubquery : public QueryPlanAndSets::Set
{
    QueryPlanAndSets plan_and_sets;
};

/// The payload of a `TupleValues` record: column count, row count, then per column the encoded
/// type and the native-encoded data.
static void writeSetValues(const DataTypes & types, const Columns & columns, WriteBuffer & out)
{
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
        auto serialization = types[col]->getDefaultSerialization();
        NativeWriter::writeData(*serialization, columns[col], out, {}, 0, 0, 0);
    }
}

static bool columnsContainNull(const Columns & columns)
{
    if (columns.size() != 1)
        return false;

    const auto & column = *columns.front();
    for (size_t row = 0, rows = column.size(); row < rows; ++row)
        if (column.isNullAt(row))
            return true;
    return false;
}

static void writeSetSourceState(
    bool source_is_empty,
    bool source_has_null,
    const Columns & columns,
    WriteBuffer & out,
    UInt64 version)
{
    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SET_SOURCE_STATE)
    {
        writeVarUInt(UInt64{source_is_empty}, out);
        writeVarUInt(UInt64{source_has_null}, out);
        return;
    }

    const bool serialized_is_empty = columns.empty() || columns.front()->empty();
    const bool serialized_has_null = columnsContainNull(columns);
    if (source_is_empty != serialized_is_empty || source_has_null != serialized_has_null)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot serialize an IN set whose logical source state differs from its retained keys for query-plan version {}; "
            "version {} or newer is required",
            version,
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SET_SOURCE_STATE);
}

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
            auto columns = from_tuple->getKeyColumns();
            writeSetSourceState(from_tuple->isSourceEmpty(), from_tuple->sourceHasNull(), columns, out, flags.version);
            writeSetValues(from_tuple->getTypes(), columns, out);
        }
        else if (auto * from_subquery = typeid_cast<FutureSetFromSubquery *>(set.get()))
        {
            if (flags.sets_must_be_ready)
            {
                /// A worker task binds a ready set before its own planning; shipping the
                /// subquery plan instead would make every task re-run the subquery.
                auto built_set = from_subquery->get();
                if (!built_set || !built_set->hasExplicitSetElements() || built_set->isTruncated())
                {
                    String reason = !built_set ? "the set is not built"
                        : built_set->isTruncated() ? "the set was truncated by `set_overflow_mode = 'break'`"
                                                   : "the set was built without its values";
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Cannot ship an IN-subquery set to distributed-plan worker tasks ({}): {}",
                        reason,
                        from_subquery->getSourceAST() ? from_subquery->getSourceAST()->formatForErrorMessage() : "");
                }

                auto columns = built_set->getSetElements();
                UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
                size_t num_bytes = 0;
                for (const auto & column : columns)
                    num_bytes += column->byteSize();
                /// `transfer_overflow_mode = 'break'` cannot apply here: truncating the set
                /// would change `IN` results on the workers, so an over-limit set always
                /// throws, whatever the overflow mode.
                if (!flags.sets_transfer_limits.check(
                        num_rows, num_bytes, "IN-subquery set shipped to distributed-plan tasks", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                    throw Exception(ErrorCodes::SET_SIZE_LIMIT_EXCEEDED,
                        "Cannot ship an IN-subquery set of {} rows ({} bytes) to distributed-plan worker tasks: "
                        "it exceeds the transfer limits", num_rows, num_bytes);

                writeIntBinary(SetSerializationKind::TupleValues, out);
                writeSetSourceState(built_set->sourceIsEmpty(), built_set->hasNull(), columns, out, flags.version);
                writeSetValues(built_set->getElementsTypes(), columns, out);
            }
            else
            {
                writeIntBinary(SetSerializationKind::SubqueryPlan, out);
                const auto * plan = from_subquery->getQueryPlan();
                if (!plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize FutureSetFromSubquery with no query plan");

                plan->serialize(out, flags);
            }
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
    const ContextPtr & context,
    size_t max_type_complexity)
{
    UInt64 num_sets = 0;
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

        UInt8 kind = 0;
        readVarUInt(kind, in);
        if (kind == UInt8(SetSerializationKind::StorageSet))
        {
            String storage_name;
            readStringBinary(storage_name, in);
            res.sets_from_storage.emplace_back(QueryPlanAndSets::SetFromStorage{{hash, std::move(columns)}, std::move(storage_name)});
        }
        else if (kind == UInt8(SetSerializationKind::TupleValues))
        {
            std::optional<bool> source_is_empty;
            bool source_has_null = false;
            if (flags.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SET_SOURCE_STATE)
            {
                UInt64 source_is_empty_value = 0;
                UInt64 source_has_null_value = 0;
                readVarUInt(source_is_empty_value, in);
                readVarUInt(source_has_null_value, in);
                if (source_is_empty_value > 1 || source_has_null_value > 1)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid serialized IN set source state");
                source_is_empty = source_is_empty_value != 0;
                source_has_null = source_has_null_value != 0;
            }

            UInt64 num_columns = 0;
            UInt64 num_rows = 0;
            readVarUInt(num_columns, in);
            readVarUInt(num_rows, in);

            if (num_columns > DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Suspiciously many columns ({}) in a serialized set (maximum: {})",
                    num_columns, DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS);
            if (num_rows > DEFAULT_NATIVE_BINARY_MAX_NUM_ROWS)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "Suspiciously many rows ({}) in a serialized set (maximum: {})",
                    num_rows, DEFAULT_NATIVE_BINARY_MAX_NUM_ROWS);

            ColumnsWithTypeAndName set_columns;
            set_columns.reserve(num_columns);

            /// The set data comes from the same plan stream, so it carries the plan's resolved type-complexity
            /// limit (the effective setting for client packets, 0 for trusted server-to-server plans). Pass it
            /// on both the column type and the column data (a Dynamic column decodes further types via NativeReader).
            FormatSettings format_settings;
            format_settings.binary.max_binary_type_complexity = max_type_complexity;

            for (size_t col = 0; col < num_columns; ++col)
            {
                auto type = decodeDataType(in, max_type_complexity);
                auto serialization = type->getDefaultSerialization();
                auto column = type->createColumn();
                NativeReader::readData(*serialization, *column, in, &format_settings, num_rows, nullptr, nullptr);

                set_columns.emplace_back(std::move(column), std::move(type), String{});
            }

            res.sets_from_tuple.emplace_back(QueryPlanAndSets::SetFromTuple{
                {hash, std::move(columns)}, std::move(set_columns), source_is_empty, source_has_null});
        }
        else if (kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            auto nested_flags = flags;
            ++nested_flags.subquery_nesting_depth;
            if (nested_flags.subquery_nesting_depth > MAX_SUBQUERY_PLAN_NESTING_DEPTH)
                throw Exception(
                    ErrorCodes::TOO_DEEP_RECURSION,
                    "Too deep nesting of subqueries for sets in deserialized query plan (more than {} levels)",
                    MAX_SUBQUERY_PLAN_NESTING_DEPTH);

            auto plan_for_set = QueryPlan::deserialize(in, context, nested_flags, max_type_complexity);

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
        auto table_node = resolveTable(identifier, context);
        const auto * storage_set = castStorage<StorageSet>(table_node->getStorage(), DeferredTable::Load).get();
        if (!storage_set)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Table {} is not a StorageSet", set.storage_name);

        /// The initiator ran this check against its own policies; the replica's may differ.
        storage_set->checkNoRowPolicy(context);

        /// The `StorageSet` this replica resolved the name to, so its set is mutable like any other.
        auto future_set = std::make_shared<FutureSetFromStorage>(
            set.hash, nullptr, storage_set->getSet(), table_node->getStorageID(), /*is_mutable_during_query_=*/ true);
        for (auto * column : set.columns)
            column->setData(future_set);
    }
}

static void makeSetsFromTuple(std::list<QueryPlanAndSets::SetFromTuple> sets, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    const FutureSetSettings set_settings(settings);
    for (auto & set : sets)
    {
        auto future_set = std::make_shared<FutureSetFromTuple>(
            set.hash,
            nullptr,
            std::move(set.set_columns),
            set_settings.transform_null_in,
            set_settings.size_limits,
            set.source_is_empty,
            set.source_has_null);
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

        auto future_set = std::make_shared<FutureSetFromSubquery>(
            set.hash, nullptr, std::make_unique<QueryPlan>(std::move(subquery_plan)), nullptr, nullptr, FutureSetSettings(settings));

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

void QueryPlan::serializeReadySets(SerializedSetsRegistry & registry, WriteBuffer & out)
{
    SerializationFlags flags;
    flags.sets_must_be_ready = true;
    serializeSets(registry, out, flags);
}

void QueryPlan::deserializeAndMakeSets(DeserializedSetsRegistry & registry, ReadBuffer & in, const ContextPtr & context)
{
    makeSets(deserializeSets(QueryPlan{}, registry, in, SerializationFlags{}, context, 0), context);
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
