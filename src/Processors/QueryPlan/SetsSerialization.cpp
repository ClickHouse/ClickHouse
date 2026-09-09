#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanOutline.h>
#include <Processors/QueryPlan/Serialization.h>

#include <Core/ProtocolDefines.h>
#include <IO/LimitReadBuffer.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/resolveStorages.h>

#include <base/scope_guard.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <tuple>

#include <Analyzer/Identifier.h>
#include <Analyzer/TableNode.h>
#include <Columns/ColumnSet.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Set.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Formats/NativeReader.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeWriter.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Storages/StorageSet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int SUPPORT_IS_DISABLED;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_serialized_query_plan_size;
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

/// Sanity caps for a hostile set payload; checked before allocation.
static constexpr UInt64 MAX_SET_COLUMNS = 1'000'000;
static constexpr UInt64 MAX_SET_STORAGE_NAME_BYTES = 16ULL << 20;

static void checkSetColumnsCount(UInt64 num_columns, const PreparedSets::Hash & hash)
{
    if (num_columns > MAX_SET_COLUMNS)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Serialized set {}_{} declares {} columns which exceeds the limit of {}",
            hash.low64, hash.high64, num_columns, MAX_SET_COLUMNS);
}

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

std::vector<std::pair<FutureSet::Hash, FutureSet *>> SerializedSetsRegistry::entriesSortedByHash() const
{
    std::vector<std::pair<FutureSet::Hash, FutureSet *>> ordered;
    ordered.reserve(sets.size());
    for (const auto & [hash, set] : sets)
        ordered.emplace_back(hash, set.get());
    std::sort(ordered.begin(), ordered.end(), [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs.first.high64, lhs.first.low64) < std::tie(rhs.first.high64, rhs.first.low64);
    });
    return ordered;
}

/// The values of a subquery set, ready to ship to a distributed-plan worker task. A task binds a
/// ready set before its own planning; shipping the subquery plan instead would make every task run
/// the subquery again. Throws when the set is not complete or is over the transfer limits: a
/// truncated set would change `IN` results on the workers, so `transfer_overflow_mode = 'break'`
/// does not apply here.
static Columns readySubquerySetValues(const FutureSetFromSubquery & from_subquery, const SizeLimits & transfer_limits, DataTypes & types)
{
    auto built_set = from_subquery.get();
    if (!built_set || !built_set->hasExplicitSetElements() || built_set->isTruncated())
    {
        String reason = !built_set ? "the set is not built"
            : built_set->isTruncated() ? "the set was truncated by `set_overflow_mode = 'break'`"
                                       : "the set was built without its values";
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot ship an IN-subquery set to distributed-plan worker tasks ({}): {}",
            reason,
            from_subquery.getSourceAST() ? from_subquery.getSourceAST()->formatForErrorMessage() : "");
    }

    auto columns = built_set->getSetElements();
    UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
    size_t num_bytes = 0;
    for (const auto & column : columns)
        num_bytes += column->byteSize();
    if (!transfer_limits.check(
            num_rows, num_bytes, "IN-subquery set shipped to distributed-plan tasks", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
        throw Exception(ErrorCodes::SET_SIZE_LIMIT_EXCEEDED,
            "Cannot ship an IN-subquery set of {} rows ({} bytes) to distributed-plan worker tasks: "
            "it exceeds the transfer limits", num_rows, num_bytes);

    types = built_set->getElementsTypes();
    return columns;
}

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

void QueryPlan::serializeSets(SerializedSetsRegistry & registry, WriteBuffer & out, const SerializationFlags & flags)
{
    /// Write sets sorted by hash, not in the unordered map iteration order,
    /// so the same plan serializes to the same bytes in every process.
    auto ordered_sets = registry.entriesSortedByHash();

    writeVarUInt(ordered_sets.size(), out);
    for (const auto & [hash, set_ptr] : ordered_sets)
    {
        writeBinary(hash, out);

        if (auto * from_storage = typeid_cast<FutureSetFromStorage *>(set_ptr))
        {
            writeIntBinary(SetSerializationKind::StorageSet, out);
            const auto & storage_id = from_storage->getStorageID();
            if (!storage_id)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "FutureSetFromStorage without storage id");

            auto storage_name = storage_id->getFullTableName();
            writeStringBinary(storage_name, out);
        }
        else if (auto * from_tuple = typeid_cast<FutureSetFromTuple *>(set_ptr))
        {
            writeIntBinary(SetSerializationKind::TupleValues, out);
            writeSetValues(from_tuple->getTypes(), from_tuple->getKeyColumns(), out);
        }
        else if (auto * from_subquery = typeid_cast<FutureSetFromSubquery *>(set_ptr))
        {
            if (flags.sets_must_be_ready)
            {
                DataTypes types;
                auto columns = readySubquerySetValues(*from_subquery, flags.sets_transfer_limits, types);
                writeIntBinary(SetSerializationKind::TupleValues, out);
                writeSetValues(types, columns, out);
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
            const auto & set_ref = *set_ptr;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown FutureSet type {}", typeid(set_ref).name());
        }
    }
}

void serializeFramedSets(
    SerializedSetsRegistry & registry,
    const QueryPlan::SerializationFlags & flags,
    PlanOutline & outline,
    std::vector<String> & payloads)
{
    auto ordered_sets = registry.entriesSortedByHash();
    outline.sets.reserve(ordered_sets.size());
    payloads.reserve(ordered_sets.size());

    for (const auto & [hash, set_ptr] : ordered_sets)
    {
        PlanOutline::SetEntry entry;
        entry.hash = hash;

        WriteBufferFromOwnString body;

        if (auto * from_storage = typeid_cast<FutureSetFromStorage *>(set_ptr))
        {
            entry.kind = UInt8(SetSerializationKind::StorageSet);
            const auto & storage_id = from_storage->getStorageID();
            if (!storage_id)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "FutureSetFromStorage without storage id");
            writeStringBinary(storage_id->getFullTableName(), body);
        }
        else if (auto * from_tuple = typeid_cast<FutureSetFromTuple *>(set_ptr))
        {
            entry.kind = UInt8(SetSerializationKind::TupleValues);
            auto types = from_tuple->getTypes();
            writeSetValues(types, from_tuple->getKeyColumns(), body);
        }
        else if (auto * from_subquery = typeid_cast<FutureSetFromSubquery *>(set_ptr))
        {
            if (flags.sets_must_be_ready)
            {
                entry.kind = UInt8(SetSerializationKind::TupleValues);
                DataTypes types;
                auto columns = readySubquerySetValues(*from_subquery, flags.sets_transfer_limits, types);
                writeSetValues(types, columns, body);
            }
            else
            {
                entry.kind = UInt8(SetSerializationKind::SubqueryPlan);
                const auto * plan = from_subquery->getQueryPlan();
                if (!plan)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize FutureSetFromSubquery with no query plan");

                /// A whole plan with its own leading version, so it says how long it is and what it
                /// is, which the older stream did not: there the nested plan just ran on inline. It
                /// is written at the version the outer plan settled on rather than choosing again,
                /// because a query that asks for a version has to get it for the nested plans too.
                plan->serialize(body, flags.version, flags.version);
            }
        }
        else
        {
            const auto & set_ref = *set_ptr;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown FutureSet type {}", typeid(set_ref).name());
        }

        body.finalize();
        outline.sets.push_back(entry);
        /// `body` is finalized and goes out of scope here, so a large set moves rather than copies.
        payloads.push_back(std::move(body.str()));
    }
}

QueryPlanAndSets deserializeFramedSets(
    QueryPlan plan,
    DeserializedSetsRegistry & registry,
    const PlanOutline & outline,
    ReadBuffer & in,
    const QueryPlan::SerializationFlags & flags,
    const ContextPtr & context,
    size_t max_type_complexity,
    UInt64 max_plan_bytes,
    size_t body_start,
    UInt64 & frames_consumed)
{
    auto budget_left = [&]() -> UInt64
    {
        const size_t used = in.count() - body_start;
        return used >= max_plan_bytes ? 0 : max_plan_bytes - used;
    };

    QueryPlanAndSets res;
    res.plan = std::move(plan);

    for (const auto & entry : outline.sets)
    {
        auto it = registry.sets.find(entry.hash);
        if (it == registry.sets.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is not registered", entry.hash.low64, entry.hash.high64);

        auto & columns = it->second;
        if (columns.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is serialized twice", entry.hash.low64, entry.hash.high64);

        /// The set size is read inline, right before its bytes. Reading it at a frame boundary keeps
        /// the stream aligned for the drain if this or a later set fails.
        UInt64 payload_size = 0;
        readVarUInt(payload_size, in);

        /// Reading through a buffer that stops at the set's own bytes keeps a nested plan from
        /// running into the next set or into the protocol after the plan. The scope guard steps over
        /// the frame and counts it whatever happens, so a failure here leaves the stream at a frame
        /// boundary for the drain the caller runs.
        LimitReadBuffer body(in, {.read_no_more = payload_size});
        SCOPE_EXIT({
            try { body.ignoreAll(); } catch (...) {} // Ok: best-effort step over the frame; a stream ending here is taken by the caller's drain // NOLINT(bugprone-empty-catch)
            ++frames_consumed;
        });

        /// The plan as a whole must fit the size limit; a set that pushes it past the limit is taken
        /// off the stream by the guard and refused, without losing the connection.
        if (payload_size > budget_left())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Serialized set {}_{} pushes the plan past `max_serialized_query_plan_size`",
                entry.hash.low64, entry.hash.high64);

        if (entry.kind == UInt8(SetSerializationKind::StorageSet))
        {
            String storage_name;
            readStringBinary(storage_name, body, MAX_SET_STORAGE_NAME_BYTES);
            res.sets_from_storage.emplace_back(QueryPlanAndSets::SetFromStorage{{entry.hash, std::move(columns)}, std::move(storage_name)});
        }
        else if (entry.kind == UInt8(SetSerializationKind::TupleValues))
        {
            UInt64 num_columns = 0;
            UInt64 num_rows = 0;
            readVarUInt(num_columns, body);
            readVarUInt(num_rows, body);
            checkSetColumnsCount(num_columns, entry.hash);

            /// Without this a few bytes could ask for an arbitrary allocation: `NativeReader::readData`
            /// sizes the column from the row count before reading it, and a row costs at least a byte.
            if (num_rows > body.bytesUntilLimit())
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Serialized set {}_{} declares {} rows but only {} bytes of its frame remain",
                    entry.hash.low64, entry.hash.high64, num_rows, body.bytesUntilLimit());

            ColumnsWithTypeAndName set_columns;

            FormatSettings format_settings;
            format_settings.binary.max_binary_type_complexity = max_type_complexity;

            for (size_t col = 0; col < num_columns; ++col)
            {
                auto type = decodeDataType(body, max_type_complexity);
                auto serialization = type->getDefaultSerialization();
                auto column = type->createColumn();
                NativeReader::readData(*serialization, *column, body, &format_settings, num_rows, nullptr, nullptr);

                set_columns.emplace_back(std::move(column), std::move(type), String{});
            }

            res.sets_from_tuple.emplace_back(QueryPlanAndSets::SetFromTuple{{entry.hash, std::move(columns)}, std::move(set_columns)});
        }
        else if (entry.kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            auto plan_for_set = QueryPlan::deserialize(body, context, max_type_complexity, flags.skip_data);

            res.sets_from_subquery.emplace_back(QueryPlanAndSets::SetFromSubquery{
                {entry.hash, std::move(columns)},
                std::move(plan_for_set)});
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} has unknown kind {}",
                entry.hash.low64, entry.hash.high64, int(entry.kind));

        if (body.bytesUntilLimit() != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Serialized set {}_{} did not consume its payload frame ({} bytes left)",
                entry.hash.low64, entry.hash.high64, body.bytesUntilLimit());
    }

    /// Every set a step referenced must have had its data in the outline. A binding left unfilled
    /// means the plan points at a set with no serialized payload, which would fail only later at
    /// execution ("No Set is passed"); reject it here at the serialization boundary instead.
    for (const auto & [hash, columns] : registry.sets)
        if (!columns.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Set {}_{} is referenced by the plan but no serialized data for it is present", hash.low64, hash.high64);

    return res;
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
            readStringBinary(storage_name, in, MAX_SET_STORAGE_NAME_BYTES);
            res.sets_from_storage.emplace_back(QueryPlanAndSets::SetFromStorage{{hash, std::move(columns)}, std::move(storage_name)});
        }
        else if (kind == UInt8(SetSerializationKind::TupleValues))
        {
            UInt64 num_columns = 0;
            UInt64 num_rows = 0;
            readVarUInt(num_columns, in);
            readVarUInt(num_rows, in);
            checkSetColumnsCount(num_columns, hash);

            /// Without this the row count could ask for an arbitrary allocation. A legacy stream
            /// has no frame, so the bound is the accepted plan size: a row costs at least a byte.
            const UInt64 max_plan_bytes = context->getServerSettings()[ServerSetting::max_serialized_query_plan_size];
            if (num_rows > max_plan_bytes)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Serialized set {}_{} declares {} rows, more than the {} bytes a plan may have",
                    hash.low64, hash.high64, num_rows, max_plan_bytes);

            ColumnsWithTypeAndName set_columns;

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

            res.sets_from_tuple.emplace_back(QueryPlanAndSets::SetFromTuple{{hash, std::move(columns)}, std::move(set_columns)});
        }
        else if (kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            auto plan_for_set = QueryPlan::deserialize(in, context, flags, max_type_complexity);

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
