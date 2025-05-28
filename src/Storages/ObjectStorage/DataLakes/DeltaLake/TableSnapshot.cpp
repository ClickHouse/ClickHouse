#include "config.h"
#include "delta_kernel_ffi.hpp"

#if USE_DELTA_KERNEL_RS

#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ObjectInfoWithPartitionColumns.h>
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include "getSchemaFromSnapshot.h"
#include "PartitionPruner.h"
#include "KernelUtils.h"
#include <fmt/ranges.h>

#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/DateLUTImpl.h>
#include <Common/LocalDate.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <DataTypes/DataTypesDecimal.h>

namespace fs = std::filesystem;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace ProfileEvents
{
    extern const Event DeltaLakePartitionPrunedFiles;
}

namespace DB
{

Field parseFieldFromString(const String & value, DB::DataTypePtr data_type)
{
    try
    {
        ReadBufferFromString buffer(value);
        auto col = data_type->createColumn();
        auto serialization = data_type->getSerialization(ISerialization::Kind::DEFAULT);
        serialization->deserializeWholeText(*col, buffer, FormatSettings{});
        return (*col)[0];
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot parse {} for data type {}: {}",
            value, data_type->getName(), getCurrentExceptionMessage(false));
    }
}

}

namespace DeltaLake
{

struct ExpressionVisitorData
{
    /// At this moment ExpressionVisitor is used only for partition columns,
    /// where only identifier expressions are allowed (only PARTITION BY col_name, ...),
    /// therefore we leave several visitor function as not implemented
    /// (see throwNotImplemented in createVisitor() below).
    /// They will be implemented once we start using statistics feature from delta-kernel.
    /// Also at this moment we use ASTPtr as a representation of the parsed expression.
    /// At this moment it suffices, as most of the visitor functions are not implemented, as explained above.
    /// But once they are implemented, most likely ASTPtr will be substituted.

    size_t list_counter = 0;
    std::unordered_map<size_t, std::unique_ptr<DB::ASTs>> type_lists;

    void addToList(size_t id, DB::ASTPtr value)
    {
        auto it = type_lists.find(id);
        if (it == type_lists.end())
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "List with id {} does not exist", id);
        }
        it->second->push_back(value);
    }

    DB::Field getValue(size_t idx)
    {
        if (type_lists.empty())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Type list is empty");

        if (!type_lists[0])
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Type list zero level value is Null");

        if (type_lists[0]->size() != 1)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Unexpected size of type list zero level value: {}", type_lists[0]->size());
        }

        const auto * expression_list = (*type_lists[0])[0]->as<DB::ASTExpressionList>();
        if (!expression_list)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Not an expression list at zero level of type list");
        }

        if (expression_list->children.size() <= idx)
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Index {} out of bounds of type list (type list size: {})",
                idx, expression_list->children.size());
        }

        const auto * literal = expression_list->children[idx]->as<DB::ASTLiteral>();
        if (!literal)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Value at position {} is not a literal", idx);

        return literal->value;
    }

    std::exception_ptr visitor_exception;
    LoggerPtr log = getLogger("DeltaLakeExpressionVisitor");
};

class ExpressionVisitor
{
public:
    static void visit(const ffi::Expression * expression, ExpressionVisitorData & data)
    {
        auto visitor = createVisitor(data);
        [[maybe_unused]] uintptr_t result = ffi::visit_expression_ref(expression, &visitor);
        chassert(result == 0, "Unexpected result: " + DB::toString(result));

        if (data.visitor_exception)
            std::rethrow_exception(data.visitor_exception);
    }

private:
    enum NotImplementedMethod
    {
        AND,
        OR,
        LT,
        LE,
        GT,
        GE,
        EQ,
        NE,
        DISTINCT,
        IN,
        NOT_IN,
        ADD,
        MINUS,
        MULTIPLY,
        DIVIDE,
        NOT,
        IS_NULL,
    };
    static ffi::EngineExpressionVisitor createVisitor(ExpressionVisitorData & data)
    {
        ffi::EngineExpressionVisitor visitor;
        visitor.data = &data;
        visitor.make_field_list = &makeFieldList;

        visitor.visit_literal_bool = &visitSimpleLiteral<bool>;
        visitor.visit_literal_byte = &visitSimpleLiteral<int8_t>;
        visitor.visit_literal_short = &visitSimpleLiteral<int16_t>;
        visitor.visit_literal_int = &visitSimpleLiteral<int32_t>;
        visitor.visit_literal_long = &visitSimpleLiteral<int64_t>;
        visitor.visit_literal_float = &visitSimpleLiteral<float>;
        visitor.visit_literal_double = &visitSimpleLiteral<double>;

        visitor.visit_literal_string = &visitStringLiteral;
        visitor.visit_literal_decimal = &visitDecimalLiteral;

        visitor.visit_literal_timestamp = &visitTimestampLiteral;
        visitor.visit_literal_timestamp_ntz = &visitTimestampNtzLiteral;
        visitor.visit_literal_date = &visitDateLiteral;
        visitor.visit_literal_binary = &visitBinaryLiteral;
        visitor.visit_literal_null = &visitNullLiteral;
        visitor.visit_literal_array = &visitArrayLiteral;
        visitor.visit_literal_struct = &visitStructLiteral;

        visitor.visit_column = &visitColumnExpression;
        visitor.visit_struct_expr = &visitStructExpression;

        visitor.visit_and = &throwNotImplemented<AND>;
        visitor.visit_or = &throwNotImplemented<OR>;
        visitor.visit_lt = &throwNotImplemented<LT>;
        visitor.visit_le = &throwNotImplemented<LE>;
        visitor.visit_gt = &throwNotImplemented<GT>;
        visitor.visit_ge = &throwNotImplemented<GE>;
        visitor.visit_eq = &throwNotImplemented<EQ>;
        visitor.visit_ne = &throwNotImplemented<NE>;
        visitor.visit_distinct = &throwNotImplemented<DISTINCT>;
        visitor.visit_in = &throwNotImplemented<IN>;
        visitor.visit_not_in = &throwNotImplemented<NOT_IN>;
        visitor.visit_add = &throwNotImplemented<ADD>;
        visitor.visit_minus = &throwNotImplemented<MINUS>;
        visitor.visit_multiply = &throwNotImplemented<MULTIPLY>;
        visitor.visit_divide = &throwNotImplemented<DIVIDE>;
        visitor.visit_not = &throwNotImplemented<NOT>;
        visitor.visit_is_null = &throwNotImplemented<IS_NULL>;

        return visitor;
    }

    static uintptr_t makeFieldList(void * data, uintptr_t capacity_hint)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        size_t id = state->list_counter++;

        auto list = std::make_unique<DB::ASTs>();
        if (capacity_hint > 0)
            list->reserve(capacity_hint);

        state->type_lists.emplace(id, std::move(list));
        return id;
    }

    template <typename Func>
    static void visitorImpl(ExpressionVisitorData & data, Func func)
    {
        try
        {
            func();
        }
        catch (...)
        {
            /// We cannot allow to throw exceptions from visitor functions,
            /// otherwise delta-kernel will panic and call terminate.
            data.visitor_exception = std::current_exception();
        }
    }

    template <NotImplementedMethod method>
    static void throwNotImplemented(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_list_id)
    {
        UNUSED(sibling_list_id);
        UNUSED(child_list_id);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Method {} not implemented", magic_enum::enum_name(method));
        });
    }

    static void visitColumnExpression(void * data, uintptr_t sibling_list_id, ffi::KernelStringSlice name)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            const auto name_str = KernelUtils::fromDeltaString(name);
            LOG_TEST(state->log, "Column expression list id: {}, name: {}", sibling_list_id, name_str);
            state->addToList(sibling_list_id, std::make_shared<DB::ASTIdentifier>(name_str));
        });
    }

    static void visitStructExpression(
        void * data,
        uintptr_t sibling_list_id,
        uintptr_t child_list_id)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            auto it = state->type_lists.find(sibling_list_id);
            if (it == state->type_lists.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "List with id {} does not exist", sibling_list_id);
            }
            auto child_it = state->type_lists.find(child_list_id);
            if (child_it == state->type_lists.end())
            {
                throw DB::Exception(
                    DB::ErrorCodes::LOGICAL_ERROR,
                    "Child list with id {} does not exist", sibling_list_id);
            }

            LOG_TEST(state->log, "Struct expression list id: {}, child list id: {}", sibling_list_id, child_list_id);

            auto list = std::make_shared<DB::ASTExpressionList>();
            list->children = std::move(*child_it->second);
            state->type_lists.erase(child_it);
            state->addToList(sibling_list_id, list);
        });
    }

    template <typename T>
    static void visitSimpleLiteral(void * data, uintptr_t sibling_list_id, T value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            LOG_TEST(state->log, "Expression list id: {}, value: {}", sibling_list_id, value);
            state->addToList(sibling_list_id, std::make_shared<DB::ASTLiteral>(value));
        });
    }

    static void visitStringLiteral(void * data, uintptr_t sibling_list_id, ffi::KernelStringSlice value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            auto value_str = KernelUtils::fromDeltaString(value);
            visitSimpleLiteral<std::string>(data, sibling_list_id, value_str);
        });
    }

    static void visitDecimalLiteral(
        void * data,
        uintptr_t sibling_list_id,
        int64_t value_ms,
        uint64_t value_ls,
        uint8_t precision,
        uint8_t scale)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            DB::Field value;
            if (precision <= DB::DecimalUtils::max_precision<DB::Decimal32>)
            {
                value = DB::DecimalField<DB::Decimal32>(value_ms, scale);
            }
            else if (precision <= DB::DecimalUtils::max_precision<DB::Decimal64>)
            {
                value = DB::DecimalField<DB::Decimal64>(value_ms, scale);
            }
            else if (precision <= DB::DecimalUtils::max_precision<DB::Decimal128>)
            {
                /// From delta-kernel-rs:
                /// "The 128bit integer
                /// is split into the most significant 64 bits in `value_ms`, and the least significant 64
                /// bits in `value_ls`"
                /// But in clickhouse decimal is in little endian, so we switch the order.
                Int128 combined_value = (static_cast<DB::Int128>(value_ls) << 64) | value_ms;
                value = DB::DecimalField<DB::Decimal128>(combined_value, scale);
            }

            LOG_TEST(state->log, "Expression element: {}", value);

            state->addToList(sibling_list_id, std::make_shared<DB::ASTLiteral>(value));
        });
    }

    static void visitDateLiteral(void * data, uintptr_t sibling_list_id, int32_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            const ExtendedDayNum daynum{value};
            LOG_TEST(state->log, "Expression Date element: {} (value: {})", DateLUT::instance().dateToString(daynum), value);
            state->addToList(sibling_list_id, std::make_shared<DB::ASTLiteral>(daynum.toUnderType()));
        });
    }

    static void visitTimestampLiteral(void * data, uintptr_t sibling_list_id, int64_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            const auto datetime_value = DB::DecimalField<DB::Decimal64>(value, 6);
            state->addToList(sibling_list_id, std::make_shared<DB::ASTLiteral>(datetime_value));
        });
    }

    static void visitTimestampNtzLiteral(void * data, uintptr_t sibling_list_id, int64_t value)
    {
        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            const auto datetime_value = DB::DecimalField<DB::Decimal64>(value, 6);
            state->addToList(sibling_list_id, std::make_shared<DB::ASTLiteral>(datetime_value));
        });
    }

    static void visitBinaryLiteral(
        void * data, uintptr_t sibling_list_id, const uint8_t * buffer, uintptr_t len)
    {
        UNUSED(sibling_list_id);
        UNUSED(buffer);
        UNUSED(len);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Method visitBinaryLiteral not implemented");
        });
    }

    static void visitNullLiteral(void * data, uintptr_t sibling_list_id)
    {
        UNUSED(sibling_list_id);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Method visitNullLiteral not implemented");
        });
    }

    static void visitArrayLiteral(void * data, uintptr_t sibling_list_id, uintptr_t child_id)
    {
        UNUSED(sibling_list_id);
        UNUSED(child_id);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Method visitArrayLiteral not implemented");
        });
    }

    static void visitStructLiteral(
        void * data, uintptr_t sibling_list_id, uintptr_t child_field_list_value, uintptr_t child_value_list_id)
    {
        UNUSED(sibling_list_id);
        UNUSED(child_field_list_value);
        UNUSED(child_value_list_id);

        ExpressionVisitorData * state = static_cast<ExpressionVisitorData *>(data);
        visitorImpl(*state, [&]()
        {
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Method visitStructLiteral not implemented");
        });
    }
};

class TableSnapshot::Iterator final : public DB::IObjectIterator
{
public:
    Iterator(
        const KernelExternEngine & engine_,
        const KernelSnapshot & snapshot_,
        KernelScan & scan_,
        const std::string & data_prefix_,
        const DB::NamesAndTypesList & schema_,
        const DB::Names & partition_columns_,
        const DB::NameToNameMap & physical_names_map_,
        DB::ObjectStoragePtr object_storage_,
        const DB::ActionsDAG * filter_dag_,
        DB::IDataLakeMetadata::FileProgressCallback callback_,
        size_t list_batch_size_,
        LoggerPtr log_)
        : engine(engine_)
        , snapshot(snapshot_)
        , scan(scan_)
        , data_prefix(data_prefix_)
        , schema(schema_)
        , partition_columns(partition_columns_)
        , physical_names_map(physical_names_map_)
        , object_storage(object_storage_)
        , callback(callback_)
        , list_batch_size(list_batch_size_)
        , log(log_)
        , thread([&, thread_group = DB::CurrentThread::getGroup()] {
            /// Attach to current query thread group, to be able to
            /// have query id in logs and metrics from scanDataFunc.
            DB::ThreadGroupSwitcher switcher(thread_group, "TableSnapshot");
            scanDataFunc();
        })
    {
        if (filter_dag_)
            pruner.emplace(*filter_dag_, schema_, partition_columns_, DB::Context::getGlobalContextInstance());
    }

    ~Iterator() override
    {
        shutdown.store(true);
        schedule_next_batch_cv.notify_one();
        if (thread.joinable())
            thread.join();
    }

    void initScanState()
    {
        scan = KernelUtils::unwrapResult(ffi::scan(snapshot.get(), engine.get(), /* predicate */{}), "scan");
        scan_data_iterator = KernelUtils::unwrapResult(
            ffi::scan_metadata_iter_init(engine.get(), scan.get()),
            "scan_metadata_iter_init");
    }

    void scanDataFunc()
    {
        initScanState();
        while (!shutdown.load())
        {
            bool have_scan_data_res = KernelUtils::unwrapResult(
                ffi::scan_metadata_next(scan_data_iterator.get(), this, visitData),
                "scan_metadata_next");

            if (have_scan_data_res)
            {
                std::unique_lock lock(next_mutex);
                if (!shutdown.load() && list_batch_size && data_files.size() >= list_batch_size)
                {
                    LOG_TEST(log, "List batch size is {}/{}", data_files.size(), list_batch_size);

                    schedule_next_batch_cv.wait(
                        lock,
                        [&]() { return (data_files.size() < list_batch_size) || shutdown.load(); });
                }
            }
            else
            {
                LOG_TEST(log, "All data files were listed");
                {
                    std::lock_guard lock(next_mutex);
                    iterator_finished = true;
                }
                data_files_cv.notify_all();
                return;
            }
        }
    }

    size_t estimatedKeysCount() override
    {
        /// For now do the same as StorageObjectStorageSource::GlobIterator.
        /// TODO: is it possible to do a precise estimation?
        return std::numeric_limits<size_t>::max();
    }

    DB::ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            DB::ObjectInfoPtr object;
            {
                std::unique_lock lock(next_mutex);
                if (!iterator_finished && data_files.empty())
                {
                    LOG_TEST(log, "Waiting for next data file");
                    schedule_next_batch_cv.notify_one();
                    data_files_cv.wait(lock, [&]() { return !data_files.empty() || iterator_finished; });
                }

                if (scan_exception)
                    std::rethrow_exception(scan_exception);

                if (data_files.empty())
                    return nullptr;

                LOG_TEST(log, "Current data files: {}", data_files.size());

                object = data_files.front();
                data_files.pop_front();
                if (data_files.empty())
                    schedule_next_batch_cv.notify_one();
            }

            chassert(object);
            if (pruner.has_value())
            {
                const auto * object_with_partition_info = dynamic_cast<const DB::ObjectInfoWithPartitionColumns *>(object.get());
                if (object_with_partition_info && pruner->canBePruned(*object_with_partition_info))
                {
                    ProfileEvents::increment(ProfileEvents::DeltaLakePartitionPrunedFiles);

                    LOG_TEST(log, "Skipping file {} according to partition pruning", object->getPath());
                    continue;
                }
            }

            object->metadata = object_storage->getObjectMetadata(object->getPath());

            if (callback)
            {
                chassert(object->metadata);
                callback(DB::FileProgress(0, object->metadata->size_bytes));
            }
            return object;
        }
    }

    static void visitData(
        void * engine_context,
        ffi::SharedScanMetadata * scan_metadata)
    {
        ffi::visit_scan_metadata(scan_metadata, engine_context, Iterator::scanCallback);
        ffi::free_scan_metadata(scan_metadata);
    }

    static void scanCallback(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::DvInfo * dv_info,
        const ffi::Expression * transform,
        const struct ffi::CStringMap * deprecated)
    {
        try
        {
            scanCallbackImpl(engine_context, path, size, stats, dv_info, transform, deprecated);
        }
        catch (...)
        {
            /// We cannot allow to throw exceptions from ScanCallback,
            /// otherwise delta-kernel will panic and call terminate.
            auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
            context->scan_exception = std::current_exception();
        }
    }

    static void scanCallbackImpl(
        ffi::NullableCvoid engine_context,
        struct ffi::KernelStringSlice path,
        int64_t size,
        const ffi::Stats * stats,
        const ffi::DvInfo * /* dv_info */,
        const ffi::Expression * transform,
        const struct ffi::CStringMap * /* deprecated */)
    {
        auto * context = static_cast<TableSnapshot::Iterator *>(engine_context);
        std::string full_path = fs::path(context->data_prefix) / DB::unescapeForFileName(KernelUtils::fromDeltaString(path));

        /// Collect partition values info.
        /// DeltaLake does not store partition values in the actual data files,
        /// but instead in data files paths directory names.
        /// So we extract these values here and put into `partitions_info`.
        DB::ObjectInfoWithPartitionColumns::PartitionColumnsInfo partitions_info;
        if (transform)
        {
            ExpressionVisitorData data;
            ExpressionVisitor::visit(transform, data);
            for (const auto & partition_column : context->partition_columns)
            {
                auto name_and_type = context->schema.tryGetByName(partition_column);
                if (!name_and_type.has_value())
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Cannot find parititon column {} in schema",
                        partition_column);
                }

                const auto pos = context->schema.getPosByName(partition_column);
                const auto value = data.getValue(pos);
                partitions_info.emplace_back(name_and_type.value(), value);
            }
        }

        LOG_TEST(
            context->log,
            "Scanned file: {}, size: {}, num records: {}, partition columns: {}",
            full_path, size, stats ? DB::toString(stats->num_records) : "Unknown", partitions_info.size());

        DB::ObjectInfoPtr object;
        if (partitions_info.empty())
            object = std::make_shared<DB::ObjectInfo>(std::move(full_path));
        else
            object = std::make_shared<DB::ObjectInfoWithPartitionColumns>(std::move(partitions_info), std::move(full_path));

        {
            std::lock_guard lock(context->next_mutex);
            context->data_files.push_back(std::move(object));
        }
        context->data_files_cv.notify_one();
    }

private:
    using KernelScan = KernelPointerWrapper<ffi::SharedScan, ffi::free_scan>;
    using KernelScanDataIterator = KernelPointerWrapper<ffi::SharedScanMetadataIterator, ffi::free_scan_metadata_iter>;

    const KernelExternEngine & engine;
    const KernelSnapshot & snapshot;
    KernelScan & scan;
    KernelScanDataIterator scan_data_iterator;
    std::optional<PartitionPruner> pruner;

    const std::string data_prefix;
    [[maybe_unused]] const DB::NamesAndTypesList & schema;
    [[maybe_unused]] const DB::Names & partition_columns;
    [[maybe_unused]] const DB::NameToNameMap & physical_names_map;
    const DB::ObjectStoragePtr object_storage;
    const DB::IDataLakeMetadata::FileProgressCallback callback;
    const size_t list_batch_size;
    const LoggerPtr log;
    std::exception_ptr scan_exception;

    /// Whether scanDataFunc should stop scanning.
    /// Set in destructor.
    std::atomic<bool> shutdown = false;
    /// A CV to notify that new data_files are available.
    std::condition_variable data_files_cv;
    /// A flag meaning that all data files were scanned
    /// and data scanning thread is finished.
    bool iterator_finished = false;

    /// A CV to notify data scanning thread to continue,
    /// as current data batch is fully read.
    std::condition_variable schedule_next_batch_cv;

    std::deque<DB::ObjectInfoPtr> data_files;
    std::mutex next_mutex;

    /// A thread for async data scanning.
    ThreadFromGlobalPool thread;
};


TableSnapshot::TableSnapshot(
    KernelHelperPtr helper_,
    DB::ObjectStoragePtr object_storage_,
    bool,
    LoggerPtr log_)
    : helper(helper_)
    , object_storage(object_storage_)
    , log(log_)
{
}

size_t TableSnapshot::getVersion() const
{
    initSnapshot();
    return snapshot_version;
}

bool TableSnapshot::update()
{
    if (!snapshot.get())
    {
        /// Snapshot is not yet created,
        /// so next attempt to create it would return the latest snapshot.
        return false;
    }
    initSnapshotImpl();
    return true;
}

void TableSnapshot::initSnapshot() const
{
    if (snapshot.get())
        return;
    initSnapshotImpl();
}

void TableSnapshot::initSnapshotImpl() const
{
    LOG_TEST(log, "Initializing snapshot");

    auto * engine_builder = helper->createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    snapshot = KernelUtils::unwrapResult(
        ffi::snapshot(KernelUtils::toDeltaString(helper->getTableLocation()), engine.get()), "snapshot");
    snapshot_version = ffi::version(snapshot.get());
    LOG_TRACE(log, "Snapshot version: {}", snapshot_version);

    scan = KernelUtils::unwrapResult(ffi::scan(snapshot.get(), engine.get(), /* predicate */{}), "scan");
    scan_state = ffi::get_global_scan_state(scan.get());
    LOG_TRACE(log, "Initialized scan state");

    std::tie(table_schema, physical_names_map) = getTableSchemaFromSnapshot(snapshot.get());
    LOG_TRACE(log, "Table logical schema: {}", fmt::join(table_schema.getNames(), ", "));

    read_schema = getReadSchemaFromSnapshot(scan_state.get());
    LOG_TRACE(log, "Table read schema: {}", fmt::join(read_schema.getNames(), ", "));

    partition_columns = getPartitionColumnsFromSnapshot(snapshot.get());
    LOG_TRACE(log, "Partition columns: {}", fmt::join(partition_columns, ", "));
}

DB::ObjectIterator TableSnapshot::iterate(
    const DB::ActionsDAG * filter_dag,
    DB::IDataLakeMetadata::FileProgressCallback callback,
    size_t list_batch_size)
{
    initSnapshot();
    return std::make_shared<TableSnapshot::Iterator>(
        engine,
        snapshot,
        scan,
        helper->getDataPath(),
        getTableSchema(),
        getPartitionColumns(),
        getPhysicalNamesMap(),
        object_storage,
        filter_dag,
        callback,
        list_batch_size,
        log);
}

const DB::NamesAndTypesList & TableSnapshot::getTableSchema() const
{
    initSnapshot();
    return table_schema;
}

const DB::NamesAndTypesList & TableSnapshot::getReadSchema() const
{
    initSnapshot();
    return read_schema;
}

const DB::Names & TableSnapshot::getPartitionColumns() const
{
    initSnapshot();
    return partition_columns;
}

const DB::NameToNameMap & TableSnapshot::getPhysicalNamesMap() const
{
    initSnapshot();
    return physical_names_map;
}

}

#endif
