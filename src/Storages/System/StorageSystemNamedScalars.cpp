#include <Storages/System/StorageSystemNamedScalars.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>

namespace DB
{

namespace
{
DataTypePtr makeKindEnum()
{
    return std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"local", 0},
        {"shared", 1},
    });
}
}

StorageSystemNamedScalars::StorageSystemNamedScalars(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemNamedScalars::getColumnsDescription()
{
    /// Two-tier disclosure:
    ///   * Value tier (getNamedScalar grant) - name, value, freshness.
    ///   * Operator tier (SHOW_NAMED_SCALARS) - body, errors, definer.
    ///     Nullable so the value-only viewer sees NULL, not partial truth.
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Named scalar name."},
        {"kind", makeKindEnum(), "Named scalar kind: local or shared."},
        {"value", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Last known value as string, NULL if missing."},
        {"loading_start_time", std::make_shared<DataTypeDateTime>(), "Time when the named scalar definition was loaded into memory."},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the last refresh attempt."},
        {"next_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Next scheduled refresh time."},
        {"last_success_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Time of the last successful refresh."},
        {"refresh_interval", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Refresh interval in seconds."},
        {"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Type of the current named scalar value."},
        {"has_value", std::make_shared<DataTypeUInt8>(), "Whether a last-good value exists."},
        {"current_value_is_valid", std::make_shared<DataTypeUInt8>(), "Whether the last refresh attempt succeeded."},
        /// --- Operator tier (SHOW_NAMED_SCALARS): NULL when the reader has only `getNamedScalar`.
        {"last_refresh_hostname", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Hostname of the last refresher."},
        {"definer", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "User the refresh runs as (SQL SECURITY DEFINER)."},
        {"expression", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Named scalar definition expression."},
        {"exception", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Last refresh error message (with error type prefix), if any."},
        {"refresh_in_flight", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "1 if a refresh body is currently executing for this scalar."},
        {"refresh_started_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Wall-clock time the in-flight refresh started; NULL if not refreshing."},
        {"consecutive_failures", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Number of consecutive failed refreshes since the last successful one."},
    };
}

void StorageSystemNamedScalars::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool show_full = access->isGranted(AccessType::SHOW_NAMED_SCALARS);
    const bool show_value_only = access->isGranted(AccessType::getNamedScalar);
    if (!show_full && !show_value_only)
        return;

    for (const auto & scoped_scalar : context->getNamedScalarsManager().listScalars())
    {
        const auto & scalar = scoped_scalar.scalar;
        const auto status = scalar->getInfo();

        size_t col = 0;
        // Value tier
        res_columns[col++]->insert(scalar->getName());
        res_columns[col++]->insert(static_cast<Int8>(scoped_scalar.cache_kind == NamedScalarCacheKind::Shared ? 1 : 0));

        if (status.value)
            res_columns[col++]->insert(applyVisitor(FieldVisitorToString(), status.value->value));
        else
            res_columns[col++]->insertDefault();

        res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(status.loading_start_time)));

        if (status.last_refresh_time)
            res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(*status.last_refresh_time)));
        else
            res_columns[col++]->insertDefault();

        if (status.refresh.next_refresh_time)
            res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(*status.refresh.next_refresh_time)));
        else
            res_columns[col++]->insertDefault();

        if (status.last_success_time)
            res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(*status.last_success_time)));
        else
            res_columns[col++]->insertDefault();

        if (status.refresh.refreshable)
            res_columns[col++]->insert(status.refresh.period_seconds);
        else
            res_columns[col++]->insertDefault();

        if (status.value && status.value->type)
            res_columns[col++]->insert(status.value->type->getName());
        else
            res_columns[col++]->insertDefault();

        res_columns[col++]->insert(static_cast<UInt8>(status.value.has_value()));
        res_columns[col++]->insert(static_cast<UInt8>(status.value && status.value->is_valid));

        // Operator tier - NULL when only the value-tier grant is present.
        if (show_full && !status.last_refresh_hostname.empty())
            res_columns[col++]->insert(status.last_refresh_hostname);
        else
            res_columns[col++]->insertDefault();

        if (show_full && !status.definer.empty())
            res_columns[col++]->insert(status.definer);
        else
            res_columns[col++]->insertDefault();

        if (show_full && status.expression)
            res_columns[col++]->insert(format({context, *status.expression}));
        else
            res_columns[col++]->insertDefault();

        /// Embed the type as a "[CODE]: " prefix so a single Nullable column
        /// covers what view_refreshes calls `exception`.
        if (show_full && !status.last_error.empty())
        {
            String exception_text = status.last_error_type.empty()
                ? status.last_error
                : "[" + status.last_error_type + "]: " + status.last_error;
            res_columns[col++]->insert(exception_text);
        }
        else
            res_columns[col++]->insertDefault();

        if (show_full)
            res_columns[col++]->insert(static_cast<UInt8>(status.refresh.refresh_started_at.has_value()));
        else
            res_columns[col++]->insertDefault();

        if (show_full && status.refresh.refresh_started_at)
            res_columns[col++]->insert(static_cast<UInt64>(std::chrono::system_clock::to_time_t(*status.refresh.refresh_started_at)));
        else
            res_columns[col++]->insertDefault();

        if (show_full)
            res_columns[col++]->insert(status.refresh.consecutive_failures);
        else
            res_columns[col++]->insertDefault();
    }
}

}
