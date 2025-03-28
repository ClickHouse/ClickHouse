#include <Interpreters/TransposedMetricLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/CurrentMetrics.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/System/StorageSystemMetricLogView.h>
#include <Storages/System/attachSystemTablesImpl.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/parseQuery.h>
#include <Common/logger_useful.h>

#include <Interpreters/InterpreterRenameQuery.h>
#include <Parsers/ASTRenameQuery.h>


namespace DB
{

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

void TransposedMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(value);
}


ColumnsDescription TransposedMetricLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time."
        },
        {
            "event_time_microseconds",
            std::make_shared<DataTypeDateTime64>(6),
            parseQuery(codec_parser, "(DoubleDelta, ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time with microseconds resolution."
        },
        {
            "metric",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric name."
        },
        {
            "value",
            std::make_shared<DataTypeInt64>(),
            parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric value."
        }
    };
}

void TransposedMetricLog::stepFunction(TimePoint current_time)
{
    /// Static lazy initialization to avoid polluting the header with implementation details
    /// For differentiation of ProfileEvents counters.
    static std::vector<ProfileEvents::Count> prev_profile_events(ProfileEvents::end());

    TransposedMetricLogElement elem;
    elem.event_date = DateLUT::instance().toDayNum(elem.event_time);
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
    {
        const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        auto & old_value = prev_profile_events[i];

        /// Profile event counters are supposed to be monotonic. However, at least the `NetworkReceiveBytes` can be inaccurate.
        /// So, since in the future the counter should always have a bigger value than in the past, we skip this event.
        /// It can be reproduced with the following integration tests:
        /// - test_hedged_requests/test.py::test_receive_timeout2
        /// - test_secure_socket::test
        if (new_value < old_value)
            continue;

        elem.metric_name += PROFILE_EVENT_PREFIX;
        elem.metric_name += ProfileEvents::getName(ProfileEvents::Event(i));
        elem.value = new_value - old_value;
        old_value = new_value;
        this->add(elem);
    }

    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        elem.metric_name += CURRENT_METRIC_PREFIX;
        elem.metric_name += CurrentMetrics::getName(CurrentMetrics::Metric(i));
        elem.value = CurrentMetrics::values[i];
        this->add(elem);
    }
}


void TransposedMetricLog::prepareTable()
{
    SystemLog<TransposedMetricLogElement>::prepareTable();

    /// Now we need to create a view and potentially rotate old
    /// system.metric_log if it existed
    if (!view_name.empty())
    {
        auto storage_id = getTableID();
        auto database = DatabaseCatalog::instance().tryGetDatabase(storage_id.getDatabaseName());
        if (database)
        {
            auto table = database->tryGetTable(view_name, getContext());
            if (table && table->getName() != "SystemMetricLogView")
            {
                /// Rename the existing table.
                int suffix = 0;
                while (DatabaseCatalog::instance().isTableExist(
                    {database->getDatabaseName(), view_name + "_" + toString(suffix)}, getContext()))
                    ++suffix;

                ASTRenameQuery::Element elem
                {
                    ASTRenameQuery::Table
                    {
                        std::make_shared<ASTIdentifier>(database->getDatabaseName()),
                        std::make_shared<ASTIdentifier>(view_name)
                    },
                    ASTRenameQuery::Table
                    {
                        std::make_shared<ASTIdentifier>(database->getDatabaseName()),
                        std::make_shared<ASTIdentifier>(view_name + "_" + toString(suffix))
                    }
                };

                auto rename = std::make_shared<ASTRenameQuery>(ASTRenameQuery::Elements{std::move(elem)});

                ActionLock merges_lock;
                if (DatabaseCatalog::instance().getDatabase(database->getDatabaseName())->getUUID() == UUIDHelpers::Nil)
                    merges_lock = table->getActionLock(ActionLocks::PartsMerge);

                auto query_context = Context::createCopy(context);
                query_context->makeQueryContext();
                /// As this operation is performed automatically we don't want it to fail because of user dependencies on log tables
                query_context->setSetting("check_table_dependencies", Field{false});
                query_context->setSetting("check_referential_table_dependencies", Field{false});

                InterpreterRenameQuery(rename, query_context).execute();

                attachNoDescription<StorageSystemMetricLogView>(getContext(), *database, "metric_log", "Metric log view", storage_id);
            }
            else if (!table)
            {
                attachNoDescription<StorageSystemMetricLogView>(getContext(), *database, "metric_log", "Metric log view", storage_id);
            }
        }


    }
}

}
