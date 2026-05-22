#include <Storages/System/StorageSystemKeeperChangelogs.h>

#include <Coordination/KeeperDispatcher.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnDefault.h>
#include <Common/logger_useful.h>

namespace DB
{

ColumnsDescription StorageSystemKeeperChangelogs::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"from_log_index", std::make_shared<DataTypeUInt64>(), "First Raft log index in the file (inclusive)."},
        {"to_log_index", std::make_shared<DataTypeUInt64>(), "Last Raft log index covered by the filename (inclusive). For the active file this is the rotation target and may be ahead of last_entry_index."},
        {"last_entry_index", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Highest log index actually appended to this file. NULL if the active file has not received any entries yet or the file is broken."},
        {"path", std::make_shared<DataTypeString>(), "File path on the disk."},
        {"disk_name", std::make_shared<DataTypeString>(), "Name of the disk holding the file."},
        {"size_bytes", std::make_shared<DataTypeUInt64>(), "Size of the file on disk."},
        {"modification_time", std::make_shared<DataTypeDateTime>(), "Last modification time of the file."},
        {"is_compressed", DataTypeFactory::instance().get("Bool"), "File payload is zstd-compressed."},
        {"active", DataTypeFactory::instance().get("Bool"), "This file is currently being appended to."},
        {"is_broken", DataTypeFactory::instance().get("Bool"), "Trailing record was found corrupted at startup."},
    };

    ColumnDescription entries_alias("entries", std::make_shared<DataTypeUInt64>());
    entries_alias.default_desc.kind = ColumnDefaultKind::Alias;
    const String alias_expression = "ifNull(last_entry_index - from_log_index + 1, 0)";
    ParserExpression expression_parser;
    entries_alias.default_desc.expression = parseQuery(
        expression_parser, alias_expression.data(), alias_expression.data() + alias_expression.size(), "expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    description.add(std::move(entries_alias));

    return description;
}

void StorageSystemKeeperChangelogs::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto dispatcher = context->tryGetKeeperDispatcher();
    if (!dispatcher)
        return;

    auto statuses = dispatcher->getChangelogsStatus();

    auto log = getLogger("SystemKeeperChangelogs");

    for (const auto & entry : statuses)
    {
        UInt64 size_bytes = 0;
        UInt32 modification_time = 0;
        try
        {
            if (entry.disk->existsFile(entry.path))
            {
                size_bytes = entry.disk->getFileSize(entry.path);
                modification_time = static_cast<UInt32>(entry.disk->getLastModified(entry.path).epochTime());
            }
            else if (!entry.active)
            {
                LOG_WARNING(log, "Finalized changelog file {} on disk {} is missing", entry.path, entry.disk->getName());
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to stat changelog file {} on disk {}", entry.path, entry.disk->getName()));
        }

        size_t i = 0;
        res_columns[i++]->insert(entry.from_log_index);
        res_columns[i++]->insert(entry.to_log_index);
        if (entry.last_entry_index.has_value())
            res_columns[i++]->insert(*entry.last_entry_index);
        else
            res_columns[i++]->insertDefault();
        res_columns[i++]->insert(entry.path);
        res_columns[i++]->insert(entry.disk->getName());
        res_columns[i++]->insert(size_bytes);
        res_columns[i++]->insert(modification_time);
        res_columns[i++]->insert(static_cast<UInt8>(entry.is_compressed));
        res_columns[i++]->insert(static_cast<UInt8>(entry.active));
        res_columns[i++]->insert(static_cast<UInt8>(entry.is_broken));
    }
}

}
