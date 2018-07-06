#include "protoHelpers.h"
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Parsers/formatAST.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <ServerMessage.capnp.h>

#include <capnp/serialize.h>
#include <boost/algorithm/string.hpp>
#include <boost/range/join.hpp>
#include <common/logger_useful.h>


namespace DB
{
    ColumnWithTypeAndName storeContext(Context & context)
    {
        capnp::MallocMessageBuilder message;
        Proto::Context::Builder proto_context = message.initRoot<Proto::Context>();

        Databases dbs = context.getDatabases();
        auto proto_databases = proto_context.initDatabases(dbs.size());

        size_t db_nomber = 0;
        for (auto & pr_db : dbs)
        {
            const String& db_name = pr_db.first;
            IDatabase& db = *pr_db.second;

            auto proto_db = proto_databases[db_nomber];
            proto_db.setName(db_name);

            std::unordered_map<String, StoragePtr> tables;
            DatabaseIteratorPtr it_tables = db.getIterator(context);
            while (it_tables->isValid())
            {
                tables[it_tables->name()] = it_tables->table();
                it_tables->next();
            }

            auto proto_tables = proto_db.initTables(tables.size());
            size_t table_no = 0;
            for (const auto & pr_table : tables)
            {
                auto current_table = proto_tables[table_no];
                current_table.setName(pr_table.first);

                const ColumnsDescription & columns = pr_table.second->getColumns();
                auto proto_columns = current_table.initColumns(columns.defaults.size());

                size_t column_no = 0;
                for (const auto& pr_column : columns.defaults)
                {
                    const String & column_name = pr_column.first;
                    const ColumnDefault & def = pr_column.second;
                    std::stringstream ss;
                    ss << def.expression;

                    auto current_column = proto_columns[column_no];
                    current_column.setName(column_name);
                    current_column.getDefault().setKind(static_cast<UInt16>(def.kind));
                    current_column.getDefault().setExpression(ss.str());

                    ++column_no;
                }

                ++table_no;
            }

            ++db_nomber;
        }

        ColumnWithTypeAndName proto_column;
        proto_column.name = "context";
        proto_column.type = std::make_shared<DataTypeUInt64>();
        MutableColumnPtr data = proto_column.type->createColumn();

        kj::Array<capnp::word> serialized = messageToFlatArray(message);
        data->insertData(reinterpret_cast<const char *>(serialized.begin()), serialized.size() * sizeof(capnp::word));

        proto_column.column = std::move(data);
        return proto_column;
    }

    void loadContext(const ColumnWithTypeAndName & , Context & )
    {
#if 0
        kj::Array<word> messageToFlatArray(MessageBuilder& builder);

        capnp::MallocMessageBuilder message;
        Proto::ServerMessage::Builder serverMessage = message.initRoot<Proto::ServerMessage>();
        /// TODO
#endif
    }
}
