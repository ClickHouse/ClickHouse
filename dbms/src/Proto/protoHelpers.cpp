#include "protoHelpers.h"
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Columns/IColumn.h>
#include <ServerMessage.capnp.h>

#include <capnp/serialize.h>
#include <sstream>


namespace DB
{
    template <typename ColumnT>
    static MutableColumnPtr serializeProto(const ColumnT & column_type, capnp::MessageBuilder & message)
    {
        MutableColumnPtr data = column_type.createColumn();

        kj::Array<capnp::word> serialized = messageToFlatArray(message);

        data->insertData(reinterpret_cast<const char *>(serialized.begin()), serialized.size() * sizeof(capnp::word));
        return data;
    }

    template <typename T>
    typename T::Reader deserializeProto(const char * data, size_t data_size)
    {
        const capnp::word * ptr = reinterpret_cast<const capnp::word *>(data);
        auto serialized = kj::arrayPtr(ptr, data_size / sizeof(capnp::word));

        capnp::FlatArrayMessageReader reader(serialized);
        return reader.getRoot<T>();
    }

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
        proto_column.column = std::move(serializeProto(*proto_column.type, message));
        return proto_column;
    }

    void loadContext(const ColumnWithTypeAndName & proto_column, Context & context)
    {
        StringRef plain_data = proto_column.column->getDataAt(0);
        size_t data_size = proto_column.column->byteSize();
        Proto::Context::Reader proto_context = deserializeProto<Proto::Context>(plain_data.data, data_size);

        // or ParserCompoundColumnDeclaration ?
        ParserColumnDeclaration parser_defaults;

        for (auto proto_database : proto_context.getDatabases())
        {
            String database_name = proto_database.getName().cStr();
            if (!context.isDatabaseExist(database_name))
            {
                // TODO
            }

            for (auto proto_table : proto_database.getTables())
            {
                String table_name = proto_table.getName().cStr();
                if (!context.isTableExist(database_name, table_name))
                {
                    // TODO
                }

                StoragePtr table = context.tryGetTable(database_name, table_name);
                // TODO: throw on fail

                ColumnsDescription column_description;
                for (auto column : proto_table.getColumns())
                {
                    String column_name = column.getName().cStr();
                    String expression = column.getDefault().getExpression().cStr();
                    ColumnDefaultKind expression_kind = static_cast<ColumnDefaultKind>(column.getDefault().getKind());
                    ASTPtr ast = parseQuery(parser_defaults, expression, expression.size());

                    column_description.defaults[column_name] = ColumnDefault{expression_kind, ast};
                }

                table->setColumns(column_description);
            }
        }
    }
}
